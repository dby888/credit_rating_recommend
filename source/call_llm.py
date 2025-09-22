# -*- coding: utf-8 -*-
"""
EFV (Events-Factors-Variables) extractor (serial-only version):
- Paragraph-first splitting; long paragraphs are sentence-packed
- Calls an LLM with strict JSON schema enforcement (one call per passage)
- Validates JSON with jsonschema
- Merges and de-duplicates outputs
- Batch APIs (serial), preserving input order

Requirements:
  pip install openai jsonschema

Env:
  export OPENAI_API_KEY=sk-...
"""

import os
import re
import json
import time
import hashlib
from typing import Any, Dict, List, Optional, Iterable, Tuple
import tiktoken


from jsonschema import Draft202012Validator
from openai import OpenAI

import settings


# ------------------------ JSON Schema (as agreed) ------------------------ #

# Build helpers for JSON Schema: enum + oneOf (with per-enum descriptions)
_EVENT_TYPE_ENUM = list(settings.EVENT_TYPES_DEFINITION.keys())
_EVENT_TYPE_DESC = "\n".join([f"- {k}: {v}" for k, v in settings.EVENT_TYPES_DEFINITION.items()])

EFV_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "events": {
            "type": "array",
            "description": (
                "Actions or happenings explicitly stated in THIS passage that have occurred, started, completed, or are clearly committed/announced, "
                "and that may affect the company’s stock price or be credit-relevant. Use an action-oriented phrase (past or clear present), "
                "include a time/period if shown. If one sentence mentions multiple dates for the same action (e.g., April and June repayments), "
                "you may keep it as one event rather than force-splitting."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Short, standardized label (≤5 words) for the realized action. Do NOT include values, units, or time."
                    },
                    "contents": {
                        "type": "string",
                        "description": "Normalized action phrase in past/clear present tense (≤20 chars). Exclude numeric values, units, and time expressions.",
                        "maxLength": 120
                    },
                    "event_type": {
                        "type": "string",
                        "enum": _EVENT_TYPE_ENUM,  # machine validation
                        "description": "One of the 18 controlled event categories."
                    },
                    "period": {
                        "type": ["string", "null"],
                        "description": "Verbatim time/period string from THIS passage if present; null if no explicit time is stated."
                    },
                    "evidence_start": {
                        "type": "string",
                        "description": (
                            "Verbatim first five words of the sentence that contains the evidence, preserving original casing and punctuation."
                        )
                    },
                    "evidence_offset": {
                        "type": "integer",
                        "description": (
                            "Total number of characters in the full sentence containing the evidence (include whitespace and punctuation)."
                        )
                    }
                },
                "required": ["name", "contents", "event_type", "period", "evidence_start", "evidence_offset"]
            }
        },

        "factors": {
            "type": "array",
            "description": (
                "Specific, reusable credit/rating considerations stated in THIS passage (drivers/constraints/policies/risks). "
                "Include explicit forward-looking statements; keep the name a concise noun phrase."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Short, standardized label (≤6 words) for the driver/constraint/policy/risk. Do NOT include values, units, or time."
                    },
                    "contents": {
                        "type": "string",
                        "description": (
                            "Generate a concise, standardized factor description (≤15 words) that captures the core driver, "
                            "constraint, policy, or risk stated in THIS passage. "
                            "Do NOT include numeric values, dates, or subjective attribution such as 'Fitch expects' or 'the company'."
                        )
                    },
                    "period": {
                        "type": ["string", "null"],
                        "description": "Verbatim time/period string if present; null if absent."
                    },
                    "evidence_start": {
                        "type": "string",
                        "description": (
                            "Verbatim first five words of the sentence that contains the evidence, preserving original casing and punctuation."
                        )
                    },
                    "evidence_offset": {
                        "type": "integer",
                        "description": (
                            "Total number of characters in the full sentence containing the evidence (include whitespace and punctuation)."
                        )
                    }
                },
                "required": ["name", "contents", "period", "evidence_start", "evidence_offset"]
            }
        },

        "variables": {
            "type": "array",
            "description": (
                "All observable, measurable quantities mentioned in THIS passage (verbatim). "
                "Each item captures one metric/value mention with its unit and period if shown (set to null if absent). "
                "If the same metric appears with different values or periods, output multiple items to cover all occurrences."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": (
                            "A concise, standardized label for the measurable metric mentioned in THIS passage. "
                            "It should be a short noun phrase (≤5 words), capturing the essence of the metric "
                            "without numeric values or periods."
                        )
                    },
                    "contents": {
                        "type": "string",
                        "description": "Verbatim metric description as written in THIS passage."
                    },
                    "value": {
                        "type": "string",
                        "description": "Verbatim numeric/date/ratio text (e.g., '$66.2 billion', '2.25 billion', '3.1x', 'FY2024')."
                    },
                    "unit": {
                        "type": ["string", "null"],
                        "description": "Verbatim unit token as it appears (e.g., '%','x','$','USD','bn','m'); null if not shown."
                    },
                    "period": {
                        "type": ["string", "null"],
                        "description": "Verbatim time/period expression (e.g., 'as of Mar 31, 2025','due Nov 2028','1H2025'); null if absent."
                    },
                    "evidence_start": {
                        "type": "string",
                        "description": (
                            "Verbatim first five words of the sentence that contains the evidence, preserving original casing and punctuation."
                        )
                    },
                    "evidence_offset": {
                        "type": "integer",
                        "description": (
                            "Total number of characters in the full sentence containing the evidence (include whitespace and punctuation)."
                        )
                    }
                },
                "required": ["name", "contents", "value", "unit", "period", "evidence_start", "evidence_offset"]
            }
        }
    },
    "required": ["events", "factors", "variables"]
}




def count_tokens(text, model="gpt-4.1"):
    enc = tiktoken.encoding_for_model(model)
    return len(enc.encode(text))

# ------------------------ Extractor Class (serial) ------------------------ #


def _merge_outputs(out: Any, text: str) -> Dict[str, Any]:
    """
    Merge outputs (no deduplication). Reconstruct the `evidence` text for every item.

    Supported input formats per item:
      A) Legacy index-based format:
         - evidence_start: int  (inclusive, relative to THIS passage)
         - evidence_end:   int  (exclusive)
         -> evidence = text[start:end]. If `end` falls in the middle of a sentence,
            extend the selection to the end of that sentence.

      B) Sentence-locator + offset format:
         - evidence_start: str  (the verbatim first five words of the sentence containing the evidence)
         - evidence_offset: int (total number of characters from the START of that sentence)
         -> Locate the sentence containing `five_words`. Use `offset` to compute a tentative end position.
            If the tentative end falls in the middle of a sentence, extend the selection
            to include the full sentence where the tentative end lies.
    """

    def ensure_list(x):
        """Ensure the input is always a list."""
        if x is None:
            return []
        if isinstance(x, list):
            return x
        return [x]

    # -------- Sentence segmentation helpers --------
    # Rough sentence segmentation for English/general text: split by `. ! ?` or Chinese equivalents.
    # Each returned span is a tuple (start, end) with end being exclusive.
    SENT_END_RE = re.compile(r'[.!?]|[。！？]')

    def sentence_spans(s: str) -> List[Tuple[int, int]]:
        """Return a list of sentence spans (start, end) for the given text."""
        spans: List[Tuple[int, int]] = []
        i = 0
        n = len(s)
        last = 0
        while i < n:
            m = SENT_END_RE.search(s, i)
            if not m:
                break
            end_idx = m.end()  # include the punctuation
            # Skip trailing quotes after punctuation
            while end_idx < n and s[end_idx] in '"\'':
                end_idx += 1
            # Optionally include trailing spaces after punctuation
            while end_idx < n and s[end_idx].isspace():
                end_idx += 1
            spans.append((last, end_idx))
            i = end_idx
            last = end_idx
        # If the text does not end with punctuation, treat the tail as a sentence
        if last < n:
            spans.append((last, n))
        return spans

    spans = sentence_spans(text)

    def span_containing(pos: int) -> Tuple[int, int]:
        """Return the (start, end) of the sentence containing a given position."""
        if not spans:
            return (0, len(text))
        if pos <= spans[0][0]:
            return spans[0]
        if pos >= spans[-1][1]:
            return spans[-1]
        for st, ed in spans:
            if st <= pos < ed:
                return (st, ed)
        return spans[-1]

    def span_starting_with_five_words(five_words: str) -> Tuple[int, int]:
        """
        Locate the sentence that starts with the given five words (verbatim, preserving original casing).
        This works by finding `five_words` in the text and then returning the span of the sentence
        that contains this match.
        """
        if not five_words:
            return (0, 0)
        idx = text.find(five_words)
        if idx < 0:
            # Fallback: try normalized spaces
            norm = re.sub(r'\s+', ' ', five_words.strip())
            idx = re.sub(r'\s+', ' ', text).find(norm)
            if idx < 0:
                # Default to the first sentence if still not found
                return spans[0] if spans else (0, len(text))
            # Cannot reliably map back to original index
            return spans[0] if spans else (0, len(text))
        return span_containing(idx)

    def rebuild_evidence(item: Dict[str, Any]) -> str:
        """Reconstruct the evidence text for a single item."""
        # Case A: legacy index-based format
        st, ed = item.get("evidence_start"), item.get("evidence_end")
        if isinstance(st, int) and isinstance(ed, int) and 0 <= st <= ed <= len(text):
            # If end is not aligned with sentence boundary, extend to the end of that sentence
            _, end_sent = span_containing(max(st, min(ed, len(text) - 1)))
            return text[st:end_sent]

        # Case B: sentence-locator + offset format
        fw = item.get("evidence_start")
        off = item.get("evidence_offset")
        if isinstance(fw, str) and isinstance(off, int) and off > 0:
            sent_st, sent_ed = span_starting_with_five_words(fw)
            if sent_st == sent_ed == 0 and len(text) == 0:
                return ""
            # Tentative end = sentence start + offset (may cross sentence boundary or end mid-sentence)
            tentative_end = sent_st + off
            # Align to the end of the sentence where the tentative end falls
            _, final_end = span_containing(max(0, min(tentative_end, len(text) - 1)))
            return text[sent_st:final_end]

        # If unable to identify, return empty
        return ""

    events: List[Dict[str, Any]] = []
    factors: List[Dict[str, Any]] = []
    variables: List[Dict[str, Any]] = []

    # Merge all outputs and reconstruct evidence
    for chunk in ensure_list(out):
        for e in (chunk.get("events") or []):
            e = dict(e)
            e["evidence"] = rebuild_evidence(e)
            events.append(e)
        for f in (chunk.get("factors") or []):
            f = dict(f)
            f["evidence"] = rebuild_evidence(f)
            factors.append(f)
        for v in (chunk.get("variables") or []):
            v = dict(v)
            v["evidence"] = rebuild_evidence(v)
            variables.append(v)

    return {"events": events, "factors": factors, "variables": variables}



class EventFactorVariableExtractor:
    """
    Serial LLM-based extractor for Events, Factors, Variables from text.
    Validates model output against EFV_SCHEMA and merges results across passages.
    """

    DEFAULT_SYSTEM_PROMPT = (
        "You are an extraction model. Use ONLY the CURRENT PASSAGE. Return STRICT JSON per the given JSON Schema."
    )

    DEFAULT_USER_INSTRUCTION = (
        "Task: Extract three sets from the provided passage: events, factors, and variables.\n"
        "Rules:\n"
        "1) Follow the provided JSON Schema as the single source of truth (field names/types/required).\n"
        "2) Use ONLY the CURRENT PASSAGE (no outside info; no guessing).\n"
        "3) Prioritize capturing items that may affect the company’s stock price or credit.\n"
    )

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        system_prompt: Optional[str] = None,
        user_instruction: Optional[str] = None,
        temperature: float = 0.0,
        max_retries: int = 0                 # <-- default: NO retry to avoid double calls
    ) -> None:
        self.model = model
        self.client = OpenAI(api_key=api_key or settings.GPT_KEY or os.getenv("OPENAI_API_KEY"))
        self.system_prompt = system_prompt or self.DEFAULT_SYSTEM_PROMPT
        self.user_instruction = user_instruction or self.DEFAULT_USER_INSTRUCTION
        self.temperature = temperature
        self.response_format_via_schema = True
        self._validator = Draft202012Validator(EFV_SCHEMA)

    # ------------------------- Public API (serial) ------------------------- #

    def extract(self, text: str) -> Dict[str, Any]:
        """
        Extract EFV from a (possibly long) text:
          - split into passages
          - call LLM per passage (serial)
          - validate and merge
        """
        out = self._call_llm(text)
        return _merge_outputs(out, text)

    def extract_batch_rows(
            self,
            rows: Iterable[Tuple[Any, Any, Any, Any, str]],
            blank: Any = '',  # Placeholder for empty values, can be set to "" if needed
            is_aggregate_by_company = False,
    ) -> List[Dict[str, Any]]:
        """
        Aggregate all contents for the same company and call LLM once per company.
        When filling results, section_id and report_id will be replaced with blank values.

        rows format: (section_id, report_id, company_name, section_name, contents)
        """
        rows = list(rows)
        if not rows:
            return []

        results: List[Dict[str, Any]] = []
        # passage_by_passage
        total = len(rows)
        print(f"Total: {total}")
        pos = 0
        if not is_aggregate_by_company:
            for row in rows:
                pos += 1
                section_id, report_id, company_name, section_name, contents = row
                if not contents or not str(contents).strip():
                    results.append({
                        "section_id": section_id,
                        "report_id": report_id,
                        "company_name": company_name,
                        "section_name": section_name,
                        "events": [], "factors": [], "variables": []
                    })
                    continue
                efv = self.extract(contents)
                results.append({
                    "section_id": section_id,
                    "report_id": report_id,
                    "company_name": company_name,
                    "section_name": section_name,
                    **efv
                })
                print(f"\rProgress: {pos}/{total}", end="", flush=True)
            return results
        # 1) Aggregate text by company_name
        company_to_parts: Dict[str, List[str]] = {}
        for _, _, company, section_name, contents in rows:
            if contents and str(contents).strip():
                company_to_parts.setdefault(company, []).append(str(contents).strip())

        # Combine all sections for each company into a single text block
        company_to_text = {c: "\n\n".join(parts) for c, parts in company_to_parts.items()}

        # 2) Call LLM once for each company
        company_to_efv: Dict[str, Dict[str, Any]] = {}
        for company, text in company_to_text.items():
            company_to_efv[company] = self.extract(text)

        # 3) Fill results: section_id and report_id set to blank
        for company in company_to_text.keys():
            efv = company_to_efv.get(company, {"events": [], "factors": [], "variables": []})
            results.append({
                "section_id": blank,
                "report_id": blank,
                "company_name": company,
                "section_name": section_name,
                **efv
            })
        return results


    def _call_llm(self, passage: str) -> Dict[str, Any]:
        """Single LLM call with schema enforcement (if supported)."""
        # with open("temp.json", "r", encoding="utf-8") as f:
        #     data = json.load(f)
        # return data
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.user_instruction},
            {"role": "user", "content": f"PASSAGE:\n{passage}"},
        ]

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "efv_schema",
                    "schema": EFV_SCHEMA,
                    "strict": True
                }
            }
        )

        content = resp.choices[0].message.content
        data = json.loads(content)
        # Validate against JSON Schema to fail fast on malformed outputs
        self._validator.validate(data)
        return data

    @staticmethod
    def _hash(*parts: str) -> str:
        """Stable hash for dedup (lowercased name + evidence, etc.)."""
        joined = "||".join([p.strip() for p in parts if p is not None])
        return hashlib.md5(joined.encode("utf-8")).hexdigest()


