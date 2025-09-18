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
                "Actions or happenings explicitly stated in THIS passage that have occurred, started, completed, or are clearly committed/announced, and that may affect the company’s stock price or be credit-relevant. Use an action-oriented phrase (past or clear present), include a time/period if shown, and quote the shortest verbatim evidence. If one sentence mentions multiple dates for the same action (e.g., April and June repayments), you may keep it as one event rather than force-splitting."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": (
                            "Action-oriented past-tense phrase (≤120 chars) summarizing the realized action, "
                            "e.g., 'Repaid April-2025 maturities', 'Began share repurchase program', "
                            "'Completed debt-financed acquisition of Whole Foods'."
                        ),
                        "maxLength": 120
                    },
                    "event_type": {
                        "type": "string",
                        "enum": _EVENT_TYPE_ENUM,          # machine validation
                        "description": (
                            "One of the 18 controlled event categories:\n" + _EVENT_TYPE_DESC
                        )
                    },
                    "period": {
                        "type": ["string", "null"],
                        "description": (
                            "Verbatim time/period string from THIS passage if present (e.g., 'April 2025', 'June 2025', '2022', "
                            "'as of Mar 31, 2025'); null if no explicit time is stated."
                        )
                        },
                    "evidence": {
                        "type": "string",
                        "description": (
                            "Shortest verbatim span from THIS passage proving the action (include the action verb and its object; "
                            "may include the time phrase if present)."
                        )
                    }
                },
                "required": ["name", "event_type","period", "evidence"]
            }
        },

        "factors": {
            "type": "array",
            "description": (
                "Specific, reusable credit considerations expressed in THIS passage (not broad buckets, not forecasts). "
                "Keep name as a concise noun-phrase label; keep evidence verbatim."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": (
                            "Reusable considerations stated in THIS passage that characterize the company’s situation, capabilities, constraints, policies, patterns, or outlook—including forward-looking statements (plans/targets/assumptions/expectations) when explicitly written. Keep the name as a concise noun phrase; add time/period if present; evidence must be a verbatim snippet."
                        )
                    },
                    "period": {
                        "type": ["string", "null"],
                        "description": "Verbatim time/period string if present; null if absent."
                    },
                    "evidence": {
                        "type": "string",
                        "description": "Verbatim snippet from THIS passage that expresses this factor/driver."
                    }
                },
                "required": ["name", "period", "evidence"]
            }
        },

        "variables": {
            "type": "array",
            "description": (
                "All observable, measurable quantities mentioned in THIS passage (verbatim). Each item captures one metric/value mention with its unit and period if shown (set to null if absent). If the same metric appears with different values or periods, output multiple items to cover all occurrences."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Metric noun phrase (verbatim or lightly normalized), e.g., 'cash and cash equivalents'."
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
                    "evidence": {
                        "type": "string",
                        "description": "Verbatim snippet from THIS passage that contains the value."
                    }
                },
                "required": ["name", "value", "unit", "period", "evidence"]
            }
        }
    },
    "required": ["events", "factors", "variables"]
}




# ------------------------ Paragraph/Sentence splitting ------------------------ #

# Abbreviations that may end with a period but should NOT end a sentence
ABBR = re.compile(
    r"""(?ix)                               # ignore case, verbose
    (?:Mr|Ms|Mrs|Dr|Prof|Sr|Jr|vs|No|Inc|Ltd|Co|Corp|Mt|St|
       Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|
       U\.S|U\.K|e\.g|i\.e|etc)
    \.$
    """
)

# Basic sentence boundary: CN/EN enders; NO variable-length look-behind
SENT_BOUNDARY = re.compile(r"(?<=[。！？；])\s+|(?<=[.!?])\s+")

def _split_paragraphs(text: str) -> List[str]:
    """Split by blank lines as paragraph separators; collapse intra-paragraph newlines."""
    text = text.replace("\r\n", "\n").strip()
    if not text:
        return []
    raw = re.split(r"\n\s*\n+", text)                     # blank lines separate paragraphs
    paras = [re.sub(r"\s*\n\s*", " ", p).strip() for p in raw if p.strip()]
    return paras

def _split_sentences(para: str) -> List[str]:
    """
    Sentence split without variable-length look-behind.
    1) Split on simple enders
    2) Merge back if previous piece ends with an abbreviation like 'U.S.' / 'Inc.'
    3) Trim tiny tails around quotes/brackets
    """
    if not para:
        return []

    # Initial split by punctuation boundaries
    parts = re.split(SENT_BOUNDARY, para.strip())
    parts = [p.strip() for p in parts if p and p.strip()]

    # Merge pieces when the left fragment ends with an abbreviation (e.g., 'U.S.')
    merged: List[str] = []
    for piece in parts:
        if merged and ABBR.search(merged[-1]):  # previous ended with known abbr => merge
            merged[-1] = (merged[-1] + " " + piece).strip()
        else:
            merged.append(piece)

    # Optional: merge very short tails (like closing quotes) into previous sentence
    out: List[str] = []
    for s in merged:
        if out and len(s) < 6 and re.match(r"""^[)"\]’”'》】）]+$""", s):
            out[-1] = (out[-1] + " " + s).strip()
        else:
            out.append(s)
    return out

def _pack_sentences_to_chunks(sents: List[str], max_chars: int, overlap_sentences: int) -> List[str]:
    """Greedy packing of consecutive sentences into chunks <= max_chars, with optional sentence overlap."""
    passages: List[str] = []
    i = 0
    n = len(sents)
    while i < n:
        chunk = sents[i]
        j = i + 1
        while j < n and len(chunk) + 1 + len(sents[j]) <= max_chars:
            chunk = f"{chunk} {sents[j]}"
            j += 1
        passages.append(chunk)
        if j >= n:
            break
        i = max(i + 1, j - max(0, overlap_sentences))
    return passages

def _split_into_passages(
    text: str,
    max_chars: int,
    overlap_sentences: int = 1
) -> List[str]:
    """
    Paragraph-first, but allow packing multiple short paragraphs into one passage
    up to `max_chars`. Only when a single paragraph exceeds `max_chars`, split
    THAT paragraph by sentences and pack them. No mid-word or mid-sentence cuts.
    """
    passages: List[str] = []
    paragraphs = _split_paragraphs(text)

    buf = ""  # rolling buffer for short paragraphs
    def _flush_buf():
        nonlocal buf
        if buf.strip():
            passages.append(buf.strip())
        buf = ""

    for para in paragraphs:
        p = para.strip()
        if not p:
            continue

        # If this paragraph alone is longer than max_chars -> sentence split it
        if len(p) > max_chars:
            _flush_buf()
            sents = _split_sentences(p)
            if not sents:
                # Rare fallback: hard slicing but extend to next whitespace to avoid mid-word cuts
                start, n = 0, len(p)
                while start < n:
                    end = min(start + max_chars, n)
                    if end < n:
                        m = re.search(r"\s", p[end:min(n, end + 200)])
                        if m:
                            end = end + m.start()
                    passages.append(p[start:end].strip())
                    start = end
            else:
                passages.extend(_pack_sentences_to_chunks(sents, max_chars, overlap_sentences))
            continue

        # Paragraph is short enough: try to pack it into the current buffer
        if not buf:
            buf = p
        else:
            # +1 for a space between paragraphs (or use "\n\n" if you prefer)
            if len(buf) + 1 + len(p) <= max_chars:
                buf = f"{buf} {p}"
            else:
                _flush_buf()
                buf = p

    _flush_buf()
    return passages


# ------------------------ Extractor Class (serial) ------------------------ #

class EventFactorVariableExtractor:
    """
    Serial LLM-based extractor for Events, Factors, Variables from text.
    Validates model output against EFV_SCHEMA and merges results across passages.
    """

    DEFAULT_SYSTEM_PROMPT = (
        "You are an extraction model. Use ONLY the CURRENT PASSAGE. Return STRICT JSON per the given JSON Schema. Copy numbers/dates/units/evidence verbatim. If a field is absent, omit or set null only if schema allows. Output JSON only."
    )

    DEFAULT_USER_INSTRUCTION = (
        "Task: Extract three sets from the provided passage: events, factors, and variables.\n"
        "Rules:\n"
        "1) Follow the provided JSON Schema as the single source of truth (field names/types/required).\n"
        "2) Use ONLY the CURRENT PASSAGE (no outside info; no guessing).\n"
        "3) Copy numbers/dates/units/evidence VERBATIM; no normalization.\n"
        "4) If a field is absent, omit it or set null only if the schema allows.\n"
        "5) Prioritize capturing items that may affect the company’s stock price or credit.\n"
        "6) Output JSON only."
    )

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        system_prompt: Optional[str] = None,
        user_instruction: Optional[str] = None,
        max_chars: int = 1800,
        overlap_sentences: int = 1,          # <-- sentence overlap (NOT characters)
        temperature: float = 0.0,
        response_format_via_schema: bool = True,
        rate_limit_per_sec: Optional[float] = None,
        max_retries: int = 0                 # <-- default: NO retry to avoid double calls
    ) -> None:
        self.model = model
        self.client = OpenAI(api_key=api_key or settings.GPT_KEY or os.getenv("OPENAI_API_KEY"))
        self.system_prompt = system_prompt or self.DEFAULT_SYSTEM_PROMPT
        self.user_instruction = user_instruction or self.DEFAULT_USER_INSTRUCTION
        self.max_chars = max_chars
        self.overlap_sentences = overlap_sentences
        self.temperature = temperature
        self.response_format_via_schema = response_format_via_schema
        self.rate_limit_per_sec = rate_limit_per_sec
        self.max_retries = max_retries
        self._validator = Draft202012Validator(EFV_SCHEMA)
        self._last_call_ts = 0.0

    # ------------------------- Public API (serial) ------------------------- #

    def extract(self, text: str) -> Dict[str, Any]:
        """
        Extract EFV from a (possibly long) text:
          - split into passages
          - call LLM per passage (serial)
          - validate and merge
        """
        passages = _split_into_passages(text, self.max_chars, self.overlap_sentences)
        outs: List[Dict[str, Any]] = []
        for p in passages:
            out = self._call_llm_with_retry(p)
            outs.append(out)
        return self._merge_outputs(outs)

    def extract_batch_texts(self, contents_list: Iterable[str]) -> List[Dict[str, Any]]:
        """
        Serial batch for a plain list of texts. Order is preserved.
        """
        results: List[Dict[str, Any]] = []
        for text in contents_list:
            if not text or not str(text).strip():
                results.append({"events": [], "factors": [], "variables": []})
            else:
                results.append(self.extract(text))
        return results

    def extract_batch_rows(
        self,
        rows: Iterable[Tuple[Any, Any, Any, Any, str]],
    ) -> List[Dict[str, Any]]:
        """
        Serial batch for rows with metadata.
        Row format: (section_id, report_id, company_name, section_name, contents)
        """
        results: List[Dict[str, Any]] = []
        for row in rows:
            section_id, report_id, company_name, section_name, contents = row
            if not contents or not str(contents).strip():
                results.append({
                    "section_id": section_id,
                    "report_id": report_id,
                    "company_name": company_name,
                    "section_name": section_name,
                    "events": [], "factors": [], "variables": []
                })
            else:
                efv = self.extract(contents)
                results.append({
                    "section_id": section_id,
                    "report_id": report_id,
                    "company_name": company_name,
                    "section_name": section_name,
                    **efv
                })
        return results

    # ------------------------- Internals (serial) ------------------------- #

    def _throttle(self) -> None:
        """Optional simple rate limiter (per instance)."""
        if not self.rate_limit_per_sec or self.rate_limit_per_sec <= 0:
            return
        min_interval = 1.0 / float(self.rate_limit_per_sec)
        now = time.time()
        wait = self._last_call_ts + min_interval - now
        if wait > 0:
            time.sleep(wait)
        self._last_call_ts = time.time()

    def _call_llm_with_retry(self, passage: str) -> Dict[str, Any]:
        """Retry only up to max_retries; default 0 (no retry)."""
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                return self._call_llm(passage)
            except Exception as e:
                last_err = e
                if attempt >= self.max_retries:
                    break
                time.sleep(1.5 * (attempt + 1))
        if last_err:
            raise last_err
        raise RuntimeError("Unknown LLM error")

    def _call_llm(self, passage: str) -> Dict[str, Any]:
        """Single LLM call with schema enforcement (if supported)."""
        self._throttle()
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.user_instruction},
            {"role": "user", "content": f"PASSAGE:\n{passage}"},
        ]

        if self.response_format_via_schema:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                # You can set an output token cap if your SDK supports it:
                # max_output_tokens=600,
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
        else:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature
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

    def _merge_outputs(self, outs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge multiple passage-level outputs into one,
        with simple dedup logic to avoid duplicates.
        """
        ev_seen, fa_seen, va_seen = set(), set(), set()
        events: List[Dict[str, Any]] = []
        factors: List[Dict[str, Any]] = []
        variables: List[Dict[str, Any]] = []

        for out in outs:
            for e in out.get("events", []):
                k = self._hash(e.get("evidence", ""))
                if k not in ev_seen:
                    ev_seen.add(k)
                    events.append(e)

            for f in out.get("factors", []):
                k = self._hash(f.get("name", "").lower(), f.get("evidence", ""))
                if k not in fa_seen:
                    fa_seen.add(k)
                    factors.append(f)

            for v in out.get("variables", []):
                k = self._hash(v.get("name", "").lower(), v.get("value_text", ""), v.get("unit") or "")
                if k not in va_seen:
                    va_seen.add(k)
                    variables.append(v)

        return {"events": events, "factors": factors, "variables": variables}
