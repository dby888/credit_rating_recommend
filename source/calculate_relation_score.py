import re
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict
import sqlite3
import pandas as pd
import settings

import data_utils

# ---- helpers ----
SENT_END_RE = re.compile(r'[.!?]|[。！？]')

def sentence_spans(text: str) -> List[Tuple[int, int]]:
    """Return sentence spans (start, end) inclusive-exclusive, coarse but robust."""
    spans, i, n, last = [], 0, len(text), 0
    while i < n:
        m = SENT_END_RE.search(text, i)
        if not m:
            break
        end_idx = m.end()
        while end_idx < n and text[end_idx] in '"\'':
            end_idx += 1
        spans.append((last, end_idx))
        i, last = end_idx, end_idx
    if last < n:
        spans.append((last, n))
    return spans

def sent_index_of(pos: int, sent_spans: List[Tuple[int,int]]) -> int:
    if not sent_spans:
        return -1
    if pos < sent_spans[0][0]:
        return 0
    if pos >= sent_spans[-1][1]:
        return len(sent_spans)-1
    for i, (s, e) in enumerate(sent_spans):
        if s <= pos < e:
            return i
    return len(sent_spans)-1

def para_spans(text: str) -> List[Tuple[int,int]]:
    """Paragraphs separated by blank line or newline followed by capital; simple heuristic."""
    parts = re.split(r'\n\s*\n+', text)  # blank-line split
    spans, cursor = [], 0
    for p in parts:
        spans.append((cursor, cursor + len(p)))
        cursor += len(p) + 2  # approximate
    if not spans:
        spans = [(0, len(text))]
    return spans

def para_index_of(pos: int, para_spans_: List[Tuple[int,int]]) -> int:
    for i, (s, e) in enumerate(para_spans_):
        if s <= pos < e:
            return i
    return len(para_spans_)-1

# ---- normalize items ----
def normalize_items(items: List[Dict[str,Any]],
                    text: str,
                    sent_spans: List[Tuple[int,int]]) -> List[Dict[str,Any]]:
    """
    Find evidence location and add sentence index.
    """
    out: List[Dict[str,Any]] = []
    pspans = para_spans(text)

    for it in (items or []):
        it = dict(it)
        ev: Optional[str] = it.get("evidence")
        if not ev:
            continue

        # 1) exact verbatim match
        start = text.find(ev)

        # 2) light fallback: whitespace collapsed
        if start < 0:
            norm_text = re.sub(r"\s+", " ", text)
            norm_ev = re.sub(r"\s+", " ", ev.strip())
            if norm_ev:
                anchor = ev[:10] if len(ev) >= 10 else ev
                approx = norm_text.find(norm_ev)
                if approx >= 0 and anchor:
                    start = text.find(anchor)

        if start < 0:
            continue

        end = start + len(ev)
        if not (0 <= start <= end <= len(text)):
            continue

        sidx = sent_index_of(start, sent_spans)
        pidx = para_index_of(start, pspans)

        it["_char_start"] = start
        it["_char_end"] = end
        it["_sent_idx"] = sidx
        it["_para_idx"] = pidx
        out.append(it)

    return out

# ---- matching ----
def link_by_position(text: str,
                     events: List[Dict[str,Any]],
                     factors: List[Dict[str,Any]],
                     variables: List[Dict[str,Any]]) -> Dict[str, List[Dict[str,Any]]]:
    """
    Rule:
      - Same sentence => score = 1.0
      - Adjacent sentences => score = 0.5
      - Otherwise => skip
    """
    sent_spans = sentence_spans(text)
    evs = normalize_items(events, text, sent_spans)
    fcs = normalize_items(factors, text, sent_spans)
    vrs = normalize_items(variables, text, sent_spans)

    def discrete_score(a_sent: int, b_sent: int) -> Optional[float]:
        if a_sent == -1 or b_sent == -1:
            return None
        if a_sent == b_sent:
            return 1.0
        if abs(a_sent - b_sent) == 1:
            return 0.5
        return None  # not recorded

    out_evt_fac, out_evt_var, out_fac_var = [], [], []

    # Event ↔ Factor
    for e in evs:
        for f in fcs:
            s = discrete_score(e["_sent_idx"], f["_sent_idx"])
            if s is not None:
                out_evt_fac.append({"event": e, "factor": f, "score": s})

    # Event ↔ Variable
    for e in evs:
        for v in vrs:
            s = discrete_score(e["_sent_idx"], v["_sent_idx"])
            if s is not None:
                out_evt_var.append({"event": e, "variable": v, "score": s})

    # Factor ↔ Variable
    for f in fcs:
        for v in vrs:
            s = discrete_score(f["_sent_idx"], v["_sent_idx"])
            if s is not None:
                out_fac_var.append({"factor": f, "variable": v, "score": s})

    return {
        "event_factor": out_evt_fac,
        "event_variable": out_evt_var,
        "factor_variable": out_fac_var,
    }

# ---- save to DB ----
def save_links_to_db(section_id_to_text, grouped, db_path=settings.database_path):
    """
    For each section_id:
      - build links with discrete scores (1.0 or 0.5)
      - delete old rows for that section_id
      - insert new rows into event_relation
    """
    all_rows = []

    for section_id, text in section_id_to_text.items():
        links = link_by_position(
            text,
            grouped[section_id]["events"],
            grouped[section_id]["factors"],
            grouped[section_id]["variables"],
        )

        # flatten
        for item in links["event_factor"]:
            all_rows.append({
                "section_id": section_id,
                "event_id": item["event"].get("id"),
                "factor_id": item["factor"].get("id"),
                "variable_id": None,
                "score": item["score"],
            })
        for item in links["event_variable"]:
            all_rows.append({
                "section_id": section_id,
                "event_id": item["event"].get("id"),
                "factor_id": None,
                "variable_id": item["variable"].get("id"),
                "score": item["score"],
            })
        for item in links["factor_variable"]:
            all_rows.append({
                "section_id": section_id,
                "event_id": None,
                "factor_id": item["factor"].get("id"),
                "variable_id": item["variable"].get("id"),
                "score": item["score"],
            })

        # --- Direct insert into SQLite ---
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1. Delete old rows for all sections being processed
    for sid in section_id_to_text.keys():
        cur.execute("DELETE FROM event_relation WHERE section_id = ?", (sid,))

    # 2. Insert new rows
    insert_sql = """
                INSERT INTO event_relation (section_id, event_id, factor_id, variable_id, score)
                VALUES (:section_id, :event_id, :factor_id, :variable_id, :score)
            """
    cur.executemany(insert_sql, all_rows)
    print(f"Inserted rows: {len(all_rows)}")
    conn.commit()
    conn.close()
    return len(all_rows)


def calculate_relation(section_names):
    section_list = data_utils.select_sections_from_db(section_names)
    section_id_to_text = {row[0]: row[4] for row in section_list}
    events, factors, variables = data_utils.fetch_efv_by_sections(section_names=section_names)
    grouped = defaultdict(lambda: {"events": [], "factors": [], "variables": []})
    for e in events:
        grouped[e.get("section_id")]["events"].append(e)
    for f in factors:
        grouped[f.get("section_id")]["factors"].append(f)
    for v in variables:
        grouped[v.get("section_id")]["variables"].append(v)
    df_result = save_links_to_db(section_id_to_text, grouped, db_path=settings.database_path)
    return df_result


# ---- main ----
if __name__ == '__main__':
    # section_names = ["liquidity and debt structure"]
    section_names = None
    result = calculate_relation(section_names)
    print(result)
