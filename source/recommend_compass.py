# recommend_compass.py
# Recommend variables, factors, and events for a given (company, section) using your EFV SQLite schema.
# - Step 1: resolve report_id set for the company (optionally by year window / limit)
# - Step 2: fetch EFV rows for (report_id IN ...) AND (section_name IN ...)
# - Step 3: rank & recommend with a simple score (freq + recency + link bonus for events)
#
# Usage:
#   python recommend_compass.py --company "Amazon.com, Inc." --sections Liquidity "Capital Structure" --k 8 --year-min 2024
#   python recommend_compass.py --company "Amazon.com, Inc." --sections Liquidity --out recs.json
#

import os
import json
import argparse
import sqlite3
from typing import Iterable, List, Dict, Any, Optional, Union, Tuple

import settings



# ---------------------------------------------------------------------
# Section normalization (accept single str or iterable)
# ---------------------------------------------------------------------
def _normalize_sections(section_names: Optional[Union[str, Iterable[str]]]) -> List[str]:
    if section_names is None:
        return []
    if isinstance(section_names, str):
        items = [section_names]
    else:
        items = list(section_names)
    out = [s.strip() for s in items if s is not None and str(s).strip()]
    return out


# ---------------------------------------------------------------------
# Step 1 — resolve report_id set for the company (optional year window / limit)
# ---------------------------------------------------------------------
def get_report_ids_for_company(
    conn: sqlite3.Connection,
    company_name: str,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[int]:
    cur = conn.cursor()
    where = ["LOWER(company_name) = LOWER(:c)"]
    params = {"c": company_name}

    if year_min is not None:
        where.append("year >= :ymin"); params["ymin"] = year_min
    if year_max is not None:
        where.append("year <= :ymax"); params["ymax"] = year_max

    sql = f"""
        SELECT id
        FROM report
        WHERE {' AND '.join(where)}
        ORDER BY date DESC, id DESC
        {('LIMIT :lim' if limit is not None else '')}
    """
    if limit is not None:
        params["lim"] = limit

    cur.execute(sql, params)
    return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------
# Step 2 — fetch EFV rows for (report_id IN …) AND (section_name IN …)
#         NOTE: Uses exact section_name to keep (report_id, section_name) index usable.
# ---------------------------------------------------------------------
def _build_in_placeholders(prefix: str, values: List[Any]) -> Tuple[str, Dict[str, Any]]:
    ph_names = [f":{prefix}{i}" for i in range(len(values))]
    clause = ", ".join(ph_names)
    params = {f"{prefix}{i}": v for i, v in enumerate(values)}
    return clause, params

def fetch_efv_by_reports_and_sections(
    conn: sqlite3.Connection,
    report_ids: List[int],
    section_names: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    if not report_ids or not section_names:
        return {"variables": [], "factors": [], "events": []}

    rid_clause, rid_params = _build_in_placeholders("rid", report_ids)
    sec_clause, sec_params = _build_in_placeholders("sn", section_names)
    params = {**rid_params, **sec_params}

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    q_var = f"""
        SELECT id, report_id, section_name, name, value, unit, period, evidence
        FROM variable
        WHERE report_id IN ({rid_clause})
          AND section_name IN ({sec_clause})
        ORDER BY report_id DESC, id DESC
    """
    q_fac = f"""
        SELECT id, report_id, section_name, name, period, evidence
        FROM factor
        WHERE report_id IN ({rid_clause})
          AND section_name IN ({sec_clause})
        ORDER BY report_id DESC, id DESC
    """
    q_evt = f"""
        SELECT id, report_id, section_name, name, event_type, period, evidence
        FROM event
        WHERE report_id IN ({rid_clause})
          AND section_name IN ({sec_clause})
        ORDER BY report_id DESC, id DESC
    """

    cur.execute(q_var, params); variables = [dict(r) for r in cur.fetchall()]
    cur.execute(q_fac, params); factors   = [dict(r) for r in cur.fetchall()]
    cur.execute(q_evt, params); events    = [dict(r) for r in cur.fetchall()]

    return {"variables": variables, "factors": factors, "events": events}


# ---------------------------------------------------------------------
# Event link bonus — how many variables/factors linked via event_relation
# ---------------------------------------------------------------------
def fetch_event_links(conn: sqlite3.Connection, event_ids: List[int]) -> Dict[int, Dict[str, int]]:
    if not event_ids:
        return {}
    clause, params = _build_in_placeholders("eid", event_ids)
    sql = f"""
        SELECT event_id,
               SUM(CASE WHEN factor_id   IS NOT NULL THEN 1 ELSE 0 END) AS factor_links,
               SUM(CASE WHEN variable_id IS NOT NULL THEN 1 ELSE 0 END) AS variable_links
        FROM event_relation
        WHERE event_id IN ({clause})
        GROUP BY event_id
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    out = {}
    for r in cur.fetchall():
        out[int(r["event_id"])] = {
            "factor_links": int(r["factor_links"] or 0),
            "variable_links": int(r["variable_links"] or 0),
        }
    return out


# ---------------------------------------------------------------------
# Step 3 — rank & recommend
#   Simple scoring:
#     variables/factors: score = freq + 0.1 * recency_rank
#     events:            score = freq + 0.1 * recency_rank + link_bonus
#   recency_rank is 1..N (newer is larger) based on max(rowid) as proxy.
# ---------------------------------------------------------------------
def _rank_items(rows: List[Dict[str, Any]], key_fields: List[str], k: int) -> List[Dict[str, Any]]:
    """
    Aggregate by key_fields (case-insensitive for name). Compute freq and recency proxy.
    Returns top-k with {"name", "...", "freq", "score"}.
    """
    from collections import defaultdict

    # recency proxy: use max(report_id) or max(id) if available
    def recency_val(r: Dict[str, Any]) -> int:
        return int(r.get("report_id") or r.get("id") or 0)

    bucket = defaultdict(lambda: {"freq": 0, "max_recency": 0, "examples": []})

    for r in rows:
        key = []
        for f in key_fields:
            val = r.get(f)
            key.append(val.lower().strip() if isinstance(val, str) else val)
        key = tuple(key)

        b = bucket[key]
        b["freq"] += 1
        rv = recency_val(r)
        if rv > b["max_recency"]:
            b["max_recency"] = rv
        if len(b["examples"]) < 3:
            b["examples"].append(r)

    # rank by recency then freq
    ranked = sorted(bucket.items(), key=lambda kv: (kv[1]["max_recency"], kv[1]["freq"]), reverse=True)

    # build output with representative fields
    out = []
    for key, agg in ranked[:k]:
        exemplar = agg["examples"][0]
        item = {f: exemplar.get(f) for f in key_fields if f in exemplar}
        # include display name if available
        if "name" in exemplar:
            item["name"] = exemplar["name"]
        item["freq"] = agg["freq"]
        item["score"] = agg["freq"] + 0.1  # tiny base; recency already in ordering
        item["examples"] = agg["examples"]
        out.append(item)
    return out


def rank_recommendations(
    conn: sqlite3.Connection,
    raw: Dict[str, List[Dict[str, Any]]],
    k: int = 10
) -> Dict[str, List[Dict[str, Any]]]:
    # Variables: group by name
    vars_ranked = _rank_items(raw["variables"], key_fields=["name"], k=k)

    # Factors: group by name
    facts_ranked = _rank_items(raw["factors"], key_fields=["name"], k=k)

    # Events: group by (name, event_type); add link bonus
    ev_rows = raw["events"]
    ev_ranked = _rank_items(ev_rows, key_fields=["name", "event_type"], k=k)

    # link bonus
    ev_ids = [r["id"] for r in ev_rows if "id" in r]
    link_map = fetch_event_links(conn, ev_ids)
    for e in ev_ranked:
        # find a representative event_id from examples
        examples = e.get("examples", [])
        any_id = next((ex.get("id") for ex in examples if ex.get("id") is not None), None)
        links = link_map.get(any_id or -1, {"factor_links": 0, "variable_links": 0})
        link_bonus = 0.5 * int(links["factor_links"] > 0) + 0.5 * int(links["variable_links"] > 0)
        e["link_bonus"] = link_bonus
        e["score"] = e.get("score", 0) + link_bonus

    # final sort by score then freq
    ev_ranked.sort(key=lambda x: (x["score"], x["freq"]), reverse=True)
    vars_ranked.sort(key=lambda x: (x["score"], x["freq"]), reverse=True)
    facts_ranked.sort(key=lambda x: (x["score"], x["freq"]), reverse=True)

    # strip heavy example payload if you want lean output (keep top-1 example)
    for col in (vars_ranked, facts_ranked, ev_ranked):
        for item in col:
            ex = item.get("examples") or []
            item["example"] = ex[0] if ex else None
            item.pop("examples", None)

    return {"variables": vars_ranked, "factors": facts_ranked, "events": ev_ranked}


# ---------------------------------------------------------------------
# High-level API
# ---------------------------------------------------------------------
def recommend_for_company_sections(
    company_name: str,
    section_names: Optional[Union[str, Iterable[str]]],
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    report_limit: Optional[int] = None,
    k: int = 10,
) -> Dict[str, Any]:
    """
    1) Resolve report IDs for company (optional year window / limit),
    2) Fetch EFV rows once per table,
    3) Rank and return recommendations.
    """
    db_path = settings.database_path
    conn = sqlite3.connect(db_path)
    try:
        report_ids = get_report_ids_for_company(conn, company_name, year_min, year_max, report_limit)
        sects = _normalize_sections(section_names)
        raw = fetch_efv_by_reports_and_sections(conn, report_ids, sects)
        ranked = rank_recommendations(conn, raw, k=k)
        return {
            "query": {
                "company": company_name,
                "sections": sects,
                "year_min": year_min,
                "year_max": year_max,
                "report_limit": report_limit,
                "top_k": k,
            },
            "recommendations": ranked
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def _parse_args():
    p = argparse.ArgumentParser(description="Recommend EFV items for a given company and section(s).")
    p.add_argument("--company", required=True, help="Company name (matches report.company_name, case-insensitive).")
    p.add_argument("--sections", nargs="+", required=True, help="One or more section names (exact match recommended).")
    p.add_argument("--k", type=int, default=10, help="Top-K per category.")
    p.add_argument("--year-min", type=int, default=None, help="Minimum year (inclusive).")
    p.add_argument("--year-max", type=int, default=None, help="Maximum year (inclusive).")
    p.add_argument("--report-limit", type=int, default=None, help="Max number of reports (most recent first).")
    p.add_argument("--out", default=None, help="Optional JSON output path.")
    return p.parse_args()


def main():
    args = _parse_args()
    res = recommend_for_company_sections(
        company_name=args.company,
        section_names=args.sections,
        year_min=args.year_min,
        year_max=args.year_max,
        report_limit=args.report_limit,
        k=args.k,
    )
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    res = recommend_for_company_sections(
        company_name="Amazon.com, Inc.",
        section_names=[
            "liquidity and debt structure",
        ],
        k=12,
    )
    print(res)
