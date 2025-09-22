# recommend_compass.py  (canonical-graph score version)
# Use sentence-level edge weights to accumulate node importance;
# Support canonical mapping aggregation; Output two views: company-specific and global industry.
import os
import json
import argparse
import sqlite3
from typing import Iterable, List, Dict, Any, Optional, Union, Tuple

import settings

# -----------------------------
# Helpers
# -----------------------------
def _normalize_sections(section_names: Optional[Union[str, Iterable[str]]]) -> List[str]:
    """Normalize section_names to a clean list of lowercase strings."""
    if section_names is None:
        return []
    if isinstance(section_names, str):
        items = [section_names]
    else:
        items = list(section_names)
    return [s.strip() for s in items if s is not None and str(s).strip()]

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Check if a given table exists in the SQLite database."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def _in_clause(names: List[str]) -> str:
    # Generate a placeholder string like '?, ?, ?' for SQL IN clause
    return ", ".join(["?"] * len(names))

def _topk(items: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    """Return Top-K items sorted by score, then frequency."""
    return sorted(items, key=lambda x: (x.get("score", 0.0), x.get("freq", 0)), reverse=True)[:k]

# -----------------------------
# Resolve report_ids and section_ids
# -----------------------------
def get_report_ids_for_company(
    conn: sqlite3.Connection,
    company_name: str,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[int]:
    """Fetch report IDs for a given company with optional year filtering."""
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
    return [r[0] for r in cur.fetchall()]

def get_section_ids_for_company_sections(
    conn: sqlite3.Connection,
    report_ids: List[int],
    section_names: List[str],
) -> List[int]:
    """Get section_ids restricted to given report_ids and section_names."""
    if not report_ids or not section_names:
        return []
    cur = conn.cursor()
    sec_place = _in_clause(section_names)
    rep_place = _in_clause(report_ids)
    sql = f"""
        SELECT id
        FROM report_sections
        WHERE report_id IN ({rep_place})
          AND LOWER(section_name) IN ({sec_place})
    """
    params = [*report_ids, *[s.lower() for s in section_names]]
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]

def get_section_ids_global_by_names(conn: sqlite3.Connection, section_names: List[str]) -> List[int]:
    """Global scope: fetch all matching section_ids only by section_name, not limited to a single company."""
    if not section_names:
        return []
    cur = conn.cursor()
    sec_place = _in_clause(section_names)
    sql = f"""
        SELECT id
        FROM report_sections
        WHERE LOWER(section_name) IN ({sec_place})
    """
    params = [s.lower() for s in section_names]
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]

# -----------------------------
# Canonical aggregation per type
# -----------------------------
def _canonical_query_parts(node_type: str) -> Dict[str, str]:
    """
    Return table and column names for the given node type.
    node_type ∈ {'event','factor','variable'}
    """
    return {
        "node_table": node_type,                       # event / factor / variable
        "id_col": "id",
        "name_col": "name",
        "map_table": f"{node_type}_to_canonical_map",  # may not exist
        "map_node_id_col": f"{node_type}_id",
        "canon_table": f"canonical_{node_type}",
        "canon_id_col": "id",
        "canon_name_col": "canonical_name",
        "er_col": f"{node_type}_id",                  # column name in event_relation
    }

def _fetch_canonical_scores_for_scope(
    conn: sqlite3.Connection,
    section_ids: List[int],
    node_type: str,
) -> List[Dict[str, Any]]:
    """
    Within the given section_ids:
      1) Aggregate edge weights (importance) by raw node_id in event_relation.
      2) Left join to canonical mapping to map to canonical_id (fallback to raw if no mapping exists).
      3) Aggregate again by canonical_id/name at Python side.
    Return list: [{canonical_id, name, score, freq}]
    """
    if not section_ids:
        return []

    p = _canonical_query_parts(node_type)
    node_table = p["node_table"]
    id_col = p["id_col"]
    name_col = p["name_col"]
    er_col = p["er_col"]
    map_table = p["map_table"]
    map_node_id_col = p["map_node_id_col"]
    canon_table = p["canon_table"]
    canon_id_col = p["canon_id_col"]
    canon_name_col = p["canon_name_col"]

    has_map = _table_exists(conn, map_table) and _table_exists(conn, canon_table)

    # Step 1: Aggregate edge weights by raw node_id from event_relation
    # Note: Only count rows where er.<type>_id is NOT NULL
    cur = conn.cursor()
    sec_place = _in_clause(section_ids)
    sql_base = f"""
        SELECT n.{id_col} AS raw_id,
               n.{name_col} AS raw_name,
               SUM(er.score) AS raw_score
        FROM event_relation er
        JOIN {node_table} n ON er.{er_col} = n.{id_col}
        WHERE er.section_id IN ({sec_place})
        GROUP BY n.{id_col}, n.{name_col}
    """
    cur.execute(sql_base, section_ids)
    rows = cur.fetchall()

    # Step 2: Map to canonical (if mapping tables exist)
    results: Dict[str, Dict[str, Any]] = {}  # key -> {canonical_id,name,score,freq}
    if has_map:
        # Fetch mapping for all raw_ids
        raw_ids = [r[0] for r in rows]
        if not raw_ids:
            return []
        raw_place = _in_clause(raw_ids)
        # raw_id -> (canon_id, canon_name)
        sql_map = f"""
            SELECT m.{map_node_id_col} AS raw_id,
                   c.{canon_id_col}     AS canon_id,
                   c.{canon_name_col}   AS canon_name
            FROM {map_table} m
            JOIN {canon_table} c ON m.canonical_id = c.{canon_id_col}
            WHERE m.{map_node_id_col} IN ({raw_place})
        """
        cur.execute(sql_map, raw_ids)
        mapping = {rid: (cid, cname) for (rid, cid, cname) in cur.fetchall()}

        # Aggregate into canonical-level groups
        for raw_id, raw_name, raw_score in rows:
            canon = mapping.get(raw_id)
            if canon:
                key = f"canon:{canon[0]}"
                name = canon[1]
                cid = canon[0]
            else:
                key = f"raw:{raw_id}"
                name = raw_name
                cid = None  # None indicates no canonical mapping
            slot = results.setdefault(key, {"canonical_id": cid, "name": name, "score": 0.0, "freq": 0})
            slot["score"] += float(raw_score or 0.0)
            slot["freq"]  += 1
    else:
        # If no mapping table, treat raw nodes as pseudo-canonical
        for raw_id, raw_name, raw_score in rows:
            key = f"raw:{raw_id}"
            slot = results.setdefault(key, {"canonical_id": None, "name": raw_name, "score": 0.0, "freq": 0})
            slot["score"] += float(raw_score or 0.0)
            slot["freq"]  += 1

    # Convert dict to list
    out = []
    for _, v in results.items():
        out.append(v)
    return out

# -----------------------------
# Ranking: company scope & global scope
# -----------------------------
def rank_recommendations_graph(
    conn: sqlite3.Connection,
    company_name: str,
    section_names: List[str],
    year_min: Optional[int],
    year_max: Optional[int],
    report_limit: Optional[int],
    k: int,
) -> Dict[str, Any]:
    """
    Based on sentence-level relationship edge weights (event_relation.score):
      - Compute canonical aggregated Top-K for company + sections (company-specific view).
      - Compute canonical aggregated Top-K for same-named sections across all companies (global industry view).
    """
    # Company-specific: restrict first by report_ids, then find section_ids
    report_ids = get_report_ids_for_company(conn, company_name, year_min, year_max, report_limit)
    comp_section_ids = get_section_ids_for_company_sections(conn, report_ids, section_names)

    # Global industry: only use section_name to get all section_ids
    global_section_ids = get_section_ids_global_by_names(conn, section_names)

    def pack_view(section_ids: List[int]) -> Dict[str, List[Dict[str, Any]]]:
        ev = _fetch_canonical_scores_for_scope(conn, section_ids, "event")
        fa = _fetch_canonical_scores_for_scope(conn, section_ids, "factor")
        va = _fetch_canonical_scores_for_scope(conn, section_ids, "variable")
        return {
            "events":    _topk(ev, k),
            "factors":   _topk(fa, k),
            "variables": _topk(va, k),
        }

    company_view = pack_view(comp_section_ids)
    global_view  = pack_view(global_section_ids)

    return {
        "company_view": company_view,   # Top-K for this company only
        "global_view":  global_view,    # Cross-company Top-K for same section_name
    }

# -----------------------------
# High-level API (keep original entry & return both views)
# -----------------------------
def recommend_for_company_sections(
    company_name: str,
    section_names: Optional[Union[str, Iterable[str]]],
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    report_limit: Optional[int] = None,
    k: int = 10,
) -> Dict[str, Any]:
    """
    New logic:
      - Replace old "frequency + recency" method with graph edge weight accumulation (with canonical aggregation).
      - Return both company_view and global_view Top-K results.
    """
    db_path = settings.database_path
    sects = _normalize_sections(section_names)
    conn = sqlite3.connect(db_path)
    try:
        views = rank_recommendations_graph(
            conn=conn,
            company_name=company_name,
            section_names=sects,
            year_min=year_min,
            year_max=year_max,
            report_limit=report_limit,
            k=k,
        )
        return {
            "query": {
                "company": company_name,
                "sections": sects,
                "year_min": year_min,
                "year_max": year_max,
                "report_limit": report_limit,
                "top_k": k,
            },
            "recommendations": views  # {"company_view":{...}, "global_view":{...}}
        }
    finally:
        conn.close()

# -----------------------------
# CLI (keep unchanged)
# -----------------------------
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
        section_names=["liquidity and debt structure"],
        k=12,
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))
