# recommend_compass.py  (canonical-graph score version, with hybrid weights as params)
# Use sentence-level edge weights to accumulate node importance;
# Support canonical mapping aggregation; Output three views: company, global, and hybrid.
import os
import json
import argparse
import sqlite3
from math import isfinite
from typing import Iterable, List, Dict, Any, Optional, Union, Tuple

from source import settings

def _round_number(x: float) -> float:
    """round score globally with SCORE_NDIGITS."""
    try:
        return round(float(x), 3) if isfinite(float(x)) else 0.0
    except Exception:
        return 0.0

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
    return [str(s).strip().lower() for s in items if s is not None and str(s).strip()]

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Check if a given table exists in the SQLite database."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def _in_clause(names: List[Any]) -> str:
    # Generate a placeholder string like '?, ?, ?' for SQL IN clause
    return ", ".join(["?"] * len(names)) if names else "?"

def _topk(items: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    """Return Top-K items sorted by score, then frequency."""
    return sorted(items, key=lambda x: (x.get("score", 0.0), x.get("freq", 0)), reverse=True)[: max(0, int(k))]

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
    aggregate: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetch importance scores for the given node type (event/factor/variable) within the specified section_ids.

    If aggregate=False:
        - Return raw-level nodes directly, without canonical aggregation.

    If aggregate=True:
        1) Aggregate edge weights by raw node_id in event_relation.
        2) If mapping tables exist, map raw nodes to canonical nodes.
        3) Aggregate again at the canonical node level.
        Returns: [{canonical_id, name, score, freq}]
    """
    if not section_ids:
        return []

    # Get table and column configurations for the given node type
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

    # Step 1: Query raw node importance (sum of edge weights) from event_relation
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

    # ---------- Return raw-level nodes directly ----------
    if not aggregate:
        return [
            {
                "canonical_id": None,
                "name": raw_name,
                "score": float(raw_score or 0.0),
                "freq": 1
            }
            for raw_id, raw_name, raw_score in rows
        ]

    # ---------- Canonical aggregation logic ----------
    has_map = _table_exists(conn, map_table) and _table_exists(conn, canon_table)
    results: Dict[str, Dict[str, Any]] = {}

    if has_map:
        # Step 2: Fetch mapping information between raw nodes and canonical nodes
        raw_ids = [r[0] for r in rows]
        if not raw_ids:
            return []

        raw_place = _in_clause(raw_ids)
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

        # Step 3: Aggregate raw nodes into canonical-level groups
        for raw_id, raw_name, raw_score in rows:
            canon = mapping.get(raw_id)
            if canon:
                key = f"canon:{canon[0]}"
                name = canon[1]
                cid = canon[0]
            else:
                key = f"raw:{raw_id}"
                name = raw_name
                cid = None  # No mapping found, keep as raw
            slot = results.setdefault(
                key, {"canonical_id": cid, "name": name, "score": 0.0, "freq": 0}
            )
            slot["score"] += float(raw_score or 0.0)
            slot["freq"] += 1
    else:
        # If no mapping tables exist, treat raw nodes as pseudo-canonical nodes
        for raw_id, raw_name, raw_score in rows:
            key = f"raw:{raw_id}"
            slot = results.setdefault(
                key, {"canonical_id": None, "name": raw_name, "score": 0.0, "freq": 0}
            )
            slot["score"] += float(raw_score or 0.0)
            slot["freq"] += 1

    return list(results.values())

def _dedup_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate by (canonical_id, normalized name). Keep the highest score for that key."""
    best: Dict[Tuple[Optional[int], str], Dict[str, Any]] = {}
    for x in items or []:
        cid = x.get("canonical_id")
        name = (x.get("name") or "").strip().lower()
        key = (cid, name)
        cur = best.get(key)
        if cur is None or (float(x.get("score", 0)) > float(cur.get("score", 0))):
            best[key] = x
    return list(best.values())

# -----------------------------
# Hybrid ranking helpers
# -----------------------------
def _key_for_item(x: Dict[str, Any]) -> Tuple[Optional[int], str]:
    """Stable key for joining company/global lists."""
    return (x.get("canonical_id"), (x.get("name") or "").strip().lower())

def _rank_normalize(items: List[Dict[str, Any]]) -> Dict[Tuple[Optional[int], str], float]:
    """
    Convert the ordering by 'score' into a [0,1] rank score (1st -> 1.0).
    Robust to scale differences across scopes.
    """
    if not items:
        return {}
    # sort desc by raw score; tie-breaking by freq desc
    sorted_items = sorted(items, key=lambda x: (float(x.get("score", 0.0)), int(x.get("freq", 0))), reverse=True)
    n = len(sorted_items)
    out: Dict[Tuple[Optional[int], str], float] = {}
    for i, it in enumerate(sorted_items, start=1):
        k = _key_for_item(it)
        out[k] = (n - i + 1) / n
    return out

def _freq_normalize(items: List[Dict[str, Any]]) -> Dict[Tuple[Optional[int], str], float]:
    """Normalize freq to [0,1] for a light tie-breaker."""
    if not items:
        return {}
    maxf = max(int(x.get("freq", 0)) for x in items) or 1
    out: Dict[Tuple[Optional[int], str], float] = {}
    for it in items:
        out[_key_for_item(it)] = int(it.get("freq", 0)) / maxf
    return out

def _blend_two_lists(
    comp_list: List[Dict[str, Any]],
    glob_list: List[Dict[str, Any]],
    *,
    w_comp: float = 0.55,     # company rank weight
    w_glob: float = 0.45,     # global rank weight
    w_freq: float = 0.05,     # tiny freq bonus (applied to max(comp, glob) freq)
    both_bonus: float = 0.05  # appears in both lists gets a small bump
) -> List[Dict[str, Any]]:
    """
    Join by (canonical_id, name), then mix normalized rank scores.
    Returns a list with updated 'score' (the mixed score), and merged freq (max of both).
    """
    comp_rank = _rank_normalize(comp_list)
    glob_rank = _rank_normalize(glob_list)
    comp_freq = _freq_normalize(comp_list)
    glob_freq = _freq_normalize(glob_list)

    comp_map = {_key_for_item(x): x for x in comp_list}
    glob_map = {_key_for_item(x): x for x in glob_list}

    keys = set(comp_map.keys()) | set(glob_map.keys())
    blended: List[Dict[str, Any]] = []
    for k in keys:
        ci = comp_map.get(k)
        gi = glob_map.get(k)

        # base meta
        base = ci or gi
        cid = base.get("canonical_id")
        name = base.get("name")

        r_comp = comp_rank.get(k, 0.0)
        r_glob = glob_rank.get(k, 0.0)
        f_comp = comp_freq.get(k, 0.0)
        f_glob = glob_freq.get(k, 0.0)

        score = (w_comp * r_comp) + (w_glob * r_glob) + (w_freq * max(f_comp, f_glob))
        if ci is not None and gi is not None:
            score += both_bonus

        freq_val = max(int(ci.get("freq", 0)) if ci else 0,
                       int(gi.get("freq", 0)) if gi else 0)

        blended.append({
            "canonical_id": cid,
            "name": name,
            "score": float(score) if isfinite(score) else 0.0,
            "freq": int(freq_val),
        })

    blended.sort(key=lambda x: (x["score"], x.get("freq", 0)), reverse=True)
    return blended

# -----------------------------
# Ranking: company scope & global scope (+ hybrid)
# -----------------------------
def rank_recommendations_graph(
    conn: sqlite3.Connection,
    company_name: str,
    section_names: List[str],
    year_min: Optional[int],
    year_max: Optional[int],
    report_limit: Optional[int],
    k: int,
    *,
    # hybrid weights as function inputs
    w_comp: float = 0.55,
    w_glob: float = 0.45,
    w_freq: float = 0.05,
    both_bonus: float = 0.05,
) -> Dict[str, Any]:
    """
    Rank EFV items using sentence-level relationship edge weights (event_relation.score).

    Views:
      - company_view: restrict to this company's reports/sections; DO NOT aggregate to canonical
                      (aggregate=False) so that raw nodes are preserved.
      - global_view:  across all companies for the same section names; aggregate to canonical
                      (aggregate=True) to merge duplicates and clean up names.
      - hybrid_view:  blend company_canon_view (company scope + canonical) and global_view with weights.

    Returns:
      {
        "company_view": {"events": [...], "factors": [...], "variables": [...]},
        "global_view":  {"events": [...], "factors": [...], "variables": [...]},
        "hybrid_view":  {"events": [...], "factors": [...], "variables": [...]}
      }
      Each list item: {"canonical_id": <id or None>, "name": <canonical/raw name>, "score": <float>, "freq": <int>}
    """
    # Resolve section ids for the company scope
    report_ids = get_report_ids_for_company(
        conn=conn,
        company_name=company_name,
        year_min=year_min,
        year_max=year_max,
        limit=report_limit,
    )
    comp_section_ids = get_section_ids_for_company_sections(
        conn=conn,
        report_ids=report_ids,
        section_names=section_names,
    )

    # Resolve section ids for the global scope (same section names, all companies)
    global_section_ids = get_section_ids_global_by_names(
        conn=conn,
        section_names=section_names,
    )

    def pack_view(section_ids: List[int], aggregate: bool) -> Dict[str, List[Dict[str, Any]]]:
        """Collect Top-K per category for a given set of section_ids with/without canonical aggregation."""
        ev = _fetch_canonical_scores_for_scope(conn, section_ids, "event",    aggregate=aggregate)
        fa = _fetch_canonical_scores_for_scope(conn, section_ids, "factor",   aggregate=aggregate)
        va = _fetch_canonical_scores_for_scope(conn, section_ids, "variable", aggregate=aggregate)

        ev = _dedup_items(ev)
        fa = _dedup_items(fa)
        va = _dedup_items(va)
        return {
            "events":    _topk(ev, k),
            "factors":   _topk(fa, k),
            "variables": _topk(va, k),
        }

    # Company view: raw granularity (no canonical aggregation)
    company_view = pack_view(comp_section_ids, aggregate=False)

    # Global view: canonical aggregation (merge across companies)
    global_view  = pack_view(global_section_ids, aggregate=True)

    # Company canonical view (company scope + canonical) for hybrid
    company_canon_view = pack_view(comp_section_ids, aggregate=True)

    # Build hybrid by blending company_canon with global
    def _mk_hybrid(comp_v: Dict[str, List[Dict[str, Any]]],
                   glob_v: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        return {
            "events":    _blend_two_lists(comp_v.get("events",    []), glob_v.get("events",    []),
                                          w_comp=w_comp, w_glob=w_glob, w_freq=w_freq, both_bonus=both_bonus),
            "factors":   _blend_two_lists(comp_v.get("factors",   []), glob_v.get("factors",   []),
                                          w_comp=w_comp, w_glob=w_glob, w_freq=w_freq, both_bonus=both_bonus),
            "variables": _blend_two_lists(comp_v.get("variables", []), glob_v.get("variables", []),
                                          w_comp=w_comp, w_glob=w_glob, w_freq=w_freq, both_bonus=both_bonus),
        }

    hybrid_view = _mk_hybrid(company_canon_view, global_view)

    # -------- Final pass: round scores for ALL views (ensures hybrid is rounded too) --------
    def _round_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for it in items or []:
            d = dict(it)
            d["score"] = _round_number(d.get("score", 0.0))
            # keep freq as int (robust)
            try:
                d["freq"] = int(d.get("freq", 0))
            except Exception:
                d["freq"] = 0
            out.append(d)
        return out

    def _round_view(view: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        return {
            "events":    _round_items(view.get("events",    [])),
            "factors":   _round_items(view.get("factors",   [])),
            "variables": _round_items(view.get("variables", [])),
        }

    company_view = _round_view(company_view)
    global_view  = _round_view(global_view)
    hybrid_view  = _round_view(hybrid_view)

    return {
        "company_view": company_view,
        "global_view":  global_view,
        "hybrid_view":  hybrid_view,
    }


# -----------------------------
# NEW: adapter to UI shape (variables/factors/events lists), with per-type k & use_global
# -----------------------------
def _as_ui_rows(items: List[Dict[str, Any]], section_display: str, k: int) -> List[Dict[str, Any]]:
    """
    Convert canonical items to UI rows expected by the Tk UI:
      id -> canonical_id (if any), section -> section_display, evidence -> canonical name
      Also include score/freq for debugging/inspection.
    """
    rows = []
    for it in _topk(items, k):
        rows.append({
            "id": it.get("canonical_id", ""),    # could be None -> ""
            "section": section_display,
            "evidence": it.get("name", ""),
            "score": it.get("score", 0.0),
            "freq": it.get("freq", 0),
        })
    return rows

def recommend(
    company_name: str,
    section_names: Optional[Union[str, Iterable[str]]],
    *,
    k_var: int = 8,
    k_factor: int = 6,
    k_event: int = 6,
    use_global: bool = True,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    report_limit: Optional[int] = None,
    w_comp: float = settings.weight_company,
    w_glob: float = settings.weight_global,
    w_freq: float = settings.weight_frequency,
    both_bonus: float = settings.both_bonus,
) -> Dict[str, Any]:
    """
    Adapter for the UI:
      - runs the graph ranker,
      - picks one view by `use_global` (True -> hybrid_view; False -> company_view),
      - returns {variables, factors, events} where each is a list of {id, section, evidence, score, freq}.
    """
    sects = _normalize_sections(section_names)
    section_display = ", ".join(sects) if sects else ""

    db_path = settings.database_path
    conn = sqlite3.connect(db_path)
    try:
        views = rank_recommendations_graph(
            conn=conn,
            company_name=company_name,
            section_names=sects,
            year_min=year_min,
            year_max=year_max,
            report_limit=report_limit,
            k=max(k_var, k_factor, k_event),  # compute enough, subselect per type below
            w_comp=w_comp, w_glob=w_glob, w_freq=w_freq, both_bonus=both_bonus,
        )
        # use_global=True -> hybrid ranking
        chosen = views["hybrid_view"] if use_global else views["company_view"]

        return {
            "variables": _as_ui_rows(chosen.get("variables", []), section_display, k_var),
            "factors":   _as_ui_rows(chosen.get("factors", []),   section_display, k_factor),
            "events":    _as_ui_rows(chosen.get("events", []),    section_display, k_event),
        }
    finally:
        conn.close()



if __name__ == "__main__":
    # Example: UI adapter API quick smoke test
    res2 = recommend(
        company_name="ASML Holding N.V.",
        section_names=["liquidity and debt structure"],
        k_var=10, k_factor=10, k_event=10,
        use_global=True,    # hybrid
        w_comp=0.6, w_glob=0.4, w_freq=0.05, both_bonus=0.05,
    )
    print(res2)
