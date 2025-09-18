import os
import sqlite3
import json
from dateutil import parser
from selectolax.parser import HTMLParser
from typing import Iterable, List, Dict, Any, Optional, Union

from snowflake_generators import get_next_id
import str_utils
from source import settings

def initiate_database():
    """
    Initialize the SQLite database.
    This function will create:
      1. report                  - Metadata table for rating reports (added 'year' column)
      2. report_sections         - Detailed table for storing section-level content
      3. event                   - Extracted events (with name, evidence, event_type, period)
      4. factor                  - Extracted factors (with name, evidence, period)
      5. variable                - Logical variables (with value/unit/period_text/period)
      6. event_relation          - Relationship table linking event-factor-variable
    """
    db_path = settings.database_path

    # Ensure that the folder for the database file exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # ===============================
    # 1. Create 'report' table
    # ===============================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS report (
            id INTEGER PRIMARY KEY,                  -- Global unique ID (Snowflake)
            rating_company TEXT NOT NULL,            -- Rating agency name (e.g., Fitch)
            company_name TEXT,                       -- Rated company name
            title TEXT NOT NULL,                     -- Report title
            words INTEGER,                           -- Total word count
            date DATE,                               -- Report publication date (YYYY-MM-DD)
            year INTEGER,                            -- Publication year (for fast filtering/grouping)
            category TEXT,                           -- Report category
            code TEXT,                               -- Internal report code
            language TEXT,                           -- Language of the report
            copyright TEXT,                          -- Copyright information
            headings TEXT,                           -- JSON string of extracted headings
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- Record creation time
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- Last update time
        );
    """)

    # Index only for rating_company and year (no date included)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_report_company_year
        ON report(rating_company, year);
    """)

    # ===============================
    # 2. Create 'report_sections' table
    # ===============================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_sections (
            id INTEGER PRIMARY KEY,                  -- Global unique ID (Snowflake)
            report_id INTEGER NOT NULL,              -- Foreign key referencing report.id
            section_name TEXT NOT NULL,              -- Standardized section name
            contents TEXT NOT NULL,                  -- Full text of this section
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- Record creation time
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- Last update time,
            ,FOREIGN KEY(report_id) REFERENCES report(id) ON DELETE CASCADE
        );
    """)

    # Index for fast lookups by report_id and section_name
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sections_report
        ON report_sections(report_id, section_name);
    """)

    # ===============================
    # 3. Create 'event' table  (section_id kept, no FK)
    # ===============================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS event (
            id INTEGER PRIMARY KEY,                       -- Global unique ID (Snowflake style)
            report_id INTEGER NOT NULL,                   -- FK -> report.id
            section_id INTEGER,                           -- Keep for precise section mapping (no FK)
            section_name TEXT NOT NULL,                   -- Denormalized for name-based lookups
            name TEXT NOT NULL,                           -- Event name/title
            evidence TEXT,                                -- Verbatim snippet for auditability
            event_type TEXT,                              -- Event type/category
            period TEXT,                                  -- Period (e.g., 'FY2024', 'Q2 2025', 'Mar-2024')
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES report(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_report_section
        ON event(report_id, section_name);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_type
        ON event(event_type);
    """)

    # ===============================
    # 4. Create 'factor' table  (section_id kept, no FK)
    # ===============================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS factor (
            id INTEGER PRIMARY KEY,                       -- Global unique ID 
            report_id INTEGER NOT NULL,                   -- FK -> report.id
            section_id INTEGER,                           -- Keep (no FK)
            section_name TEXT NOT NULL,
            name TEXT NOT NULL,                           -- Factor name
            evidence TEXT,                                -- Verbatim snippet
            period TEXT,                                  -- Period text
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES report(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_factor_report_section
        ON factor(report_id, section_name);
    """)

    # ===============================
    # 5. Create 'variable' table  (section_id kept, no FK)
    # ===============================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS variable (
            id INTEGER PRIMARY KEY,                       -- Global unique ID 
            report_id INTEGER NOT NULL,                   -- FK -> report.id
            section_id INTEGER,                           -- Keep (no FK)
            section_name TEXT NOT NULL,
            name TEXT NOT NULL,                           -- Variable logical name
            value TEXT,                                   -- Parsed value (raw text)
            unit TEXT,                                    -- Unit (%, USD bn, etc.)
            period TEXT,                             -- Time period as text (e.g., 'FY2024', 'Q2 2025')
            evidence TEXT,                                -- Verbatim snippet
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES report(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_variable_report_section
        ON variable(report_id, section_name);
    """)

    # ===============================
    # 6. Create 'event_relation' table
    # ===============================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS event_relation (
            id INTEGER PRIMARY KEY,                       -- Global unique ID 
            event_id INTEGER,                             -- FK -> event.id
            factor_id INTEGER,                            -- FK -> factor.id (nullable)
            variable_id INTEGER,                          -- FK -> variable.id (nullable)
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(event_id) REFERENCES event(id) ON DELETE CASCADE,
            FOREIGN KEY(factor_id) REFERENCES factor(id) ON DELETE CASCADE,
            FOREIGN KEY(variable_id) REFERENCES variable(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_relation_event
        ON event_relation(event_id);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_relation_factor
        ON event_relation(factor_id);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_relation_variable
        ON event_relation(variable_id);
    """)


    conn.commit()
    conn.close()
    print(f"Database initialized successfully at: {db_path}")




def read_fitch_report_html(html_path):
    html = open(html_path, "r", encoding="utf-8").read()
    tree = HTMLParser(html)

    # CSS Selector
    title = tree.css_first("h1").text()
    sub_titles = [node.text() for node in tree.css("h2")]

def get_all_html_files(rating_report_file_path):
    """
    Read all HTML files under the specified folder.

    Parameters
    ----------
    rating_report_file_path : str
        Path to the folder containing rating report HTML files.

    Returns
    -------
    html_files : list
        A list of absolute paths to all HTML files found in the folder.
    """
    html_files = []

    # Walk through the folder to find all .html files
    for root, _, files in os.walk(rating_report_file_path):
        for file in files:
            if file.lower().endswith(".html"):
                full_path = os.path.join(root, file)
                html_files.append(full_path)
    return html_files


def new_id():
    return get_next_id()


def insert_reports(articles, rating_company="Fitch"):
    """
    Batch insert multiple parsed reports into the 'report' table.
    Each report will use a globally unique Snowflake ID.

    Parameters
    ----------
    articles : list of dict
        A list where each dict contains parsed report data:
            - title: str, report title
            - words: int, total word count
            - date: str, report date in 'YYYY-MM-DD'
            - category: str, report category/type
            - code: str, report code
            - language: str, language of the report
            - copyright: str, copyright statement
            - headings: list, list of section headings
    rating_company : str, default = "Fitch"
        The rating agency name.

    Returns
    -------
    list
        A list of generated report IDs, in the same order as the input list.
    """
    if not isinstance(articles, list) or len(articles) == 0:
        raise ValueError("Parameter 'articles' must be a non-empty list of dictionaries.")

        # Required keys
    required_keys = [
        "company_name", "title", "words", "date", "category",
        "code", "language", "copyright", "headings"
    ]

    db_path = settings.database_path
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    inserted_ids = []
    data_to_insert = []
    contents = []
    for article in articles:
        # Validate fields
        for key in required_keys:
            if key not in article:
                raise ValueError(f"Missing required field in article: {key}")

        # Generate unique Snowflake ID
        report_id = get_next_id()
        inserted_ids.append(report_id)

        # Parse 'year' from 'date'
        try:
            # parser.parse can handle '3 July 2025', '2025-07-03', etc.
            parsed_date = parser.parse(article["date"])
            date_str = parsed_date.strftime("%Y-%m-%d")  # unified format
            report_year = parsed_date.year
        except Exception:
            raise ValueError(f"Invalid date format for report: {article['date']}")

        # Convert headings list to JSON string
        headings_json = json.dumps(article["headings"], ensure_ascii=False)

        data_to_insert.append((
            report_id,
            rating_company,
            article["company_name"],
            article["title"],
            article["words"],
            date_str,
            report_year,  # auto-generated year
            article["category"],
            article["code"],
            article["language"],
            article["copyright"],
            headings_json
        ))


        body_text =article["body_text"]
        if body_text is None:
            continue
        for section in body_text.keys():
            content = body_text[section]
            contents.append((get_next_id(), report_id, section, content))


    # Perform batch insert
    # Batch insert into SQLite
    cur.executemany("""
               INSERT INTO report (
                   id, rating_company, company_name, title, words, date, year,
                   category, code, language, copyright, headings
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           """, data_to_insert)
    conn.commit()
    print(f"Inserted {len(inserted_ids)} reports into 'report' table.")

    cur.executemany("""
            INSERT INTO report_sections (id, report_id, section_name, contents)
            VALUES (?, ?, ?, ?)
        """, contents)
    conn.commit()
    print(f"Inserted {len(contents)} section into 'report_sections' table.")
    conn.close()


    return inserted_ids

def delete_reports_by_rating_company(rating_company):
    """
    Delete all reports in the 'report' table that match the given rating company.

    Parameters
    ----------
    rating_company : str
        The name of the rating agency to delete.
        Example: "Fitch"

    Returns
    -------
    int
        The number of deleted rows.
    """
    if not isinstance(rating_company, str) or not rating_company.strip():
        raise ValueError("Parameter 'rating_company' must be a non-empty string.")

    db_path = settings.database_path
    conn = sqlite3.connect(db_path)

    # Enable foreign key support to activate ON DELETE CASCADE
    conn.execute("PRAGMA foreign_keys = ON;")

    cur = conn.cursor()

    # Execute deletion
    cur.execute("DELETE FROM report WHERE rating_company = ?", (rating_company.strip(),))
    deleted_count = cur.rowcount  # Number of rows affected

    conn.commit()
    conn.close()

    print(f"Deleted {deleted_count} rows for rating company: {rating_company}")

    return deleted_count

def select_sections_from_db(section_names=None):
    """
    Load sections from 'report_sections' table, joined with report metadata.

    Parameters
    ----------
    section_names : list[str] or None
        - If None or empty, load ALL sections.
        - If list is provided, only load sections whose section_name matches
          any value in the list (case-insensitive).

    Returns
    -------
    list of tuples
        [
            (section_id, report_id, company_name, section_name, contents),
            ...
        ]
    """
    db_path = settings.database_path
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Enable foreign key support
    conn.execute("PRAGMA foreign_keys = ON;")

    # --- Build query dynamically ---
    if section_names and len(section_names) > 0:
        # Normalize section names to lowercase
        section_names = [name.lower().strip() for name in section_names]
        placeholders = ",".join("?" for _ in section_names)  # (?, ?, ?)
        query = f"""
            SELECT rs.id, rs.report_id, r.company_name, rs.section_name, rs.contents
            FROM report_sections AS rs
            JOIN report AS r ON rs.report_id = r.id
            WHERE LOWER(rs.section_name) IN ({placeholders})
        """
        cur.execute(query, section_names)
    else:
        # Load all sections
        query = """
            SELECT rs.id, rs.report_id, r.company_name, rs.section_name, rs.contents
            FROM report_sections AS rs
            JOIN report AS r ON rs.report_id = r.id
        """
        cur.execute(query)

    rows = cur.fetchall()
    conn.close()
    return rows

def _norm_period(p):
    """
    Normalize 'period' to a single string or None.
    Accepts string | list | None. Lists are joined with '; '.
    """
    if p is None:
        return None
    if isinstance(p, list):
        items = [str(x).strip() for x in p if x is not None and str(x).strip()]
        return "; ".join(items) if items else None
    s = str(p).strip()
    return s or None

def _safe(s):
    """
    Trim strings and convert empty strings to None.
    """
    if s is None:
        return None
    s = str(s).strip()
    return s if s else None

def insert_efv_rows(rows):
    """
    Insert EFV JSON rows into three tables: event, factor, variable,
    using application-generated Snowflake-like IDs (new_id()).

    Input format (per row):
      {
        "report_id": int | None,
        "section_id": int | None,          # optional; no FK constraint
        "section_name": str | None,
        "events": [
            {"name": str, "event_type": str, "period": str|list|null, "evidence": str}, ...
        ],
        "factors": [
            {"name": str, "period": str|list|null, "evidence": str}, ...
        ],
        "variables": [
            {"name": str, "value": str, "unit": str|null, "period": str|null, "evidence": str}, ...
        ]
      }

    Returns:
      dict: counts of inserted rows for each table.
    """
    if not rows:
        return {"events": 0, "factors": 0, "variables": 0}

    conn = sqlite3.connect(settings.database_path)
    cur = conn.cursor()

    # Explicitly insert IDs generated by new_id()
    ev_sql = """
        INSERT INTO event (id, report_id, section_id, section_name, name, evidence, event_type, period)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    fa_sql = """
        INSERT INTO factor (id, report_id, section_id, section_name, name, evidence, period)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    var_sql = """
        INSERT INTO variable (id, report_id, section_id, section_name, name, value, unit, period, evidence)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    ev_rows, fa_rows, var_rows = [], [], []

    # Build batched parameter lists
    for row in rows:
        report_id = row.get("report_id")
        section_id = row.get("section_id")  # may be None
        section_name = row.get("section_name")

        for ev in row.get("events", []):
            ev_rows.append((
                new_id(),                          # id
                report_id,
                section_id,
                _safe(section_name),
                _safe(ev.get("name")),
                _safe(ev.get("evidence")),
                _safe(ev.get("event_type")),
                _norm_period(ev.get("period")),
            ))

        for fa in row.get("factors", []):
            fa_rows.append((
                new_id(),                          # id
                report_id,
                section_id,
                _safe(section_name),
                _safe(fa.get("name")),
                _safe(fa.get("evidence")),
                _norm_period(fa.get("period")),
            ))

        for va in row.get("variables", []):
            var_rows.append((
                new_id(),                          # id
                report_id,
                section_id,
                _safe(section_name),
                _safe(va.get("name")),
                _safe(va.get("value")),
                _safe(va.get("unit")),
                _norm_period(va.get("period")),
                _safe(va.get("evidence")),
            ))

    # Single transaction for performance and atomicity
    try:
        conn.execute("BEGIN")
        if ev_rows:
            cur.executemany(ev_sql, ev_rows)
        if fa_rows:
            cur.executemany(fa_sql, fa_rows)
        if var_rows:
            cur.executemany(var_sql, var_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"events": len(ev_rows), "factors": len(fa_rows), "variables": len(var_rows)}

    """
    Delete rows from {event, factor, variable} by section_name.
    Optionally constrain by report_id to avoid cross-report deletions.

    Args:
        section_name: Exact section_name to match (case-sensitive by default in SQLite).
        report_id: Optional report_id filter; if provided, deletes only rows for this report.
        dry_run: If True, do not delete—return the would-be deletion counts.

    Returns:
        dict: {"event": n1, "factor": n2, "variable": n3, "total": n}
    """
    if not section_name or not str(section_name).strip():
        raise ValueError("section_name must be a non-empty string.")

    params = {"section_name": section_name}
    where_clause = "section_name = :section_name"

    if report_id is not None:
        where_clause += " AND report_id = :report_id"
        params["report_id"] = report_id

    # Build count and delete statements
    tables = ["event", "factor", "variable"]
    count_sql = {t: f"SELECT COUNT(1) FROM {t} WHERE {where_clause}" for t in tables}
    delete_sql = {t: f"DELETE FROM {t} WHERE {where_clause}" for t in tables}

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")  # ensure cascades (e.g., event_relation) are enforced
    cur = conn.cursor()

    # Compute would-be counts first
    counts = {}
    for t in tables:
        cur.execute(count_sql[t], params)
        counts[t] = cur.fetchone()[0]

    if dry_run:
        counts["total"] = sum(counts.values())
        conn.close()
        return counts

    # Perform deletion in a single transaction
    try:
        conn.execute("BEGIN")
        deleted = {}
        for t in tables:
            cur.execute(delete_sql[t], params)
            deleted[t] = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    deleted["total"] = sum(deleted.values())
    return deleted

def delete_efv_by_section_names(
    section_names: Optional[Iterable[str]],
    report_id: Optional[int] = None,
) -> Dict[str, int]:
    """
    Delete rows from {event, factor, variable} by case-insensitive section_name match.

    Behavior:
      - If `section_names` is None: wipe the three tables (optionally filtered by report_id).
      - Else: delete rows whose LOWER(section_name) is in the provided list (normalized to lowercase).

    Notes:
      - We normalize inputs using str.casefold() (more robust than lower()).
      - SQL uses LOWER(section_name) ... to match the normalized inputs.
    """
    conn = sqlite3.connect(settings.database_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    tables = ["event", "factor", "variable"]
    deleted = {t: 0 for t in tables}

    try:
        conn.execute("BEGIN")

        # Case A: wipe all (optionally by report_id)
        if section_names is None:
            where = ""
            params = {}
            if report_id is not None:
                where = " WHERE report_id = :report_id"
                params["report_id"] = report_id

            for t in tables:
                cur.execute(f"DELETE FROM {t}{where}", params)
                deleted[t] = cur.rowcount or 0

        # Case B: delete by list of section_names (case-insensitive)
        else:
            # Normalize: copy and casefold the provided names; drop blanks/dups
            names_norm = []
            seen = set()
            for s in section_names:
                if s is None:
                    continue
                norm = str(s).strip()
                if not norm:
                    continue
                norm_cf = norm.casefold()
                if norm_cf not in seen:
                    seen.add(norm_cf)
                    names_norm.append(norm_cf)

            if not names_norm:
                conn.commit()
                deleted["total"] = sum(deleted.values())
                return deleted

            # Build IN clause with named params :sn0, :sn1, ...
            placeholders = [f":sn{i}" for i in range(len(names_norm))]
            in_clause = ", ".join(placeholders)
            params = {f"sn{i}": v for i, v in enumerate(names_norm)}

            # WHERE LOWER(section_name) IN (...) [AND report_id = :report_id]
            where = f"LOWER(section_name) IN ({in_clause})"
            if report_id is not None:
                where += " AND report_id = :report_id"
                params["report_id"] = report_id

            for t in tables:
                cur.execute(f"DELETE FROM {t} WHERE {where}", params)
                deleted[t] = cur.rowcount or 0

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    deleted["total"] = sum(deleted.values())
    return deleted

def _normalize_sections(section_names: Optional[Union[str, Iterable[str]]]) -> List[str]:
    """
    Accept a single string or an iterable of strings.
    Returns a clean list of non-empty section names (original case).
    """
    if section_names is None:
        return []
    if isinstance(section_names, str):
        items = [section_names]
    else:
        items = list(section_names)
    # strip blanks & drop empties
    out = [s.strip() for s in items if s is not None and str(s).strip()]
    return out


# ---------- Step 1: resolve report_id set ----------
def get_report_ids_for_company(
    company_name: str,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    limit: Optional[int] = None,
    DB_PATH : str = settings.database_path
) -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
    ids = [r["id"] for r in cur.fetchall()]
    conn.close()
    return ids


# ---------- Step 2: query each EFV table once ----------
def fetch_efv_by_reports_and_sections(
    report_ids: Iterable[int],
    section_names: Optional[Union[str, Iterable[str]]],
    DB_PATH : str = settings.database_path
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch variables, factors, events for given report_ids and section_names.
    section_names can be a single string or an iterable of strings.
    :param DB_PATH:
    """
    report_ids = [int(x) for x in report_ids if x is not None]
    sects = _normalize_sections(section_names)

    if not report_ids or not sects:
        return {"variables": [], "factors": [], "events": []}

    # Build IN (...) placeholders
    rid_ph = ", ".join([f":rid{i}" for i in range(len(report_ids))])
    sn_ph  = ", ".join([f":sn{i}"  for i in range(len(sects))])
    rid_params = {f"rid{i}": v for i, v in enumerate(report_ids)}
    sn_params  = {f"sn{i}":  v for i, v in enumerate(sects)}
    params = {**rid_params, **sn_params}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    q_var = f"""
        SELECT id, report_id, section_name, name, value, unit, period, evidence
        FROM variable
        WHERE report_id IN ({rid_ph})
          AND LOWER(section_name) IN ({sn_ph})
        ORDER BY report_id DESC, id DESC
    """
    q_fac = f"""
        SELECT id, report_id, section_name, name, period, evidence
        FROM factor
        WHERE report_id IN ({rid_ph})
          AND LOWER(section_name) IN ({sn_ph})
        ORDER BY report_id DESC, id DESC
    """
    q_evt = f"""
        SELECT id, report_id, section_name, name, event_type, period, evidence
        FROM event
        WHERE report_id IN ({rid_ph})
          AND LOWER(section_name) IN ({sn_ph})
        ORDER BY report_id DESC, id DESC
    """

    cur.execute(q_var, params); variables = [dict(r) for r in cur.fetchall()]
    cur.execute(q_fac, params); factors   = [dict(r) for r in cur.fetchall()]
    cur.execute(q_evt, params); events    = [dict(r) for r in cur.fetchall()]
    conn.close()

    return {"variables": variables, "factors": factors, "events": events}


# ---------- One-call helper ----------
def fetch_efv_for_company_section(
    company_name: str,
    section_names: Optional[Union[str, Iterable[str]]],
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    report_limit: Optional[int] = None,
    DB_PATH : str = settings.database_path
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Resolve report_ids for the company, then query each EFV table once.
    section_names can be a single string or an iterable of strings.
    """
    rids = get_report_ids_for_company(company_name, year_min, year_max, report_limit, DB_PATH)
    return fetch_efv_by_reports_and_sections(rids, section_names, DB_PATH = DB_PATH)


if __name__ == '__main__':
    # initiate_database()
    print(fetch_efv_for_company_section("Amazon.com, Inc.", "liquidity and debt structure"))