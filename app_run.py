import os
import sys
import json
import sqlite3
from typing import Dict, List, Tuple, Optional
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import time

# Your project recommender entry (keep the original path/import)
from source.recommend_compass import recommend as rc_recommend
from source.settings import database_path
from source.settings import include_section
# ------------------------------------------------------------
# Paths & Config
# ------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = database_path
TEMP_DIR = os.path.join(PROJECT_ROOT, "temp")
CONFIG_PATH = os.path.join(TEMP_DIR, "ui_config.json")

DEFAULT_CONFIG = {
    "use_global": True,   # include global (cross-company) signals
    "k_var": 8,
    "k_factor": 6,
    "k_event": 6,
}

# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------

def ensure_dirs():
    os.makedirs(TEMP_DIR, exist_ok=True)

def load_config() -> Dict:
    ensure_dirs()
    if not os.path.exists(CONFIG_PATH):
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        merged = DEFAULT_CONFIG.copy()
        merged.update({k: v for k, v in data.items() if k in DEFAULT_CONFIG})
        return merged
    except Exception:
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

def save_config(cfg: Dict) -> None:
    ensure_dirs()
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def get_conn() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)

# ------------------------------------------------------------
# Data access helpers
# ------------------------------------------------------------

def fetch_sections(conn: sqlite3.Connection) -> List[str]:
    """Collect available section names."""
    sections: List[str] = []
    try:
        cur = conn.execute("""
            SELECT DISTINCT section_name
            FROM report_sections
            WHERE section_name IS NOT NULL AND TRIM(section_name) <> ''
            ORDER BY section_name
        """)
        sections = [r[0] for r in cur.fetchall()]
    except Exception:
        sections = []
    return sections

def shorten_company_name(name: str) -> str:
    if not name:
        return ""
    cut_tokens = [",", " Inc", " Ltd", " Limited", " PLC", " Corp", " Corporation", " Co."]
    short = name
    for tok in cut_tokens:
        if tok in short:
            short = short.split(tok)[0]
    short = short.strip()
    if len(short) > 28:
        short = short[:25].rstrip() + "…"
    return short

def fetch_companies(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    names: List[str] = []
    if table_exists(conn, "report"):
        try:
            cur = conn.execute("""
                SELECT DISTINCT company_name
                FROM report
                WHERE company_name IS NOT NULL AND TRIM(company_name) <> ''
            """)
            names = [r[0] for r in cur.fetchall()]
        except sqlite3.OperationalError:
            pass

    uniques = sorted(set(n for n in names if n))
    short_to_full: Dict[str, str] = {}
    pairs: List[Tuple[str, str]] = []
    for full in uniques:
        short = shorten_company_name(full)
        if short in short_to_full:
            suffix = (full[:12] + "…") if len(full) > 12 else full
            short = f"{short} ({suffix})"
        short_to_full[short] = full
        pairs.append((short, full))
    return pairs

# ------------------------------------------------------------
# Name lookup helper (prefer more complete rows)
# ------------------------------------------------------------

import sqlite3
from typing import Optional, Dict, Any

def find_examples_by_name(
    conn: sqlite3.Connection,
    node_type: str,          # "event" | "factor" | "variable"
    name_query: str,         # match against table.name
    *,
    limit: int = 1,
) -> Optional[Dict[str, Any]]:
    """
    Exact, case-insensitive match using parameterized SQL (no LIKE).
    Step 1: prefer a row with non-empty `period`;
    Step 2: fallback to any row;
    Returns the first row as a dict, or None.
    """
    node_type = node_type.lower()
    if node_type not in {"event", "factor", "variable"}:
        raise ValueError("node_type must be one of: 'event', 'factor', or 'variable'")

    cur = conn.cursor()

    # Step 1: exact match + require non-empty period
    sql_with_period = f"""
        SELECT *
        FROM {node_type}
        WHERE name = ? COLLATE NOCASE
          AND period IS NOT NULL AND TRIM(period) <> ''
        ORDER BY id DESC
        LIMIT ?
    """
    cur.execute(sql_with_period, (name_query, int(limit)))
    rows = cur.fetchall()

    if not rows:
        sql_no_period = f"""
            SELECT *
            FROM {node_type}
            WHERE name = ? COLLATE NOCASE
            ORDER BY id DESC
            LIMIT ?
        """
        cur.execute(sql_no_period, (name_query, int(limit)))
        rows = cur.fetchall()

    # Use column names from the last executed statement
    cols = [d[0] for d in cur.description] if rows else []
    cur.close()

    return dict(zip(cols, rows[0])) if rows else None

# ------------------------------------------------------------
# Recommendation plumbing
# ------------------------------------------------------------

def run_recommendation(company_name: str, section_name: str, cfg: Dict) -> Dict:
    if not section_name:
        return {"variables": [], "factors": [], "events": []}

    # ✅ no company => global
    use_global = bool(cfg.get("use_global", True))
    if not company_name:
        use_global = True

    return rc_recommend(
        company_name=company_name or "",         # 允许空
        section_names=[section_name],
        k_var=int(cfg.get("k_var", 8)),
        k_factor=int(cfg.get("k_factor", 8)),
        k_event=int(cfg.get("k_event", 15)),
        use_global=use_global,
    )

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------

class SettingsDialog(tk.Toplevel):
    def __init__(self, master, cfg: Dict):
        super().__init__(master)
        self.title("Settings")
        self.resizable(False, False)
        latest = load_config()
        self.cfg = {**cfg, **latest}
        self.result: Optional[Dict] = None

        pad = {"padx": 12, "pady": 8}

        self.var_global = tk.BooleanVar(value=self.cfg.get("use_global", True))
        ttk.Checkbutton(self, text="Include global (cross-company) context",
                        variable=self.var_global).grid(row=0, column=0, columnspan=2, sticky="w", **pad)

        ttk.Label(self, text="Variables (k)").grid(row=1, column=0, sticky="e", **pad)
        self.var_kv = tk.IntVar(value=int(self.cfg.get("k_var", 8)))
        ttk.Spinbox(self, from_=0, to=100, textvariable=self.var_kv, width=8).grid(row=1, column=1, sticky="w", **pad)

        ttk.Label(self, text="Factors (k)").grid(row=2, column=0, sticky="e", **pad)
        self.var_kf = tk.IntVar(value=int(self.cfg.get("k_factor", 6)))
        ttk.Spinbox(self, from_=0, to=100, textvariable=self.var_kf, width=8).grid(row=2, column=1, sticky="w", **pad)

        ttk.Label(self, text="Events (k)").grid(row=3, column=0, sticky="e", **pad)
        self.var_ke = tk.IntVar(value=int(self.cfg.get("k_event", 6)))
        ttk.Spinbox(self, from_=0, to=100, textvariable=self.var_ke, width=8).grid(row=3, column=1, sticky="w", **pad)

        ttk.Separator(self, orient="horizontal").grid(row=4, column=0, columnspan=2, sticky="ew", padx=12, pady=(4, 6))

        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=5, column=0, columnspan=2, sticky="ew")
        btn_frame.columnconfigure((0, 1), weight=1)

        ttk.Button(btn_frame, text="Cancel", command=self.destroy).grid(row=0, column=0, sticky="e", padx=8, pady=8)
        ttk.Button(btn_frame, text="Save", command=self._save).grid(row=0, column=1, sticky="w", padx=8, pady=8)

        self.grab_set()
        self.transient(master)
        self.wait_visibility()
        self.focus()

    def _save(self):
        self.result = {
            "use_global": bool(self.var_global.get()),
            "k_var": int(self.var_kv.get()),
            "k_factor": int(self.var_kf.get()),
            "k_event": int(self.var_ke.get()),
        }
        save_config(self.result)
        self.destroy()

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Recommendation Demo")
        self.geometry("1200x600")
        self.minsize(1100, 560)

        # ttk theme
        self.style = ttk.Style(self)
        if "vista" in self.style.theme_names():
            self.style.theme_use("vista")

        self.cfg = load_config()

        # Top controls
        top = ttk.Frame(self); top.pack(fill="x", padx=16, pady=12)
        ttk.Label(top, text="Section").grid(row=0, column=0, sticky="w")
        self.cmb_section = ttk.Combobox(top, state="readonly", width=36)
        self.cmb_section.grid(row=1, column=0, sticky="w", padx=(0, 24))

        ttk.Label(top, text="Company").grid(row=0, column=1, sticky="w")
        self.cmb_company = ttk.Combobox(top, state="readonly", width=36)
        self.cmb_company.grid(row=1, column=1, sticky="w", padx=(0, 24))

        self.btn_go = ttk.Button(top, text="Recommend", command=self.on_recommend)
        self.btn_go.grid(row=1, column=2, sticky="w")

        self.btn_settings = ttk.Button(top, text="⚙", width=3, command=self.on_settings)
        self.btn_settings.grid(row=1, column=3, sticky="w", padx=(12, 0))

        # Status bar
        self.status = tk.StringVar(value="Ready")
        ttk.Label(self, textvariable=self.status, anchor="w").pack(fill="x", side="bottom", padx=8, pady=4)

        # Notebook tabs
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=16, pady=12)

        # columns set
        self.events_columns = ("name", "score", "event_type", "contents", "period", "evidence")
        self.events_widths = [180, 70, 120, 220, 100, 300]

        self.factors_columns = ("name", "score", "contents", "period", "evidence")
        self.factors_widths = [180, 70, 260, 100, 300]

        self.vars_columns = ("name", "score", "contents", "period", "evidence", "value", "unit")
        self.vars_widths = [180, 70, 220, 100, 300, 90, 70]

        # 创建 TreeView
        self.tree_events = self._make_tree(self.nb, "Events", self.events_columns, self.events_widths)
        self.tree_factors = self._make_tree(self.nb, "Factors", self.factors_columns, self.factors_widths)
        self.tree_vars = self._make_tree(self.nb, "Variables", self.vars_columns, self.vars_widths)

        self._populate_dropdowns()

        # Bottom buttons
        btns = ttk.Frame(self); btns.pack(fill="x", padx=16, pady=(0, 12))
        ttk.Button(btns, text="Copy JSON", command=self.copy_json).pack(side="left")
        ttk.Button(btns, text="Save JSON…", command=self.save_json).pack(side="left", padx=(8, 0))

        self.current_payload: Dict = {"variables": [], "factors": [], "events": []}

    def _make_tree(self, parent, label, columns, widths):
        """Create TreeView with fixed column widths and centered headers."""
        frame = ttk.Frame(parent)
        parent.add(frame, text=label)

        tree = ttk.Treeview(frame, columns=columns, show="headings")

        # 设置列标题和固定列宽
        for col, width in zip(columns, widths):
            tree.heading(col, text=col.replace("_", " ").title(), anchor="center")  # 标题居中
            tree.column(col, width=width, anchor="w", stretch=False)  # 内容靠左，宽度固定

        # 滚动条
        yscroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=yscroll.set)
        yscroll.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)

        # 双击 evidence 显示详情
        tree.bind("<Double-1>", lambda e, t=tree: self._on_double_click(e, t))
        return tree

    def _on_double_click(self, event, tree: ttk.Treeview):
        """Double-click any cell to show its full content in a popup."""
        region = tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        col_id = tree.identify_column(event.x)  # like "#3"
        row_id = tree.identify_row(event.y)
        if not row_id or not col_id:
            return

        col_index = int(col_id[1:]) - 1
        columns = tree["columns"]
        if col_index < 0 or col_index >= len(columns):
            return

        col_name = columns[col_index]
        row_vals = tree.item(row_id, "values") or ()
        value = row_vals[col_index] if col_index < len(row_vals) else ""

        # 弹窗显示完整内容（所有列通用）
        popup = tk.Toplevel(self)
        popup.title(f"{col_name} • Detail")
        popup.geometry("720x420")
        popup.minsize(520, 300)

        # 顶部信息栏（列名 & 复制按钮）
        topbar = ttk.Frame(popup)
        topbar.pack(fill="x", padx=10, pady=(10, 0))

        ttk.Label(topbar, text=col_name.replace("_", " ").title()).pack(side="left")

        def _copy_text():
            self.clipboard_clear()
            self.clipboard_append(str(value))
            # 轻量提示
            try:
                popup.title(f"{col_name} • Copied")
                popup.after(800, lambda: popup.title(f"{col_name} • Detail"))
            except Exception:
                pass

        ttk.Button(topbar, text="Copy", command=_copy_text).pack(side="right")

        # 文本主体（可滚动、自动换行）
        body = ttk.Frame(popup)
        body.pack(fill="both", expand=True, padx=10, pady=10)

        txt = tk.Text(body, wrap="word", font=("Segoe UI", 10))
        txt.insert("1.0", str(value))
        txt.configure(state="disabled")

        yscroll = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=yscroll.set)

        txt.pack(side="left", fill="both", expand=True)
        yscroll.pack(side="right", fill="y")

        popup.transient(self)
        popup.grab_set()
        popup.focus_set()

    def _clear_results(self):
        for tree in (self.tree_events, self.tree_factors, self.tree_vars):
            for i in tree.get_children():
                tree.delete(i)

    def _populate_dropdowns(self):
        try:
            with get_conn() as conn:
                sections = include_section
                companies = fetch_companies(conn)
        except Exception as e:
            messagebox.showerror("Database Error", str(e))
            sections, companies = [], []

        self.cmb_section["values"] = sections
        self.cmb_company_pairs = companies  # [(short, full)]
        company_display_values = [""]
        company_display_values.extend([p[0] for p in companies])
        self.cmb_company["values"] = company_display_values

        if sections:
            self.cmb_section.current(0)

    def on_settings(self):
        dlg = SettingsDialog(self, self.cfg)
        self.wait_window(dlg)
        if getattr(dlg, "result", None):
            self.cfg = load_config()
            self.status.set(
                f"Settings saved • use_global={self.cfg.get('use_global')} • "
                f"k_var={self.cfg.get('k_var')} • k_factor={self.cfg.get('k_factor')} • k_event={self.cfg.get('k_event')}"
            )

    def on_recommend(self):
        section = self.cmb_section.get()
        comp_short = self.cmb_company.get()
        full = next((f for (s, f) in self.cmb_company_pairs if s == comp_short), "")

        if not section:
            messagebox.showinfo("Select Inputs", "Please choose a Section.")
            return

        self._clear_results()

        if full:
            self.status.set(f"Running… section='{section}', company='{comp_short}'")
        else:
            self.status.set(f"Running… section='{section}', GLOBAL view (no company selected)")

        self.btn_go.config(state="disabled")
        self.update_idletasks()
        time.sleep(0.2)

        try:
            payload = run_recommendation(full, section, self.cfg)
            self._render_payload(payload)
            self.current_payload = payload
            self.status.set("Done")
        except Exception as e:
            import traceback
            messagebox.showerror("Error", traceback.format_exc())
            self.status.set("Error")
        finally:
            self.btn_go.config(state="normal")
            self.nb.update_idletasks()

    def _render_payload(self, payload: Dict):
        """
        For each recommendation item:
          - treat 'evidence' as the lookup name
          - fetch the first best-matching row from the corresponding table
          - fill fields in this order:
              Events:    name | score | event_type | contents | period | evidence
              Factors:   name | score | contents   | period   | evidence
              Variables: name | score | contents   | period   | evidence | value | unit
        """

        def first_best(conn, node_type: str, name_to_find: str) -> Optional[Dict]:
            # 先精确，再 LIKE；find_examples_by_name 现在返回 dict 或 None
            row = find_examples_by_name(conn, node_type, name_to_find, limit=1)
            return row

        def enrich(node_type: str, items: List[Dict]) -> List[Tuple]:
            rows_out: List[Tuple] = []
            if not items:
                return rows_out
            with get_conn() as conn:
                for r in items:
                    name_to_find = (r.get("evidence") or "").strip()
                    if not name_to_find:
                        continue
                    best = first_best(conn, node_type, name_to_find)
                    score_val = r.get("score", "")

                    if node_type == "event":
                        if best:
                            rows_out.append((
                                best.get("name", name_to_find),
                                score_val,
                                best.get("event_type", ""),
                                best.get("contents", ""),
                                best.get("period", ""),
                                best.get("evidence", ""),
                            ))
                        else:
                            rows_out.append((name_to_find, score_val, "", "", "", ""))
                    elif node_type == "factor":
                        if best:
                            rows_out.append((
                                best.get("name", name_to_find),
                                score_val,
                                best.get("contents", ""),
                                best.get("period", ""),
                                best.get("evidence", ""),
                            ))
                        else:
                            rows_out.append((name_to_find, score_val, "", "", ""))
                    else:  # variable
                        if best:
                            rows_out.append((
                                best.get("name", name_to_find),
                                score_val,
                                best.get("contents", ""),
                                best.get("period", ""),
                                best.get("evidence", ""),
                                best.get("value", ""),
                                best.get("unit", ""),
                            ))
                        else:
                            rows_out.append((name_to_find, score_val, "", "", "", "", ""))
            return rows_out

        # clear existing rows
        for tree in (self.tree_events, self.tree_factors, self.tree_vars):
            for i in tree.get_children():
                tree.delete(i)

        # enrich & render
        ev_rows = enrich("event",   payload.get("events", []))
        fa_rows = enrich("factor",  payload.get("factors", []))
        va_rows = enrich("variable",payload.get("variables", []))

        for vals in ev_rows:
            self.tree_events.insert("", "end", values=vals)
        for vals in fa_rows:
            self.tree_factors.insert("", "end", values=vals)
        for vals in va_rows:
            self.tree_vars.insert("", "end", values=vals)

        self.tree_events.update_idletasks()
        self.tree_factors.update_idletasks()
        self.tree_vars.update_idletasks()

    def copy_json(self):
        try:
            s = json.dumps(self.current_payload, ensure_ascii=False, indent=2)
            self.clipboard_clear()
            self.clipboard_append(s)
            self.status.set("JSON copied to clipboard")
        except Exception as e:
            messagebox.showerror("Copy Error", str(e))

    def save_json(self):
        try:
            default = os.path.join(TEMP_DIR, "recs.json")
            os.makedirs(TEMP_DIR, exist_ok=True)
            path = filedialog.asksaveasfilename(
                initialfile=os.path.basename(default), initialdir=TEMP_DIR,
                defaultextension=".json", filetypes=[("JSON", "*.json")]
            )
            if not path:
                return
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.current_payload, f, ensure_ascii=False, indent=2)
            self.status.set(f"Saved: {path}")
        except Exception as e:
            messagebox.showerror("Save Error", str(e))

if __name__ == "__main__":
    try:
        app = App()
        app.mainloop()
    except Exception as e:
        print(f"Fatal error: {e}")
    # with sqlite3.connect(DB_PATH) as conn:
    #     # 查询 event 表，按 name 精确匹配
    #     name_to_search = "Cash position maintained"
    #
    #     print(f"Searching for record with name='{name_to_search}'...\n")
    #
    #     record = find_examples_by_name(
    #         conn=conn,
    #         node_type="event",  # 也可以是 "factor" 或 "variable"
    #         name_query=name_to_search,
    #         limit=1  # 只要第一条记录
    #     )
    #
    #     if record:
    #         print("Found record (prefer with period):")
    #         for key, value in record.items():
    #             print(f"{key}: {value}")
    #     else:
    #         print("No matching record found.")
