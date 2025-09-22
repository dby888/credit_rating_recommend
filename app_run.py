import os
import sys
import json
import sqlite3
from typing import Dict, List, Tuple, Optional

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import time

# ------------------------------------------------------------
# Paths & Config
# ------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_ROOT, "result.db")
TEMP_DIR = os.path.join(PROJECT_ROOT, "temp")
CONFIG_PATH = os.path.join(TEMP_DIR, "ui_config.json")

DEFAULT_CONFIG = {
    "use_global": True,              # whether to include global (cross-company) signals
    "k_var": 8,                      # number of variable recommendations
    "k_factor": 6,                   # number of factor recommendations
    "k_event": 6                     # number of event recommendations
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
        # fill any missing keys with defaults (future-compatible)
        merged = DEFAULT_CONFIG.copy()
        merged.update({k: v for k, v in data.items() if k in DEFAULT_CONFIG})
        return merged
    except Exception:
        # fallback to default if file is broken
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
    conn = sqlite3.connect(DB_PATH)
    return conn


# ------------------------------------------------------------
# Data access helpers (defensive against schema variations)
# ------------------------------------------------------------

def fetch_sections(conn: sqlite3.Connection) -> List[str]:
    """Collect available section names.
    Prefer the canonical `section` table; otherwise, gather distinct section_name from efv tables.
    """
    sections: List[str] = []
    try:
        if table_exists(conn, "section"):
            cur = conn.execute("SELECT DISTINCT section_name FROM section WHERE section_name IS NOT NULL AND TRIM(section_name) <> '' ORDER BY section_name")
            sections = [r[0] for r in cur.fetchall()]
        else:
            candidates = []
            for t in ("event", "factor", "variable"):
                if table_exists(conn, t):
                    try:
                        cur = conn.execute(f"SELECT DISTINCT section_name FROM {t} WHERE section_name IS NOT NULL AND TRIM(section_name) <> ''")
                        candidates.extend(r[0] for r in cur.fetchall())
                    except sqlite3.OperationalError:
                        pass
            sections = sorted({s for s in candidates if s})
    except Exception:
        sections = []
    return sections


def shorten_company_name(name: str) -> str:
    """Make a short display name (safe & readable)."""
    if not name:
        return ""
    # Heuristics: keep up to first comma/Inc./Ltd. etc.
    cut_tokens = [",", " Inc", " Ltd", " Limited", " PLC", " Corp", " Corporation", " Co."]
    short = name
    for tok in cut_tokens:
        if tok in short:
            short = short.split(tok)[0]
    short = short.strip()
    # Keep a reasonable length
    if len(short) > 28:
        short = short[:25].rstrip() + "…"
    return short


def fetch_companies(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    """Return list of (short_display, full_company_name), with disambiguation to avoid short-name collisions."""
    names: List[str] = []
    if table_exists(conn, "company"):
        try:
            cur = conn.execute("SELECT DISTINCT company_name FROM company WHERE company_name IS NOT NULL AND TRIM(company_name) <> ''")
            names = [r[0] for r in cur.fetchall()]
        except sqlite3.OperationalError:
            pass
    if not names and table_exists(conn, "report"):
        try:
            cur = conn.execute("SELECT DISTINCT company_name FROM report WHERE company_name IS NOT NULL AND TRIM(company_name) <> ''")
            names = [r[0] for r in cur.fetchall()]
        except sqlite3.OperationalError:
            pass

    uniques = sorted(set(n for n in names if n))
    short_to_full: Dict[str, str] = {}
    pairs: List[Tuple[str, str]] = []
    for full in uniques:
        short = shorten_company_name(full)
        if short in short_to_full:
            # Disambiguate: append prefix of full name
            suffix = (full[:12] + "…") if len(full) > 12 else full
            short = f"{short} ({suffix})"
        short_to_full[short] = full
        pairs.append((short, full))
    return pairs


# ------------------------------------------------------------
# Recommendation plumbing (pluggable)
# ------------------------------------------------------------

# Optional: Try to import a project-specific recommender if present
_RECOMMENDER = None
try:
    # Expecting: source/recommend_compass.py with a function `recommend(company_name: str, section_names: List[str], k_var: int, k_factor: int, k_event: int, use_global: bool) -> Dict`
    sys.path.append(os.path.join(PROJECT_ROOT))
    from source.recommend_compass import recommend as _RECOMMENDER  # type: ignore
except Exception:
    _RECOMMENDER = None


def fallback_recommend(conn: sqlite3.Connection, company_name: str, section_name: str, k_var: int, k_factor: int, k_event: int, use_global: bool) -> Dict:
    """A very simple SQL-based fallback recommender, used when the project-specific
    recommender is not importable. It ranks by frequency (and recentness if columns exist)."""
    out = {"variables": [], "factors": [], "events": []}

    def _grab(table: str, k: int) -> List[Dict]:
        if not table_exists(conn, table):
            return []
        where = ["section_name = ?"]
        params: List = [section_name]
        if not use_global:
            where.append("report_id IN (SELECT id FROM report WHERE company_name = ?)")
            params.append(company_name)
        where_clause = " AND ".join(where)

        # Try to use recency if a timestamp exists
        order = "ORDER BY created_at DESC" if any(col in _columns(conn, table) for col in ("created_at", "ts", "timestamp")) else "ORDER BY id DESC"
        sql = f"""
            SELECT id, section_name, COALESCE(evidence, '') AS evidence
            FROM {table}
            WHERE {where_clause}
            {order}
            LIMIT ?
        """
        cur = conn.execute(sql, (*params, k))
        rows = cur.fetchall()
        results = [{"id": r[0], "section": r[1], "evidence": r[2]} for r in rows]
        return results

    out["variables"] = _grab("variable", k_var)
    out["factors"] = _grab("factor", k_factor)
    out["events"] = _grab("event", k_event)
    return out


def _columns(conn: sqlite3.Connection, table: str) -> List[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return [r[1] for r in cur.fetchall()]
    except Exception:
        return []


def run_recommendation(company_name: str, section_name: str, cfg: Dict) -> Dict:
    """Glue function that either calls the project recommender (if present) or uses a SQL fallback."""
    if not company_name or not section_name:
        return {"variables": [], "factors": [], "events": []}

    # Debug print so you can see effective settings in console
    print("[UI] run_recommendation with:", cfg)

    if _RECOMMENDER is not None:
        try:
            return _RECOMMENDER(
                company_name=company_name,
                section_names=[section_name],
                k_var=int(cfg.get("k_var", 8)),
                k_factor=int(cfg.get("k_factor", 6)),
                k_event=int(cfg.get("k_event", 6)),
                use_global=bool(cfg.get("use_global", True)),
            )
        except Exception as e:
            messagebox.showwarning("Recommender Error", f"Falling back to SQL recommender due to error:{e}")

    with get_conn() as conn:
        return fallback_recommend(
            conn,
            company_name=company_name,
            section_name=section_name,
            k_var=int(cfg.get("k_var", 8)),
            k_factor=int(cfg.get("k_factor", 6)),
            k_event=int(cfg.get("k_event", 6)),
            use_global=bool(cfg.get("use_global", True)),
        )


# ------------------------------------------------------------
# UI
# ------------------------------------------------------------

class SettingsDialog(tk.Toplevel):
    def __init__(self, master, cfg: Dict):
        super().__init__(master)
        self.title("Settings")
        self.resizable(False, False)
        # Always load latest from disk so dialog reflects what will be used
        latest = load_config()
        self.cfg = {**cfg, **latest}
        self.result: Optional[Dict] = None
        self.cfg = cfg.copy()
        self.result: Optional[Dict] = None

        pad = {"padx": 12, "pady": 8}

        # Use global
        self.var_global = tk.BooleanVar(value=self.cfg.get("use_global", True))
        chk = ttk.Checkbutton(self, text="Include global (cross-company) context", variable=self.var_global)
        chk.grid(row=0, column=0, columnspan=2, sticky="w", **pad)

        # k-values
        ttk.Label(self, text="Variables (k)").grid(row=1, column=0, sticky="e", **pad)
        self.var_kv = tk.IntVar(value=int(self.cfg.get("k_var", 8)))
        ttk.Spinbox(self, from_=0, to=100, textvariable=self.var_kv, width=8).grid(row=1, column=1, sticky="w", **pad)

        ttk.Label(self, text="Factors (k)").grid(row=2, column=0, sticky="e", **pad)
        self.var_kf = tk.IntVar(value=int(self.cfg.get("k_factor", 6)))
        ttk.Spinbox(self, from_=0, to=100, textvariable=self.var_kf, width=8).grid(row=2, column=1, sticky="w", **pad)

        ttk.Label(self, text="Events (k)").grid(row=3, column=0, sticky="e", **pad)
        self.var_ke = tk.IntVar(value=int(self.cfg.get("k_event", 6)))
        ttk.Spinbox(self, from_=0, to=100, textvariable=self.var_ke, width=8).grid(row=3, column=1, sticky="w", **pad)

        sep = ttk.Separator(self, orient="horizontal")
        sep.grid(row=4, column=0, columnspan=2, sticky="ew", padx=12, pady=(4, 6))

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
        self.geometry("900x560")
        self.minsize(820, 520)

        # Use ttk themed widgets for a cleaner look
        self.style = ttk.Style(self)
        if "vista" in self.style.theme_names():
            self.style.theme_use("vista")

        self.cfg = load_config()

        # Top controls
        top = ttk.Frame(self)
        top.pack(fill="x", padx=16, pady=12)

        # Section dropdown
        ttk.Label(top, text="Section").grid(row=0, column=0, sticky="w")
        self.cmb_section = ttk.Combobox(top, state="readonly", width=36)
        self.cmb_section.grid(row=1, column=0, sticky="w", padx=(0, 24))

        # Company dropdown (short display)
        ttk.Label(top, text="Company").grid(row=0, column=1, sticky="w")
        self.cmb_company = ttk.Combobox(top, state="readonly", width=36)
        self.cmb_company.grid(row=1, column=1, sticky="w", padx=(0, 24))

        # Recommend button
        self.btn_go = ttk.Button(top, text="Recommend", command=self.on_recommend)
        self.btn_go.grid(row=1, column=2, sticky="w")

        # Gear (settings)
        self.btn_settings = ttk.Button(top, text="⚙", width=3, command=self.on_settings)
        self.btn_settings.grid(row=1, column=3, sticky="w", padx=(12, 0))

        # Status bar
        self.status = tk.StringVar(value="Ready")
        bar = ttk.Label(self, textvariable=self.status, anchor="w")
        bar.pack(fill="x", side="bottom", padx=8, pady=4)

        # Notebook for results
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=16, pady=12)

        self.tree_vars = self._make_tree(self.nb, "Variables")
        self.tree_factors = self._make_tree(self.nb, "Factors")
        self.tree_events = self._make_tree(self.nb, "Events")

        self._populate_dropdowns()

        # Contextual buttons
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=16, pady=(0, 12))
        ttk.Button(btns, text="Copy JSON", command=self.copy_json).pack(side="left")
        ttk.Button(btns, text="Save JSON…", command=self.save_json).pack(side="left", padx=(8, 0))

        self.current_payload: Dict = {"variables": [], "factors": [], "events": []}

    def _make_tree(self, parent, label) -> ttk.Treeview:
        frame = ttk.Frame(parent)
        parent.add(frame, text=label)
        tree = ttk.Treeview(frame, columns=("id", "section", "evidence"), show="headings")
        tree.heading("id", text="ID")
        tree.heading("section", text="Section")
        tree.heading("evidence", text="Evidence")
        tree.column("id", width=80, anchor="center")
        tree.column("section", width=180)
        tree.column("evidence", width=520)
        tree.pack(fill="both", expand=True)
        return tree

    # Clear all result trees so the UI visibly updates before a new run
    def _clear_results(self):
        for tree in (self.tree_vars, self.tree_factors, self.tree_events):
            for i in tree.get_children():
                tree.delete(i)

    def _populate_dropdowns(self):
        try:
            with get_conn() as conn:
                sections = fetch_sections(conn)
                companies = fetch_companies(conn)
        except Exception as e:
            messagebox.showerror("Database Error", str(e))
            sections, companies = [], []

        self.cmb_section["values"] = sections
        self.cmb_company_pairs = companies  # [(short, full)]
        self.cmb_company["values"] = [p[0] for p in companies]

        if sections:
            self.cmb_section.current(0)
        if companies:
            self.cmb_company.current(0)

    def on_settings(self):
        dlg = SettingsDialog(self, self.cfg)
        # Block here until the dialog is closed so we can read dlg.result
        self.wait_window(dlg)
        if getattr(dlg, "result", None):
            # Re-read from disk to ensure persistence is reflected in-memory, too
            self.cfg = load_config()
            # Also reflect to status for quick verification
            self.status.set(
                f"Settings saved • use_global={self.cfg.get('use_global')} • k_var={self.cfg.get('k_var')} • k_factor={self.cfg.get('k_factor')} • k_event={self.cfg.get('k_event')}"
            )

    def on_recommend(self):
        section = self.cmb_section.get()
        comp_short = self.cmb_company.get()
        full = next((f for (s, f) in self.cmb_company_pairs if s == comp_short), "")

        if not section or not full:
            messagebox.showinfo("Select Inputs", "Please choose both Section and Company.")
            return

        # Clear old results to ensure visible change, and show current selection
        self._clear_results()
        self.status.set(f"Running… section='{section}', company='{comp_short}'")
        self.btn_go.config(state="disabled")
        self.update_idletasks()
        time.sleep(0.2)  # Add slight delay to visibly refresh UI

        try:
            payload = run_recommendation(full, section, self.cfg)
            self._render_payload(payload)
            self.current_payload = payload
            self.status.set("Done")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.status.set("Error")
        finally:
            self.btn_go.config(state="normal")
            self.nb.update_idletasks()

    def _render_payload(self, payload: Dict):
        def fill(tree: ttk.Treeview, rows: List[Dict]):
            for i in tree.get_children():
                tree.delete(i)
            for r in rows:
                tree.insert("", "end", values=(r.get("id", ""), r.get("section", ""), r.get("evidence", "")))
            tree.update_idletasks()

        fill(self.tree_vars, payload.get("variables", []))
        fill(self.tree_factors, payload.get("factors", []))
        fill(self.tree_events, payload.get("events", []))
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
            path = filedialog.asksaveasfilename(initialfile=os.path.basename(default), initialdir=TEMP_DIR, defaultextension=".json", filetypes=[("JSON", "*.json")])
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
