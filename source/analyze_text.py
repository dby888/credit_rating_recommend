# -*- coding: utf-8 -*-
"""
Rule-only variable extractor (multi-sentence).

Signature (fixed order):
    extract_variables(section_id, report_id, company_name, section_name, contents)

- `contents` is a LIST OF SENTENCES (strings). We will scan all sentences.
- No reliance on section_name normalization; rules are content-driven.
- Returns rows ready to INSERT into `variable_observations` (without `id`):
    {report_id, section_id, company_name, section_name, name, unit, value, evidence}

Covered variables (non-exhaustive but practical):
  Liquidity/Capital:
    cash_balance, cp_program_limit, undrawn_rcf, rcf_maturity,
    revolver_limit, revolver_maturity, maturity_due_YYYY_MM (bond/notes amount+due)
    buyback_program_start_year, buyback_status_<year>, buyback_status_<year>_<Q>
  Rating:
    rating_action (affirm/upgrade/downgrade), rating_outlook (Stable/Positive/Negative)
  Regulatory/Legal:
    fine_amount (if amount present), fine_event (mention), legal_event (lawsuit/settlement),
    license_status (approved/revoked/suspended)
  Guidance/Results:
    guidance_action (raised/cut/reiterated), ebitda_value, revenue_direction (grow/decline/beat/miss)
  M&A / Investment:
    mna_event (acquisition/divestiture), mna_consideration (EV/consideration)
  Operations/Facility:
    operations_event (commission/expand/shutdown/restart/launch/recall/withdraw),
    capacity_change (value with unit)

You can extend rule banks by adding more patterns and emit() calls.
"""

import re
from typing import List, Dict, Any, Optional, Tuple

# ======================
# Common regex & helpers
# ======================

MONEY = re.compile(r'(?i)(?:usd|\$|eur|sgd|hkd|cny|rmb)\s*([\d.,]+)\s*(bn|billion|m|million)?')
DUE   = re.compile(r'(?i)\bdue\b\s+(?:in\s+)?([A-Za-z]{3,9}\.?\s*\d{4}|\b[A-Za-z]{3}\b\s*\d{4}|\d{4}-\d{2}|\b[A-Za-z]{3,9}\s*\d{1,2},\s*\d{4})')

MONTHS = {
    'jan':'01','january':'01','feb':'02','february':'02','mar':'03','march':'03',
    'apr':'04','april':'04','may':'05','jun':'06','june':'06','jul':'07','july':'07',
    'aug':'08','august':'08','sep':'09','sept':'09','september':'09',
    'oct':'10','october':'10','nov':'11','november':'11','dec':'12','december':'12'
}

def _trim_float(x: float) -> str:
    s = f"{x:.3f}"
    return s.rstrip('0').rstrip('.')

def _norm_money(amount_str: str, scale: Optional[str]) -> Tuple[str, str]:
    """
    Normalize to consistent currency-unit:
      - 'm'/'million' -> USD_mn
      - else (None/'bn'/'billion') -> USD_bn
    Returns (value_str, unit_str).
    """
    val = float(amount_str.replace(',', ''))
    if scale and scale.lower() in ('m', 'million'):
        return (_trim_float(val), 'USD_mn')
    return (_trim_float(val), 'USD_bn')

def _norm_month_year(raw: str) -> str:
    """
    Normalize Month/Year variants to 'YYYY-MM'; if only Year present, fallback to 'YYYY-01'.
    """
    t = raw.strip().replace('.', '')
    if re.fullmatch(r'\d{4}-\d{2}', t):
        return t
    m = re.search(r'([A-Za-z]{3,9})\s*(?:\d{1,2},\s*)?(\d{4})', t)
    if m:
        mon, year = m.group(1).lower(), m.group(2)
        mon2 = MONTHS.get(mon[:3], None) or MONTHS.get(mon, None)
        if mon2:
            return f"{year}-{mon2}"
    y = re.search(r'\b(20\d{2})\b', t)
    if y:
        return f"{y.group(1)}-01"
    return t

def _emit(rows: List[Dict[str, Any]],
          report_id: int, section_id: int, company_name: str, section_name: str,
          name: str, unit: Optional[str], value: str, evidence: str) -> None:
    """
    Append one variable row. Fields match `variable_observations` except `id` (Snowflake) and timestamps.
    """
    rows.append({
        "report_id": report_id,
        "section_id": section_id,
        "company_name": company_name,
        "section_name": section_name,
        "name": name,
        "unit": unit,
        "value": value,
        "evidence": evidence
    })

# ======================
# Pattern banks by topic
# ======================

# Liquidity / Capital structure
CASH_BAL      = re.compile(r'(?i)\bcash (?:and )?cash equivalents\b')
CP_HINT       = re.compile(r'(?i)\b(cp program|commercial paper)\b')
RCF_HINT      = re.compile(r'(?i)\b(rcf|revolving credit facility)\b')
REV_HINT      = re.compile(r'(?i)\brevolver\b')
MATURITY_PAIR = re.compile(
    r'(?i)(?:notes?|bonds?|debt|amount)\D{0,20}(?:\$|usd)\s*([\d.,]+)\s*(bn|billion|m|million)?\D{0,50}'
    r'\bdue\b\s+(?:in\s+)?([A-Za-z]{3,9}\.?\s*\d{4}|\b[A-Za-z]{3}\b\s*\d{4}|\b[A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{4}-\d{2})'
)

# Rating action
RATING_ACTION = re.compile(r'(?i)\b(affirmed?|upgraded?|downgraded?)\b')
OUTLOOK       = re.compile(r'(?i)\boutlook\b.*?\b(stable|positive|negative)\b')

# Regulatory/Legal
FINE_HINT     = re.compile(r'(?i)\b(fine|penalt(y|ies))\b')
LAWSUIT_HINT  = re.compile(r'(?i)\b(lawsuit|litigation|settlement|injunction)\b')
LICENSE_HINT  = re.compile(r'(?i)\b(license|licence)\b.*?\b(approved|revoked|suspended)\b')

# Guidance/Results
GUIDANCE_ACT  = re.compile(r'(?i)\bguidance\b.*?\b(raised|cut|reiterated)\b')
EBITDA_HINT   = re.compile(r'(?i)\bebitda\b')
REVENUE_DIR   = re.compile(r'(?i)\brevenue\b.*?\b(rise|grow|increase|decline|drop|fall|beat|miss)\b')

# M&A / Investment
ACQ_HINT      = re.compile(r'(?i)\b(acquire|acquisition|takeover|buyout)\b')
DIV_HINT      = re.compile(r'(?i)\b(divest|disposal|asset sale|spin[- ]?off|carve[- ]?out)\b')
CONSID_VAL    = re.compile(r'(?i)(?:enterprise value|consideration)\D{0,10}(?:\$|usd)\s*([\d.,]+)\s*(bn|billion|m|million)?')

# Operations / Product / Facility
FACILITY_HINT = re.compile(r'(?i)\b(plant|facility|datacenter|mine|refinery|line)\b')
OPS_VERB      = re.compile(r'(?i)\b(commission|expand|shutdown|restart|launch|recall|withdraw)\b')
CAPACITY_VAL  = re.compile(r'(?i)\b(capacity)\b.*?([\d.,]+\s*(mw|gw|kt|mt|units?))')

# Buyback signals
BUYBACK_START = re.compile(r'(?i)\bbegan (?:a )?share repurchase program in (\d{4})\b')
BUYBACK_NONE  = re.compile(
    r'(?i)\b(?:did not repurchase|no repurchases)\b(?:.*?\b(1Q|2Q|3Q|4Q)\s*(20\d{2})\b|\b.*?\b(20\d{2})\b)'
)

# ================
# Main entry point
# ================
def extract_all(section_id: int,
                      report_id: int,
                      company_name: str,
                      section_name: str,
                      contents: List[str]) -> List[Dict[str, Any]]:
    """
    Rule-only extractor over a LIST of sentences.
    We iterate all sentences, fire pattern banks, and emit variables with evidence.
    """
    rows: List[Dict[str, Any]] = []
    if not contents:
        return rows

    for sent in contents:
        if not sent or not sent.strip():
            continue
        s = sent.strip()

        # ---- Liquidity / Capital structure ----
        if CASH_BAL.search(s):
            m = MONEY.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "cash_balance", unit, val, evidence=m.group(0))

        if CP_HINT.search(s):
            m = MONEY.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "cp_program_limit", unit, val, evidence=m.group(0))

        if RCF_HINT.search(s):
            m = MONEY.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "undrawn_rcf", unit, val, evidence=m.group(0))
            d = DUE.search(s)
            if d:
                due = _norm_month_year(d.group(1))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "rcf_maturity", "date", due, evidence=d.group(0))

        if REV_HINT.search(s):
            m = MONEY.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "revolver_limit", unit, val, evidence=m.group(0))
            d = DUE.search(s)
            if d:
                due = _norm_month_year(d.group(1))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "revolver_maturity", "date", due, evidence=d.group(0))

        for mm in MATURITY_PAIR.finditer(s):
            amt_str, scale, due_raw = mm.groups()
            val, unit = _norm_money(amt_str, scale)
            due = _norm_month_year(due_raw)
            key = f"maturity_due_{due.replace('-', '_')}"
            _emit(rows, report_id, section_id, company_name, section_name,
                  key, unit, val, evidence=mm.group(0))

        # ---- Buybacks ----
        bs = BUYBACK_START.search(s)
        if bs:
            _emit(rows, report_id, section_id, company_name, section_name,
                  "buyback_program_start_year", "year", bs.group(1), evidence=bs.group(0))

        bn = BUYBACK_NONE.search(s)
        if bn:
            q, y_q, y_only = bn.groups()
            if y_only:
                _emit(rows, report_id, section_id, company_name, section_name,
                      f"buyback_status_{y_only}", "enum", "none", evidence=bn.group(0))
            elif q and y_q:
                _emit(rows, report_id, section_id, company_name, section_name,
                      f"buyback_status_{y_q}_{q.upper()}", "enum", "none", evidence=bn.group(0))

        # ---- Rating ----
        ra = RATING_ACTION.search(s)
        if ra:
            _emit(rows, report_id, section_id, company_name, section_name,
                  "rating_action", "enum", ra.group(1).lower(), evidence=ra.group(0))
        ro = OUTLOOK.search(s)
        if ro:
            _emit(rows, report_id, section_id, company_name, section_name,
                  "rating_outlook", "enum", ro.group(1).capitalize(), evidence=ro.group(0))

        # ---- Regulatory / Legal ----
        if FINE_HINT.search(s):
            m = MONEY.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "fine_amount", unit, val, evidence=m.group(0))
            else:
                _emit(rows, report_id, section_id, company_name, section_name,
                      "fine_event", "enum", "mentioned", evidence=FINE_HINT.search(s).group(0))
        if LAWSUIT_HINT.search(s):
            _emit(rows, report_id, section_id, company_name, section_name,
                  "legal_event", "enum", "lawsuit/settlement", evidence=LAWSUIT_HINT.search(s).group(0))
        lic = LICENSE_HINT.search(s)
        if lic:
            _emit(rows, report_id, section_id, company_name, section_name,
                  "license_status", "enum", lic.group(2).lower(), evidence=lic.group(0))

        # ---- Guidance / Results ----
        ga = GUIDANCE_ACT.search(s)
        if ga:
            _emit(rows, report_id, section_id, company_name, section_name,
                  "guidance_action", "enum", ga.group(1).lower(), evidence=ga.group(0))

        if EBITDA_HINT.search(s):
            m = MONEY.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "ebitda_value", unit, val, evidence=m.group(0))

        rd = REVENUE_DIR.search(s)
        if rd:
            _emit(rows, report_id, section_id, company_name, section_name,
                  "revenue_direction", "enum", rd.group(1).lower(), evidence=rd.group(0))

        # ---- M&A / Investment ----
        if ACQ_HINT.search(s):
            _emit(rows, report_id, section_id, company_name, section_name,
                  "mna_event", "enum", "acquisition", evidence=ACQ_HINT.search(s).group(0))
            m = CONSID_VAL.search(s)
            if m:
                val, unit = _norm_money(m.group(1), m.group(2))
                _emit(rows, report_id, section_id, company_name, section_name,
                      "mna_consideration", unit, val, evidence=m.group(0))
        if DIV_HINT.search(s):
            _emit(rows, report_id, section_id, company_name, section_name,
                  "mna_event", "enum", "divestiture", evidence=DIV_HINT.search(s).group(0))

        # ---- Operations / Facility ----
        if FACILITY_HINT.search(s) and OPS_VERB.search(s):
            _emit(rows, report_id, section_id, company_name, section_name,
                  "operations_event", "enum", OPS_VERB.search(s).group(1).lower(), evidence=s)
        cap = CAPACITY_VAL.search(s)
        if cap:
            # cap.group(2) holds "123 MW" or similar; capture unit string in group(3)
            _emit(rows, report_id, section_id, company_name, section_name,
                  "capacity_change", cap.group(3).lower(), cap.group(2), evidence=cap.group(0))

    return rows

if __name__ == '__main__':
    pass