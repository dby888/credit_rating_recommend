import os
import re

# FILE PATH
current_file = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_file))  # go up two levels
data_file_path = os.path.join(project_root, "data")
temp_file_path = os.path.join(project_root, "temp")
database_path = os.path.join(data_file_path, "result.db")
Fitch_report_file_path = os.path.join(data_file_path, "Fitch Rating Reports")

# NAME
articles_subfix = "extracted_article"


# section names
TYPICAL_SECTION_NAME = [
    "summary",
    "rating action",
    "rating rationale",
    "key rating drivers",
    "rating sensitivities",
    "best/worst case rating scenario",
    "outlook",
    "derivation summary",
    "summary of financial adjustments",
    "macroeconomic assumptions and sector forecasts",
    "issuer profile",
    "liquidity and debt structure",
    "debt and liquidity",
    "esg considerations",
    "applicable criteria",
    "related research",
    "contacts",
    "references",
    "corporate rating",
    "instrument ratings",
    "issue rating",
    "ratings",
    "additional disclosures",
]


# Alias keywords (lowercased, purely for semantic matching)
# 只保留标准词，小写即可
ALIAS_KEYWORDS = {
    "summary": ["summary", "overview", "executive summary"],
    "rating action": ["rating action", "affirm", "upgrade", "downgrade", "revise"],
    "rating rationale": ["rationale", "reason", "justification"],
    "key rating drivers": ["key rating driver", "principal driver"],
    "rating sensitivities": ["rating sensitivity", "sensitivities"],
    "best/worst case rating scenario": ["best case", "worst case", "upside case", "downside case"],
    "outlook": ["outlook"],
    "derivation summary": ["derivation summary"],
    "issuer profile": ["issuer profile", "company profile"],
    "liquidity and debt structure": ["liquidity and debt structure"],
    "debt and liquidity": ["debt and liquidity"],
    "esg considerations": ["esg consideration"],
    "applicable criteria": ["applicable criteria", "relevant criteria"],
    "related research": ["related research"],
    "contacts": ["contact"],
    "references": ["references for substantially material source","references for substantially material source cited as key driver of rating", "information sources", "references"],
    "corporate rating": ["corporate rating"],
    "instrument ratings": ["instrument ratings", "issue ratings"],
    "issue rating": ["issue rating"],
    "ratings": ["ratings"],
    "additional disclosures":["additional disclosure","additional-disclosures","additional-disclosure"],
}

EXCLUDE_SECTIONS = {"contacts", "contact","issuer profile",
    "summary of financial adjustments",
    "references for substantially material source cited as key driver of rating",
    "macroeconomic assumptions and sector forecasts"}

# EVENT
RATING_EVENT_PATTERN = re.compile(
    r"\b(downgrad(?:e|ed|es)|upgrad(?:e|ed|es)|affirm(?:ed|s)?|revis(?:e|ed|es)|withdraw(?:n|s)?|"
    r"placed on (rating )?watch|outlook (revised|changed|to)|default(?:ed)?|filed for bankruptcy)\b",
    re.IGNORECASE
)

# https://link.springer.com/article/10.1007/s10579-021-09562-4/tables/2
EVENT_TYPES = [
    "CSR/brand","Deal","Dividend","Employment","Expense","Facility",
    "FinancialReport","Financing","Investment","Legal","Macroeconomics",
    "Merger/acquisition","Product/service","Profit/loss","Rating",
    "Revenue","SalesVolume","SecurityValue"
]
EVENT_TYPES_DEFINITION = {
    "CSR/brand": "Corporate social responsibility or brand reputation actions (e.g., donations, sustainability initiatives, awards/recognition).",
    "Deal": "Non-M&A commercial contracts or large orders (e.g., long-term supply agreements, major customer wins, strategic partnerships without control change).",
    "Dividend": "Dividend actions: initiation, increase, decrease, suspension, or special dividend announcements.",
    "Employment": "Workforce-related moves: hiring plans, layoffs, leadership/management changes, policy updates.",
    "Expense": "Notable cost items or programs: restructuring charges, cost-cut initiatives, opex guidance/cuts.",
    "Facility": "Physical assets/capacity: build, expand, commission, shutdown, restart of plants, data centers, mines, lines, etc.",
    "FinancialReport": "Earnings/results and related disclosures: metrics, guidance, restatements, beats/misses when framed as reporting.",
    "Financing": "Debt/equity financing and liquidity lines: issuance, refinancing, buybacks of debt, CP/RCF/revolver setup/usage, maturities, coupons.",
    "Investment": "Capital projects or minority/strategic stakes (non-control), joint ventures; distinct from M&A control transactions.",
    "Legal": "Litigation and regulatory actions: lawsuits, settlements, fines/penalties, approvals, revocations, injunctions.",
    "Macroeconomics": "Macro conditions or policy impacting the company/sector broadly (rates, tariffs, sector-wide regulation, benchmarks).",
    "Merger/acquisition": "Control-changing transactions: acquisitions, mergers; also divestitures/spin-offs resulting in control change.",
    "Product/service": "Product or service actions: launches, pricing changes, recalls, discontinuations, feature/policy updates.",
    "Profit/loss": "Profitability statements outside full earnings sections (e.g., net profit/loss disclosures, break-even points).",
    "Rating": "Credit rating actions by agencies: affirmations, upgrades, downgrades, outlook or watch changes.",
    "Revenue": "Top-line disclosures or guidance outside full results context (revenue levels, growth rates, drivers).",
    "SalesVolume": "Units/throughput metrics (shipments, production, sales units, utilization) without direct revenue mapping.",
    "SecurityValue": "Market prices/valuation of securities (stock/bond price moves, market cap changes) and their stated drivers."
}



extract_schema = {
  "type": "object",
  "title": "text analysis task",
  "description": "Extract three sets from the provided passage: events, variables, and factors. Copy all numbers/dates/entities verbatim into *_text fields (no normalization). Omit anything not present in the passage. Evidence snippets must come from the CURRENT passage.",
  "properties": {
    "events": {
      "type": "array",
      "description": "A realized, dated (or date-implied) occurrence that shifts the firm’s credit risk, with a clear direction or state change.",
      "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "summary": {
            "type": "string",
            "maxLength": 120,
            "description": "Concise summary (<=120 chars) of the event."
          },
          "evidence": {
            "type": "string",
            "description": "Verbatim evidence snippet from THIS passage supporting the event."
          }
        },
        "required": ["summary", "evidence"]
      }
    },
    "variables": {
      "type": "array",
      "description": "An observable, measurable quantity that quantifies a factor or the impact of an event, preferably with units/periods, and is suitable for time-series or cross-section analysis.",
      "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "name": {
            "type": "string",
            "description": "Variable/metric noun phrase from THIS passage (verbatim or lightly normalized label, e.g., 'net debt/EBITDA')."
          },
          "value_text": {
            "type": "string",
            "description": "Verbatim numeric/date/ratio text copied from THIS passage (no normalization)."
          },
          "unit": {
            "type": ["string", "null"],
            "description": "Verbatim unit text as appears in THIS passage (e.g., '%', 'x', '$', 'USD', 'bn', 'm'); null if not shown."
          },
          "evidence": {
            "type": "string",
            "description": "Verbatim evidence snippet from THIS passage that contains the value."
          }
        },
        "required": ["name", "value_text", "evidence"]
    },
    "factors": {
      "type": "array",
      "description": "A specific, reusable credit consideration (not a broad bucket), e.g., debt maturity concentration, interest-coverage deterioration, operating-cash-flow volatility, covenant-breach risk, regulatory fines, FX exposure, customer concentration, refinancing risk, asset-sale execution, working-capital swings, capex intensity.",
      "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "name": {
            "type": "string",
            "description": "Factor label from THIS passage (verbatim or lightly normalized, specific and actionable)."
          },
          "evidence": {
            "type": "string",
            "description": "Verbatim evidence snippet from THIS passage that expresses this factor/driver."
          }
        },
        "required": ["name", "evidence"]
      }
    }

  },
  "required": ["events", "variables", "factors"],
  "additionalProperties": False
}
}
