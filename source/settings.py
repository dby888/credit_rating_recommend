import os
import re

# Recommend Params
# Weight for company-specific recommendations
# Weight for the Company View.
# Controls how much the company’s historical data influences the final recommendation score.
weight_company = 0.55
# Weight for the Global View.
# Determines the contribution of aggregated industry-wide data to the final score.
weight_global = 0.45
# Weight for frequency factor.
# Adds importance to items that frequently appear in reports.
weight_frequency = 0.05
# Bonus score when an item exists in both Company and Global Views
# Bonus weight applied when an item appears in both Company and Global Views, boosting confidence in recommendations.
both_bonus = 0.05

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
# 'Summary of Financial Adjustments'
include_section = ['Derivation Summary', 'Key Assumptions', 'Key Rating Drivers', 'Liquidity and Debt Structure', 'Peer Analysis', "RATING SENSITIVITIES"]

TYPICAL_SECTION_NAME = [
    "summary",
    "rating action",
    "rating rationale",
    "key rating drivers",
    "rating sensitivities",
    "best worst case rating scenario",
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
    "summary of financial adjustments",
    "key assumptions",
    "peer analysis"
]

GPT_KEY = "Your GPT Key"

# Alias keywords (lowercased, purely for semantic matching)
# 只保留标准词，小写即可
ALIAS_KEYWORDS = {
    "summary": ["summary", "overview", "executive summary"],
    "rating action": ["rating action", "affirm", "upgrade", "downgrade", "revise"],
    "rating rationale": ["rationale", "reason", "justification"],
    "key rating drivers": ["key rating driver", "principal driver"],
    "rating sensitivities": ["rating sensitivity", "sensitivities"],
    "best worst case rating scenario": ["best case", "worst case", "upside case", "downside case"],
    "outlook": ["outlook"],
    "derivation summary": ["derivation summary"],
    "issuer profile": ["issuer profile", "company profile"],
    "summary of financial adjustments": ["summary of financial adjustments"],
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
    "peer analysis": ["peer analysis"],
    "key assumptions": ["key assumptions"],
    "additional disclosures":["additional disclosure","additional-disclosures","additional-disclosure"],
}

EXCLUDE_SECTIONS = {"contacts", "contact","issuer profile",
    "summary of financial adjustments",
    "references for substantially material source cited as key driver of rating",
    "macroeconomic assumptions and sector forecasts","summary of financial adjustments"}

# EVENT
RATING_EVENT_PATTERN = re.compile(
    r"\b(downgrad(?:e|ed|es)|upgrad(?:e|ed|es)|affirm(?:ed|s)?|revis(?:e|ed|es)|withdraw(?:n|s)?|"
    r"placed on (rating )?watch|outlook (revised|changed|to)|default(?:ed)?|filed for bankruptcy)\b",
    re.IGNORECASE
)

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



# EFV_SCHEMA: Dict[str, Any] = {
#     "type": "object",
#     "additionalProperties": False,
#     "properties": {
#         "events": {
#             "type": "array",
#             "description": (
#                 "Actions or happenings explicitly stated in THIS passage that have occurred, started, completed, or are clearly committed/announced, and that may affect the company’s stock price or be credit-relevant. Use an action-oriented phrase (past or clear present), include a time/period if shown, and quote the shortest verbatim evidence. If one sentence mentions multiple dates for the same action (e.g., April and June repayments), you may keep it as one event rather than force-splitting."
#             ),
#             "items": {
#                 "type": "object",
#                 "additionalProperties": False,
#                 "properties": {
#                     "name": {
#                         "type": "string",
#                         "description": (
#                             "Short, standardized label (≤5 words) for the realized action, "
#                             "used for indexing and cross-document matching. "
#                             "Do NOT include values, units, or time. "
#                             "Examples: 'Debt Repayment', 'Share Buyback Start', 'Acquisition Completed', 'Dividend Announcement'."
#                         )
#                     },
#                     "contents": {
#                         "type": "string",
#                         "description": (
#                             "Verbatim or lightly normalized action phrase in past/clear present tense (≤120 chars), "
#                             "capturing the action and its object/scope as written in THIS passage. "
#                             "Exclude numeric values, units, and time expressions (these belong in 'period' and 'evidence'). "
#                             "Examples: 'Repaid bond maturities', 'Began share repurchase program', "
#                             "'Completed acquisition of Whole Foods', 'Announced organizational restructuring'."
#                         ),
#                         "maxLength": 120
#                     },
#                     "event_type": {
#                         "type": "string",
#                         "enum": _EVENT_TYPE_ENUM,  # machine validation
#                         "description": (
#                             "One of the 18 controlled event categories:\n" + _EVENT_TYPE_DESC
#                         )
#                     },
#                     "period": {
#                         "type": ["string", "null"],
#                         "description": (
#                             "Verbatim time/period string from THIS passage if present (e.g., 'April 2025', 'June 2025', '2022', "
#                             "'as of Mar 31, 2025'); null if no explicit time is stated."
#                         )
#                     },
#                     "evidence": {
#                         "type": "string",
#                         "description": (
#                             "Shortest verbatim span from THIS passage proving the action (include the action verb and its object; "
#                             "may include the time phrase if present)."
#                         )
#                     }
#                 },
#                 # REQUIRED must include ALL keys in properties:
#                 "required": ["name", "contents", "event_type", "period", "evidence"]
#             }
#         },
#
#         "factors": {
#             "type": "array",
#             "description": (
#                 "Specific, reusable credit/rating considerations stated in THIS passage (drivers/constraints/policies/risks). "
#                 "Include explicit forward-looking statements; keep the name a concise noun phrase; evidence is verbatim."
#             ),
#             "items": {
#                 "type": "object",
#                 "additionalProperties": False,
#                 "properties": {
#                     "name": {
#                         "type": "string",
#                         "description": (
#                             "Short, standardized label (≤6 words) for the driver/constraint/policy/risk, "
#                             "used for indexing and reuse across documents. "
#                             "Do NOT include values, units, or time. "
#                             "Examples: 'Liquidity Backstop Availability', 'High Customer Concentration', "
#                             "'Leverage Reduction Commitment', 'Regulatory Investigation Risk', 'FX Exposure Hedging Policy'."
#                         )
#                     },
#                     "contents": {
#                         "type": "string",
#                         "description": (
#                             "Generate a concise, standardized factor description (≤15 words) that captures the core driver, "
#                             "constraint, policy, or risk stated in THIS passage. "
#                             "Do NOT include numeric values, dates, or subjective attribution such as 'Fitch expects' or 'the company'. "
#                             "Focus only on the essential concept so it can be reused across documents. "
#                         )
#                     },
#                     "period": {
#                         "type": ["string", "null"],
#                         "description": "Verbatim time/period string if present; null if absent."
#                     },
#                     "evidence": {
#                         "type": "string",
#                         "description": "Verbatim snippet from THIS passage that expresses this factor/driver."
#                     }
#                 },
#                 # REQUIRED must include ALL keys in properties:
#                 "required": ["name", "contents", "period", "evidence"]
#             }
#         },
#
#         "variables": {
#             "type": "array",
#             "description": (
#                 "All observable, measurable quantities mentioned in THIS passage (verbatim). Each item captures one metric/value mention with its unit and period if shown (set to null if absent). If the same metric appears with different values or periods, output multiple items to cover all occurrences."
#             ),
#             "items": {
#                 "type": "object",
#                 "additionalProperties": False,
#                 "properties": {
#                     "name": {
#                         "type": "string",
#                         "description": (
#                             "A concise, standardized label for the measurable metric mentioned in THIS passage, "
#                             "used for quick identification and filtering. "
#                             "It should be a short noun phrase (≤5 words), capturing the essence of the metric "
#                             "without numeric values or periods. Examples: 'Total Debt', 'EBITDA Margin', "
#                             "'Cash Balance', 'Interest Coverage Ratio'."
#                         )
#                     },
#                     "contents": {
#                         "type": "string",
#                         "description": (
#                             "Verbatim (or lightly normalized) metric description as written in THIS passage, "
#                             "excluding any numeric values, units, or time expressions. Capture the full noun phrase "
#                             "and necessary qualifiers/scope (e.g., 'cash and cash equivalents', "
#                             "'total debt excluding leases', 'operating cash flow before working capital'). "
#                             "Text only: do not include values, dates, or symbols such as $, %, or x. "
#                             "This differs from 'name', which is a short standardized label (≤5 words) used for indexing "
#                             "and cross-document matching."
#                         )
#                     },
#                     "value": {
#                         "type": "string",
#                         "description": "Verbatim numeric/date/ratio text (e.g., '$66.2 billion', '2.25 billion', '3.1x', 'FY2024')."
#                     },
#                     "unit": {
#                         "type": ["string", "null"],
#                         "description": "Verbatim unit token as it appears (e.g., '%','x','$','USD','bn','m'); null if not shown."
#                     },
#                     "period": {
#                         "type": ["string", "null"],
#                         "description": "Verbatim time/period expression (e.g., 'as of Mar 31, 2025','due Nov 2028','1H2025'); null if absent."
#                     },
#                     "evidence": {
#                         "type": "string",
#                         "description": "Verbatim snippet from THIS passage that contains the value."
#                     }
#                 },
#                 # REQUIRED must include ALL keys in properties:
#                 "required": ["name", "contents", "value", "unit", "period", "evidence"]
#             }
#         }
#     },
#     "required": ["events", "factors", "variables"]
# }