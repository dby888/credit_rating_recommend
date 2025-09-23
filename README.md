
# Credit Report Recommendation System

This project provides a recommendation engine for credit rating reports, focusing on Events, Factors, and Variables (EFV).
It helps analysts quickly identify relevant historical information when drafting specific report sections by combining company-specific data and industry-wide data.

## Project Structure
```
.
├── data/                            # Input data and intermediate files
│   └── reports/                     # Raw credit rating report HTML files
│
├── source/                          # Core source code
│   ├── analyze_text.py              # Core text analysis logic
│   ├── calculate_relation_score.py  # Compute sentence-level relationship scores
│   ├── call_llm.py                  # LLM integration for event/factor/variable extraction
│   ├── correct_data_id.py           # Scripts for correcting and validating data IDs
│   ├── data_utils.py                # Data preprocessing and transformation utilities
│   ├── fitch_articles.csv           # Example Fitch dataset for testing
│   ├── parse_html.py                # Parse and clean HTML reports
│   ├── process_report.py            # Process and structure rating report content
│   ├── recommend_compass.py         # Main recommendation engine module
│   ├── settings.py                  # Global configuration and constants
│   ├── snowflake_generators.py      # Unique ID generator using Snowflake algorithm
│   ├── str_utils.py                 # String handling utilities
│   ├── temp.json                    # Temporary cache or config file
│   └── utils/                       # General utility package (to be extended)
│
├── temp/                            # Temporary files and cache storage
│   └── main.py                      # Local test scripts for temp usage
│
├── main.py                          # Entry point (Command Line)
├── app_run.py                       # Entry point (UI)
├── README.md                        # Project documentation
├── requirements.txt                 # Python dependencies for project setup
├── Final Output.pdf                 # Sample Output
└── Process_Documentation.pdf        # Detailed Project documentation

```

## Key Modules

### 1. **Extract Text** (`parse_html.py, process_report.py`): 
   - Parses raw HTML credit rating reports into structured sections.
   - Processes parsed report data and prepares it for recommendation tasks.
   
### 2. **Extract Information** (`call_llm.py`):
   - Integrates Large Language Models (LLMs) to extract events, factors, and variables from report text.

### 3. **Relation Builder** (`calculate_relation_score.py`):
   - Calculates relationship scores between sentences for ranking events, factors, and variables in credit rating reports.

### 4. **Recommend Engine** (`recommend_compass.py`):
   - Core recommendation engine that generates Company, Global**, and Hybrid recommendations for events, factors, and variables.

### 5. **Entry** (`app_run.py, main.py`):
   - Unified entry script that supports both command-line interface (CLI) and interactive UI modes. 
   - Used to run the recommendation pipeline, view results, and manage configurations.

## Installation

Install the necessary dependencies using:

```bash
pip install -r requirements.txt
```


## Usage

### 1: Running the Recommendation Engine(UI)

```bash
python main.py
```

### 2: Running the Recommendation Engine (CLI)
Run the engine to generate top-ranked events, factors, and variables for a given company and section directly from the command line.
```bash
python main.py --company "Amazon.com, Inc." --sections "liquidity and debt structure"
```
- `<company>`: Company name (case-insensitive, must match report.company_name). Example: "Amazon.com, Inc."
- `<sections>`: One or more report section names for generating recommendations (exact match recommended). Example: "liquidity and debt structure"
- `<k_var>`: optional, Top-K variables to return (default: 8).
- `<k_factor>`: optional, Top-K factors to return (default: 6).
- `<k_event>`: optional, Top-K events to return (default: 6).
- `<year_min>`: optional, Minimum year for reports (inclusive). Example: 2023
- `<year_max>`: optional, Maximum year for reports (inclusive).  Example: 2024
- `<report_limit>`: optional, Maximum number of most recent reports to include.
- `<out>` :optional, JSON file path to save output instead of printing to console. Example: results/amazon_liquidity.json

If you encounter any issues, more info are in Process_Documentation in root.
