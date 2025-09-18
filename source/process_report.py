# get data from html to database
import json
from pathlib import Path
import re

import str_utils
import data_utils
import settings
import parse_html
from source.call_llm import EventFactorVariableExtractor


def is_section_include_event(name):
    if str_utils.normalize_for_match(name) in settings.EXCLUDE_SECTIONS:
        return False
    return True


def process_raw_data(file_path, rating_company="Fitch"):
    """
    get raw data
    :param rating_company: default is Fitch
    :param file_path: html report path file
    :return:
    """
    # initiate_database()
    html_paths = data_utils.get_all_html_files(file_path)
    articles_total = []
    # delete old record
    deleted = data_utils.delete_reports_by_rating_company(rating_company)
    print(f"{deleted} reports deleted.")
    if rating_company == "Fitch":
        for html_path in html_paths:
            articles = parse_html.parse_fitch_factiva(html_path)
            # Get original HTML filename (without extension)
            company_name = Path(html_path).stem
            for row in articles:
                row["company_name"] = company_name
            articles_total.extend(articles)
        data_utils.insert_reports(articles_total, rating_company)
        return


def extract_event(section_names=None):
    """
    Extract event-related sentences from selected report sections.

    Parameters
    ----------
    section_names : list[str] or None
        Optional list of section names to filter.
        - If None or empty, all sections will be fetched.

    Returns
    -------
    list of dict
        [
            {
                "report_id": int,
                "section_id": int,
                "company_name": str,
                "section_name": str,
                "event_text": str
            },
            ...
        ]
    """
    # 1. Load data from DB
    rows = data_utils.select_sections_from_db(section_names)

    events = []

    # 2. Iterate through each section
    extractor = EventFactorVariableExtractor(
        model="gpt-4.1-2025-04-14",  # set your model
        api_key=settings.GPT_KEY,  # or set OPENAI_API_KEY env var
        temperature=0.0,
        response_format_via_schema=True,
        max_chars=8000,
        rate_limit_per_sec=None  # set e.g., 2.0 to throttle if needed
    )
    row_list_need = []
    for row in rows:
        section_id, report_id, company_name, section_name, contents = row

        # Skip sections in the blacklist
        if not is_section_include_event(section_name):
            continue
        row_list_need.append(row)
        break

    # batch_rows = extractor.extract_batch_rows(row_list_need)
    # print("\n=== BATCH (ROWS WITH METADATA) RESULT ===")
    # print(json.dumps(batch_rows, ensure_ascii=False, indent=2))
    # out_path = "efv_batch_rows.json"
    # with open(out_path, "w", encoding="utf-8") as f:
    #     json.dump(batch_rows, f, ensure_ascii=False, indent=2)

    # print("Saved to:", out_path)
    with open("efv_batch_rows.json", "r", encoding="utf-8") as f:
        batch_rows = json.load(f)
    data_utils.delete_efv_by_section_names(section_names)
    stats = data_utils.insert_efv_rows(batch_rows)
    print(stats)
    return events


if __name__ == '__main__':
    # get_html_data
    # process_raw_data(settings.Fitch_report_file_path, "Fitch")
    extract_event(["liquidity and debt structure"])
