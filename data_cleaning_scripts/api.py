import pandas as pd
import requests
import urllib3
import time
import os
import argparse
from dotenv import load_dotenv

# 1) SETUP
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Read token from environment (supports .env via load_dotenv())
token = (os.getenv("DATAWEB_TOKEN") or "").strip()
if not token:
    raise SystemExit(
        "Missing API token. Set DATAWEB_TOKEN (preferred) or USITC_DATAWEB_TOKEN."
    )

base_url = "https://datawebws.usitc.gov/dataweb"
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": "Bearer " + token,
}

metrics = [
    "CONS_CUSTOMS_VALUE",
    "CONS_FIR_UNIT_QUANT",
]

# Configuration
START_YEAR = 1996
END_YEAR = 2005
OUTPUT_DIR = "data"
DELAY_BETWEEN_REQUESTS = 3  # seconds - increased to avoid rate limiting
DELAY_BETWEEN_YEARS = 5  # seconds between years
MAX_RETRIES = 5  # max retries on 429 error
RETRY_BASE_DELAY = 30  # base delay for retry backoff (seconds)
REQUEST_TIMEOUT = 120
HS_CHAPTER_START = 0
HS_CHAPTER_END = 99


def fetch_data_by_hs_chapter(year, hs_chapter, metric_codes, request_timeout):
    """
    Fetch data for a specific year and 2-digit HS chapter (00-99).
    Requests all metrics in a single API call.
    """
    # HS chapter as 2-digit string (e.g , "01", "85")
    hs_prefix = f"{hs_chapter:02d}"

    requestData = {
        "savedQueryType": "",
        "isOwner": True,
        "unitConversion": "0",
        "manualConversions": [],
        "reportOptions": {"tradeType": "Import", "classificationSystem": "HTS"},
        "searchOptions": {
            "MiscGroup": {
                "districts": {
                    "aggregation": "Aggregate District",
                    "districtGroups": {},
                    "districts": [],
                    "districtsExpanded": [{"name": "All Districts", "value": "all"}],
                    "districtsSelectType": "all",
                },
                "importPrograms": {
                    "aggregation": None,
                    "importPrograms": [],
                    "programsSelectType": "all",
                },
                "extImportPrograms": {
                    "aggregation": "Aggregate CSC",
                    "extImportPrograms": [],
                    "extImportProgramsExpanded": [],
                    "programsSelectType": "all",
                },
                "provisionCodes": {
                    "aggregation": "Aggregate RPCODE",
                    "provisionCodesSelectType": "all",
                    "rateProvisionCodes": [],
                    "rateProvisionCodesExpanded": [],
                    "rateProvisionGroups": {"systemGroups": []},
                },
            },
            "commodities": {
                "aggregation": "Break Out Commodities",
                "codeDisplayFormat": "NO",
                "commodities": [hs_prefix],  # e.g., ["01"]
                "commoditiesExpanded": [
                    {"name": f"Chapter {hs_prefix}", "value": hs_prefix}
                ],
                "commoditiesManual": hs_prefix,
                "commodityGroups": {"systemGroups": [], "userGroups": []},
                "commoditySelectType": "list",  # Use "list" not "manual"
                "granularity": "10",  # 10-digit HTS level
                "groupGranularity": None,
                "searchGranularity": None,
                "showHTSValidDetails": "",
            },
            "componentSettings": {
                "dataToReport": metric_codes,  # All metrics in one call
                "scale": "1",
                "timeframeSelectType": "fullYears",
                "years": [str(year)],
                "startDate": None,
                "endDate": None,
                "startMonth": None,
                "endMonth": None,
                "yearsTimeline": "Monthly",
            },
            "countries": {
                "aggregation": "Break Out Countries",
                "countries": ["5700"],
                "countriesExpanded": [{"name": "China - CN - CHN", "value": "5700"}],
                "countriesSelectType": "list",
                "countryGroups": {"systemGroups": [], "userGroups": []},
            },
        },
        "sortingAndDataFormat": {
            "DataSort": {
                "columnOrder": ["COUNTRY", "YEAR", "HTS10"],
                "fullColumnOrder": [
                    {
                        "checked": False,
                        "disabled": False,
                        "hasChildren": False,
                        "name": "HTS10",
                        "value": "HTS10",
                        "classificationSystem": "",
                        "groupUUID": "",
                        "items": [],
                        "tradeType": "",
                    },
                    {
                        "checked": False,
                        "disabled": False,
                        "hasChildren": False,
                        "name": "Year",
                        "value": "YEAR",
                        "classificationSystem": "",
                        "groupUUID": "",
                        "items": [],
                        "tradeType": "",
                    },
                    {
                        "checked": False,
                        "disabled": False,
                        "hasChildren": False,
                        "name": "Countries",
                        "value": "COUNTRY",
                        "classificationSystem": "",
                        "groupUUID": "",
                        "items": [],
                        "tradeType": "",
                    },
                ],
                "sortOrder": [
                    {"sortData": "HTS10", "orderBy": "asc", "year": "0"},
                    {"sortData": "Year", "orderBy": "asc", "year": "0"},
                    {"sortData": "Countries", "orderBy": "asc", "year": "0"},
                ],
            },
            "reportCustomizations": {
                "exportCombineTables": False,
                "totalRecords": "20000",
                "exportRawData": False,
            },
        },
        "deletedCountryUserGroups": [],
        "deletedCommodityUserGroups": [],
        "deletedDistrictUserGroups": [],
    }

    response = requests.post(
        base_url + "/api/v2/report2/runReport",
        headers=headers,
        json=requestData,
        verify=True,
        timeout=request_timeout,
    )
    return response


def get_api_column_labels(resp_json):
    """
    Extract real column header labels from DataWeb response.
    """
    table0 = resp_json["dto"]["tables"][0]
    labels = []
    for cg in table0.get("column_groups", []):
        for col in cg.get("columns", []):
            labels.append(col.get("label"))
    return labels


def find_column_index(column_labels, target_label):
    """Find a column index by label (case-insensitive), with a small fallback."""
    normalized = [(lbl or "").strip().lower() for lbl in column_labels]
    target = (target_label or "").strip().lower()

    if target and target in normalized:
        return normalized.index(target)

    # Fallback: tolerate minor label variations from the API
    for i, lbl in enumerate(normalized):
        if "quantity" in lbl and "description" in lbl:
            return i

    return None


def parse_response(response):
    """Parse the API response and return (column_labels, rows)."""
    rows = []
    labels = []

    if response.status_code != 200:
        print(f"    Error: Status {response.status_code}")
        return labels, rows

    try:
        resp_json = response.json()
    except ValueError as e:
        print(f"    Error parsing JSON: {e}")
        return labels, rows

    # Get column labels from API metadata (these correspond to rowEntries/value positions)
    raw_labels = get_api_column_labels(resp_json)
    qty_desc_idx = find_column_index(raw_labels, "Quantity Description")

    # Add "Data Type" as first column to distinguish metrics
    labels = ["Data Type"] + raw_labels

    dto = resp_json.get("dto", {})
    tables = dto.get("tables", [])

    for table in tables:
        for row_group in table.get("row_groups", []):
            for row in row_group.get("rowsNew", []):
                row_entries = row.get("rowEntries", [])
                row_values = [entry.get("value") for entry in row_entries]

                # Determine data type from the API's "Quantity Description" column
                # "Value for: X" means Customs Value, otherwise it's First Unit Quantity
                data_type = "Unknown"
                if qty_desc_idx is not None and len(row_values) > qty_desc_idx:
                    qty_desc = row_values[qty_desc_idx] or ""
                    if isinstance(qty_desc, str) and qty_desc.startswith("Value for:"):
                        data_type = "Customs Value"
                    else:
                        data_type = "First Unit Quantity"

                rows.append([data_type] + row_values)

    return labels, rows


def resolve_column_names(api_labels, max_cols):
    """
    Resolve output column names from API labels.
    Fail fast if labels are missing/misaligned instead of silently using Col_*.
    """
    if not api_labels:
        raise RuntimeError(
            "Missing API column labels; aborting to avoid writing generic Col_* headers."
        )

    if len(api_labels) < max_cols:
        raise RuntimeError(
            f"API label count mismatch (labels={len(api_labels)}, row_columns={max_cols}); "
            "aborting to avoid writing generic Col_* headers."
        )

    return api_labels[:max_cols]


def parse_args():
    parser = argparse.ArgumentParser(
        description="USITC DataWeb API - China Imports by year/chapter"
    )
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument(
        "--year", type=int, default=None, help="Shortcut for a single year"
    )
    parser.add_argument("--hs-chapter-start", type=int, default=HS_CHAPTER_START)
    parser.add_argument("--hs-chapter-end", type=int, default=HS_CHAPTER_END)
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR)
    parser.add_argument(
        "--delay-between-requests", type=float, default=DELAY_BETWEEN_REQUESTS
    )
    parser.add_argument(
        "--delay-between-years", type=float, default=DELAY_BETWEEN_YEARS
    )
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    parser.add_argument("--retry-base-delay", type=float, default=RETRY_BASE_DELAY)
    parser.add_argument("--request-timeout", type=int, default=REQUEST_TIMEOUT)
    parser.add_argument(
        "--no-merge-existing-year",
        action="store_true",
        help="Overwrite year CSV from this run instead of merging/deduping with existing file",
    )
    parser.add_argument(
        "--rebuild-combined",
        action="store_true",
        help="Rebuild a combined CSV from saved per-year files after run",
    )
    parser.add_argument("--combined-start-year", type=int, default=START_YEAR)
    parser.add_argument("--combined-end-year", type=int, default=END_YEAR)
    parser.add_argument(
        "--combined-file",
        type=str,
        default=None,
        help="Optional output path for rebuilt combined CSV",
    )
    return parser.parse_args()


def fetch_with_retries(year, hs_chapter, metric_codes, args):
    """
    Retry on:
    - HTTP 429 (rate limiting)
    - network exceptions (timeout/DNS/etc.)
    - HTTP 5xx transient server errors
    """
    response = None
    for attempt in range(args.max_retries):
        try:
            response = fetch_data_by_hs_chapter(
                year, hs_chapter, metric_codes, args.request_timeout
            )
        except requests.RequestException as e:
            if attempt == args.max_retries - 1:
                print(f"✗ Request error after {args.max_retries} attempts: {e}")
                return None
            retry_delay = args.retry_base_delay * (2**attempt)
            print(
                f"\n    Request error. Waiting {retry_delay}s before retry {attempt + 1}/{args.max_retries}...",
                end=" ",
            )
            time.sleep(retry_delay)
            continue

        if response.status_code == 429:
            if attempt == args.max_retries - 1:
                print(f"\n    Rate limited (429) after {args.max_retries} attempts")
                return None
            retry_delay = args.retry_base_delay * (2**attempt)
            print(
                f"\n    Rate limited (429). Waiting {retry_delay}s before retry {attempt + 1}/{args.max_retries}...",
                end=" ",
            )
            time.sleep(retry_delay)
            continue

        if 500 <= response.status_code < 600:
            if attempt == args.max_retries - 1:
                print(
                    f"\n    Server error ({response.status_code}) after {args.max_retries} attempts"
                )
                return response
            retry_delay = args.retry_base_delay * (2**attempt)
            print(
                f"\n    Server error ({response.status_code}). Waiting {retry_delay}s before retry {attempt + 1}/{args.max_retries}...",
                end=" ",
            )
            time.sleep(retry_delay)
            continue

        return response

    return response


def merge_year_dataframe(year_file, df_new):
    """
    Merge new rows into an existing year CSV and drop exact duplicates.
    Returns (df_merged, existing_rows, added_rows).
    """
    df_new = df_new.fillna("")

    if not os.path.exists(year_file):
        return df_new, 0, len(df_new)

    df_existing = pd.read_csv(year_file, dtype=str).fillna("")

    if list(df_existing.columns) != list(df_new.columns):
        raise RuntimeError(
            f"Column mismatch for {year_file}; existing and new columns differ."
        )

    existing_unique = df_existing.drop_duplicates(ignore_index=True)
    df_merged = pd.concat([existing_unique, df_new], ignore_index=True).drop_duplicates(
        ignore_index=True
    )
    added_rows = len(df_merged) - len(existing_unique)
    return df_merged, len(df_existing), added_rows


def rebuild_combined_from_year_files(
    output_dir, start_year, end_year, combined_file=None
):
    """
    Build a combined CSV from saved per-year files and dedupe exact rows.
    """
    frames = []
    missing_years = []
    expected_columns = None

    for year in range(start_year, end_year + 1):
        year_file = os.path.join(output_dir, f"China_Imports_{year}.csv")
        if not os.path.exists(year_file):
            missing_years.append(year)
            continue

        df_year = pd.read_csv(year_file, dtype=str).fillna("")
        if expected_columns is None:
            expected_columns = list(df_year.columns)
        elif list(df_year.columns) != expected_columns:
            raise RuntimeError(
                f"Column mismatch in {year_file}; cannot safely rebuild combined file."
            )
        frames.append(df_year)

    if not frames:
        raise RuntimeError(
            f"No per-year files found in {output_dir} for {start_year}-{end_year}."
        )

    df_combined = pd.concat(frames, ignore_index=True).drop_duplicates(
        ignore_index=True
    )

    if combined_file is None:
        combined_file = os.path.join(
            output_dir, f"China_Imports_{start_year}_{end_year}.csv"
        )

    df_combined.to_csv(combined_file, index=False)
    print(f"\nRebuilt {combined_file}")
    print(f"Total rows: {len(df_combined)}")
    if missing_years:
        print(
            "Missing year files (not included): "
            + ", ".join(str(year) for year in missing_years)
        )

    return combined_file


def main():
    args = parse_args()

    start_year = args.year if args.year is not None else args.start_year
    end_year = args.year if args.year is not None else args.end_year

    if start_year > end_year:
        raise SystemExit("start-year must be <= end-year")
    if not (0 <= args.hs_chapter_start <= 99 and 0 <= args.hs_chapter_end <= 99):
        raise SystemExit("hs-chapter-start and hs-chapter-end must be in 0..99")
    if args.hs_chapter_start > args.hs_chapter_end:
        raise SystemExit("hs-chapter-start must be <= hs-chapter-end")
    if args.max_retries < 1:
        raise SystemExit("max-retries must be >= 1")

    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 60)
    print("USITC DataWeb API - China Imports 1996-2005")
    print("Breaking down by HS Chapter (00-99) to avoid row limits")
    print(
        f"Run range: years {start_year}-{end_year}, chapters {args.hs_chapter_start:02d}-{args.hs_chapter_end:02d}"
    )
    print("=" * 60)

    all_data = []
    api_labels = None  # Will store column labels from first successful response

    for year in range(start_year, end_year + 1):
        print(f"\n{'='*60}")
        print(f"YEAR {year}")
        print(f"{'='*60}")

        year_data = []
        failed_chapters = []

        for hs_chapter in range(args.hs_chapter_start, args.hs_chapter_end + 1):
            hs_str = f"{hs_chapter:02d}"
            print(f"  Fetching HS Chapter {hs_str}...", end=" ")

            try:
                response = fetch_with_retries(year, hs_chapter, metrics, args)

                if response is None:
                    failed_chapters.append(hs_chapter)
                    time.sleep(args.delay_between_requests)
                    continue

                labels, rows = parse_response(response)
                if response.status_code != 200:
                    failed_chapters.append(hs_chapter)

                # Store column labels from first successful response
                if labels and api_labels is None:
                    api_labels = labels

                if rows:
                    year_data.extend(rows)
                    print(f"✓ {len(rows)} rows")
                else:
                    print("✓ 0 rows (no data)")

            except requests.RequestException as e:
                failed_chapters.append(hs_chapter)
                print(f"✗ Request error: {e}")
            except (ValueError, KeyError, TypeError, IndexError) as e:
                failed_chapters.append(hs_chapter)
                print(f"✗ Parse/processing error: {e}")

            # Delay between requests to avoid rate limiting
            time.sleep(args.delay_between_requests)

        # Save year data incrementally
        if year_data:
            max_cols = max(len(row) for row in year_data)
            col_names = resolve_column_names(api_labels, max_cols)

            year_data_padded = [
                row + [None] * (max_cols - len(row)) for row in year_data
            ]
            df_year_new = pd.DataFrame(year_data_padded, columns=col_names)

            year_file = os.path.join(args.output_dir, f"China_Imports_{year}.csv")
            if args.no_merge_existing_year:
                df_year_final = df_year_new.fillna("")
                existing_rows = 0
                added_rows = len(df_year_final)
            else:
                df_year_final, existing_rows, added_rows = merge_year_dataframe(
                    year_file, df_year_new
                )

            df_year_final.to_csv(year_file, index=False)
            all_data.extend(df_year_final.values.tolist())
            if args.no_merge_existing_year:
                print(f"\n  Saved {year_file} ({len(df_year_final)} rows)")
            else:
                print(
                    f"\n  Saved {year_file} ({len(df_year_final)} total rows; +{added_rows} new rows, {existing_rows} previously saved)"
                )
        else:
            print("  No successful chapter data for this year in this run.")

        if failed_chapters:
            failed_display = ", ".join(f"{ch:02d}" for ch in failed_chapters)
            print(f"  Failed chapters to backfill later: {failed_display}")

        # Longer delay between years
        if year < end_year:
            print(f"\n  Waiting {args.delay_between_years}s before next year...")
            time.sleep(args.delay_between_years)

    # Save combined file
    print(f"\n{'='*60}")
    print("SAVING COMBINED DATA")
    print(f"{'='*60}")

    if all_data:
        max_cols = max(len(row) for row in all_data)
        col_names = resolve_column_names(api_labels, max_cols)

        all_data_padded = [row + [None] * (max_cols - len(row)) for row in all_data]

        df_combined = pd.DataFrame(all_data_padded, columns=col_names)
        combined_file = os.path.join(
            args.output_dir, f"China_Imports_{start_year}_{end_year}.csv"
        )
        df_combined.to_csv(combined_file, index=False)

        print(f"\nSaved {combined_file}")
        print(f"Total rows: {len(df_combined)}")
        print("\nPreview:")
        print(df_combined.head(10))
    else:
        print("\nNo data collected!")

    if args.rebuild_combined:
        print(f"\n{'='*60}")
        print("REBUILDING MASTER COMBINED FILE")
        print(f"{'='*60}")
        rebuild_combined_from_year_files(
            output_dir=args.output_dir,
            start_year=args.combined_start_year,
            end_year=args.combined_end_year,
            combined_file=args.combined_file,
        )

    print("\nDone!")


if __name__ == "__main__":
    main()
