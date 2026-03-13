from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Dict, Set

import pandas as pd
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from APIs.google_spreadsheets import GoogleAPI
from processing.utils import get_last_curation_timestamp, save_last_curation_timestamp
from curation.annotation import curate_value
from curation.output_rules import (
    apply_output_rules,
    get_output_rules,
    sheets_to_load_for_rules,
)


def fetch_new_rows(google_api: GoogleAPI, source_sheet_id: str, last_timestamp: dt.datetime) -> Dict[str, pd.DataFrame]:
    """
    Read all worksheets from the source spreadsheet and return only rows
    whose `Submission date` is newer than `last_timestamp`.

    The returned mapping is:

        {
            "<worksheet_name>": DataFrame([...]),
            ...
        }
    """
    new_rows: Dict[str, pd.DataFrame] = {}

    if not source_sheet_id:
        return new_rows

    # Normalise comparison timestamp to UTC so we can safely compare against
    # the `Submission date` values, which are in ISO format with a trailing 'Z'
    # (UTC designator), e.g. "2026-03-13T09:07:32.901575Z".
    if last_timestamp.tzinfo is not None:
        last_ts_utc = last_timestamp.astimezone(dt.timezone.utc)
    else:
        last_ts_utc = last_timestamp.replace(tzinfo=dt.timezone.utc)

    worksheet_names = google_api.get_all_worksheets(source_sheet_id)

    for sheet_name in worksheet_names:
        df = google_api.read_table(source_sheet_id, sheet_name)

        if df.empty:
            continue

        if "Submission date" not in df.columns:
            # Not a submissions worksheet; skip it.
            continue

        # Parse submission timestamps as UTC-aware datetimes.
        # pandas.to_datetime understands the trailing 'Z' as UTC.
        submission_times = pd.to_datetime(
            df["Submission date"],
            utc=True,
            errors="coerce",
        )

        # Keep only rows with a valid timestamp strictly newer than last_ts_utc.
        mask = submission_times.notna() & (submission_times > last_ts_utc)
        filtered = df.loc[mask].reset_index(drop=True)

        if not filtered.empty:
            new_rows[sheet_name] = filtered

    return new_rows


def curate_rows_per_sheet(
    raw_rows: Dict[str, pd.DataFrame],
    owncloud_images_token: str,
) -> Dict[str, pd.DataFrame]:
    """
    Apply curation rules to all newly collected rows per worksheet.

    Internally this applies `curate_value` cell-wise to supported sheets
    and returns a new mapping with curated DataFrames.
    """
    curated: Dict[str, pd.DataFrame] = {}
    print(">>> Curating sheets")

    for sheet_name, df in raw_rows.items():
        print(f">>>'{sheet_name}' with {len(df)} rows.")
        # For now we only curate LSI sheets; others are ignored.
        if not sheet_name.startswith("LSI"):
            continue

        if df.empty:
            continue

        # Apply curate_value to every cell
        curated_df = df.map(lambda v: curate_value(v, owncloud_images_token))
        curated[sheet_name] = curated_df

    return curated


def load_existing_sheets(
    google_api: GoogleAPI,
    target_sheet_id: str,
    sheet_names: Set[str],
) -> Dict[str, pd.DataFrame]:
    """Load current content of the given sheets from the target spreadsheet."""
    result: Dict[str, pd.DataFrame] = {}
    if not target_sheet_id:
        return result
    existing_tabs = google_api.get_all_worksheets(target_sheet_id)
    for name in sheet_names:
        if name in existing_tabs:
            result[name] = google_api.read_table(target_sheet_id, name)
        else:
            result[name] = pd.DataFrame()
    return result


def write_curated_rows(
    google_api: GoogleAPI,
    target_sheet_id: str,
    rows_to_write: Dict[str, pd.DataFrame],
    overwrite_sheets: Set[str],
) -> None:
    """
    Write prepared data to the target spreadsheet.

    - Sheets in overwrite_sheets: full overwrite of the tab.
    - All other sheets: append rows to the tab (create if missing).
    """
    if not target_sheet_id:
        return

    print(f">>> Writing sheets")

    for sheet_name, df in rows_to_write.items():
        print(f">>>'{sheet_name}' with {len(df)} rows.")
        if df.empty:
            continue
        if sheet_name in overwrite_sheets:
            google_api.overwrite_table(target_sheet_id, sheet_name, df)
        else:
            row_dicts = df.to_dict(orient="records")
            google_api.add_rows(target_sheet_id, sheet_name, row_dicts)


def main(args: argparse.Namespace) -> None:
    """
    Top-level orchestration for LSI curation:

    1. Load configuration and last curation timestamp.
    2. Read new rows (based on `Submission date`) from all tabs.
    3. Curate the collected rows.
    4. Store curated data in the target spreadsheet.
    5. Update the stored timestamp.
    """
    load_dotenv("CONFIG.env")

    source_sheet_id = os.environ.get("RAW_SHEET_ID")
    lsi_target_sheet_id = os.environ.get("LSI_SHEET_LATEST_SUBMISSIONS_ID")
    owncloud_images_token = os.environ.get('OWNCLOUD_IMAGES_TOKEN')

    google_api = GoogleAPI()

    now = dt.datetime.now(ZoneInfo("Europe/Paris"))
    print(f'>>> {now}')
    last_timestamp = get_last_curation_timestamp()

    raw_rows = fetch_new_rows(google_api, source_sheet_id, last_timestamp)
    if raw_rows:
        curated = curate_rows_per_sheet(raw_rows, owncloud_images_token)

        rules = get_output_rules()
        sheets_to_load = sheets_to_load_for_rules(rules)
        existing_sheets = load_existing_sheets(google_api, lsi_target_sheet_id, sheets_to_load)

        rows_to_write, overwrite_sheets = apply_output_rules(curated, existing_sheets, rules)

        write_curated_rows(google_api, lsi_target_sheet_id, rows_to_write, overwrite_sheets)
        save_last_curation_timestamp(now)
    else:
        print(">>> No new rows to curate.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Curate submissions from Google Sheets.")
    # Outline: we can later add options like --since, --dry-run, etc.
    args = parser.parse_args()
    main(args)
