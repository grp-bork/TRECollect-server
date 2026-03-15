"""
Compute statistics and summary from curated sheet data and save to CSV.

For development: full_snapshot can be saved/loaded as pickle to avoid API calls.
"""

from __future__ import annotations

import pickle
from typing import Dict

import pandas as pd


def load_snapshot(path: str) -> Dict[str, pd.DataFrame]:
    """Load full_snapshot from pickle file (same structure as passed to save_snapshot)."""
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        return {}


def compute_and_save_statistics(full_snapshot: Dict[str, pd.DataFrame], configs: Dict[str, dict]) -> None:
    """
    Compute statistics and summary from the full curated spreadsheet snapshot
    and write results to a CSV file.

    full_snapshot: sheet_name -> DataFrame (complete contents of each sheet after curation).
    configs: reserved for later use.
    """
    full_snapshot = load_snapshot("full_snapshot.pkl")

    # Per site_id: which sheets and how many rows (sheet_name -> row count).
    per_site: Dict[str, Dict[str, int]] = {}

    for sheet_name, df in full_snapshot.items():
        if df.empty or "Site ID" not in df.columns:
            continue
        site_col = df["Site ID"].dropna().astype(str).str.strip()
        for sid in site_col[site_col != ""].unique():
            count = int((site_col == sid).sum())
            if sid not in per_site:
                per_site[sid] = {}
            per_site[sid][sheet_name] = count

    if not per_site:
        return

    sheet_names = sorted(
        name for name, df in full_snapshot.items()
        if not df.empty and "Site ID" in df.columns
    )
    columns = ["Site ID"] + sheet_names
    rows = []
    for site_id in sorted(per_site.keys()):
        row = {"Site ID": site_id}
        for sh in sheet_names:
            row[sh] = per_site[site_id].get(sh, 0)
        rows.append(row)

    out_df = pd.DataFrame(rows, columns=columns)
    out_df.to_csv("statistics.csv", index=False)
    print(f">>> Statistics written to statistics.csv")


if __name__ == "__main__":
    compute_and_save_statistics(None, None)