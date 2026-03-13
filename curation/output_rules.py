"""
Configurable output rules: merge sheets, upsert by key, etc.

Rules are applied after cell-level curation and before writing.
Each rule is a dict; supported types are documented in apply_output_rules.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

import pandas as pd


# Rule type for merging several source sheets into one target sheet,
# upserting by a key column and never overwriting non-empty with empty.
MERGE_UPSERT = "merge_upsert"

# Default rules: add more dicts here or load from file later.
OUTPUT_RULES: List[Dict[str, Any]] = [
    {
        "type": MERGE_UPSERT,
        "sources": ["LSI 14-1", "LSI 14-2", "LSI 14-3"],
        "target": "LSI 14",
        "key_column": "Site ID",
        # For LSI 14 we also maintain a \"Total score\" column that is
        # recomputed as the sum of all other columns whose header contains
        # the word \"total\".
        "total_score_column": "Total score",
        "total_from_contains": "total",
    },
]


def get_output_rules() -> List[Dict[str, Any]]:
    """Return the list of output rules (configurable)."""
    return list(OUTPUT_RULES)


def _is_empty(val: Any) -> bool:
    return pd.isna(val) or val == ""


def _is_zero(val: Any) -> bool:
    """
    Return True if val represents numeric zero.

    Treats 0, 0.0, and string forms like "0" or "0.0" as zero.
    """
    if isinstance(val, (int, float)):
        return val == 0
    if isinstance(val, str):
        try:
            return float(val) == 0.0
        except ValueError:
            return False
    return False


def _recompute_total_score(
    df: pd.DataFrame,
    total_column: str,
    keyword: str,
) -> pd.DataFrame:
    """
    Recompute a \"Total score\"-style column as the row-wise sum of all
    other columns whose name contains the given keyword.

    Non-numeric values are ignored in the sum. The total column itself
    is excluded from the inputs.
    """
    if df.empty:
        return df

    keyword_lower = keyword.lower()
    total_lower = total_column.lower()

    candidate_cols = [
        col
        for col in df.columns
        if keyword_lower in str(col).lower() and str(col).lower() != total_lower
    ]

    if not candidate_cols:
        return df

    numeric_part = df[candidate_cols].apply(pd.to_numeric, errors="coerce")
    df[total_column] = numeric_part.sum(axis=1, skipna=True)
    return df


def _upsert_by_key(
    combined_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_column: str,
) -> pd.DataFrame:
    """Merge new_df into combined_df by key_column; never overwrite non-empty with empty."""
    if key_column not in new_df.columns:
        return combined_df

    if combined_df.empty:
        combined_df = new_df.copy()
    elif key_column not in combined_df.columns:
        combined_df = combined_df.copy()
        combined_df[key_column] = pd.NA

    for _, row in new_df.iterrows():
        key_val = row.get(key_column)
        if _is_empty(key_val):
            continue

        existing_cols = list(combined_df.columns)
        row_dict = row.to_dict()

        for col in existing_cols:
            if col not in row_dict:
                row_dict[col] = pd.NA

        for col in row_dict:
            if col not in combined_df.columns:
                combined_df[col] = pd.NA

        matches = combined_df.index[combined_df[key_column] == key_val].tolist()

        if not matches:
            combined_df = pd.concat(
                [combined_df, pd.DataFrame([row_dict])],
                ignore_index=True,
            )
        else:
            idx = matches[0]
            for col, val in row_dict.items():
                if _is_empty(val):
                    # Source empty -> never copy.
                    continue

                existing_val = combined_df.at[idx, col]

                if _is_empty(existing_val):
                    # Target empty -> always take source.
                    combined_df.at[idx, col] = val
                elif _is_zero(existing_val) and not _is_zero(val):
                    # Target is zero, source is a different non-zero value -> replace.
                    combined_df.at[idx, col] = val
                else:
                    # Target has a non-empty, non-zero value -> keep it.
                    continue

    return combined_df


def apply_output_rules(
    curated_rows: Dict[str, pd.DataFrame],
    existing_sheets: Dict[str, pd.DataFrame],
    rules: List[Dict[str, Any]],
) -> Tuple[Dict[str, pd.DataFrame], Set[str]]:
    """
    Apply configured output rules to curated per-sheet data.

    Args:
        curated_rows: sheet_name -> DataFrame of curated new rows.
        existing_sheets: sheet_name -> DataFrame of current content (for merge targets).
        rules: list of rule dicts (e.g. from get_output_rules()).

    Returns:
        (rows_to_write, overwrite_sheet_names)
        - rows_to_write: sheet_name -> DataFrame to write (merged or as-is).
        - overwrite_sheet_names: sheets that must be overwritten (e.g. merge targets);
          all others are written by appending.
    """
    rows_to_write: Dict[str, pd.DataFrame] = {}
    overwrite_sheets: Set[str] = set()
    consumed_sources: Set[str] = set()

    for rule in rules:
        if rule.get("type") == MERGE_UPSERT:
            sources = rule.get("sources") or []
            target = rule.get("target")
            key_column = rule.get("key_column", "Site ID")
            if not target:
                continue

            for s in sources:
                consumed_sources.add(s)

            existing = existing_sheets.get(target, pd.DataFrame())
            combined = existing.copy()

            for src in sources:
                if src in curated_rows:
                    df = curated_rows[src]
                    if not df.empty:
                        combined = _upsert_by_key(combined, df, key_column)

            # Optional rule extras: recompute a \"Total score\" column after merging.
            total_col = rule.get("total_score_column")
            total_kw = rule.get("total_from_contains")
            if total_col and total_kw:
                combined = _recompute_total_score(combined, total_col, total_kw)

            if not combined.empty:
                rows_to_write[target] = combined
                overwrite_sheets.add(target)

    for sheet_name, df in curated_rows.items():
        if sheet_name in consumed_sources:
            continue
        if df.empty:
            continue
        rows_to_write[sheet_name] = df

    return rows_to_write, overwrite_sheets


def sheets_to_load_for_rules(rules: List[Dict[str, Any]]) -> Set[str]:
    """Return set of sheet names that must be read from the target for rules (e.g. merge targets)."""
    out: Set[str] = set()
    for rule in rules:
        if rule.get("type") == MERGE_UPSERT and rule.get("target"):
            out.add(rule["target"])
    return out
