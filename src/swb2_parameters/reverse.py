from __future__ import annotations

import pandas as pd
from typing import List, Dict

FAMILY_PREFIXES = {"cn": "cn_", "rz": "rz_", "max_net_infil": "max_net_infil_"}
DEFAULT_UNITS = {"cn": "unitless", "rz": "ft", "max_net_infil": "in/day"}

META_COLS = {
    "lu_code", "lu_code2", "lu_code_2", "lu_cdl", "lu_nlcd",
    "group", "parlbnd", "parubnd", "parval1", "units", "notes", "ref", "drained_condition",
}

LONG_COLUMNS = [
    "lu_code",
    "lu_cdl",
    "lu_nlcd",
    "description",
    "group",
    "column",
    "parlbnd",
    "parubnd",
    "parval1",
    "units",
    "notes",
    "ref",
    "drained_condition",
]


def _normalize(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _detect_wide_key_column(df_wide: pd.DataFrame) -> str:
    norm_map = { _normalize(c): c for c in df_wide.columns }
    for norm, actual in norm_map.items():
        if norm.startswith("lucode"):
            return actual
    for cand in ("lu_cdl", "lu_nlcd"):
        if cand in df_wide.columns:
            return cand
    raise KeyError(
        "Wide table must contain a land-use key column: a name beginning with 'lu_code', "
        "or a fallback 'lu_cdl'/'lu_nlcd'. None were found."
    )


def _find_description_columns(df_wide: pd.DataFrame) -> List[str]:
    return [c for c in df_wide.columns if _normalize(c).startswith("description")]


def _units_for_column(col: str) -> str:
    c = col.lower()
    if c.startswith(FAMILY_PREFIXES["cn"]): return DEFAULT_UNITS["cn"]
    if c.startswith(FAMILY_PREFIXES["rz"]): return DEFAULT_UNITS["rz"]
    if c.startswith(FAMILY_PREFIXES["max_net_infil"]): return DEFAULT_UNITS["max_net_infil"]
    return ""


def make_long_from_wide(
    df_wide: pd.DataFrame,
    include_families: List[str],
    include_explicit: List[str] | None = None,
) -> pd.DataFrame:
    """Create a baseline *long* table from a *wide* table.

    Returns an empty DataFrame with the canonical columns if no rows/parameters are found.
    """
    # If the wide frame itself is empty, return an empty long frame right away.
    if df_wide is None or df_wide.empty:
        return pd.DataFrame(columns=LONG_COLUMNS)

    # Detect key & description sources
    key_col = _detect_wide_key_column(df_wide)
    desc_cols = _find_description_columns(df_wide)

    rows: List[Dict[str, str | int | float]] = []

    # Build a list of parameter columns once (skip land-use keys, description*, meta)
    param_cols: List[str] = []
    for col in df_wide.columns:
        norm = _normalize(col)
        if col == key_col: continue
        if norm.startswith("lucode"): continue
        if norm.startswith("description"): continue
        if col in META_COLS: continue
        param_cols.append(col)

    # If there are no parameter columns, return an empty long frame (with columns)
    if not param_cols:
        return pd.DataFrame(columns=LONG_COLUMNS)

    # Emit long rows
    for _, wide_row in df_wide.iterrows():
        key_val = str(wide_row.get(key_col, "")).strip()

        # First non-empty description across any description* columns
        description_val = ""
        for dc in desc_cols:
            val = wide_row.get(dc)
            if pd.notna(val):
                s = str(val).strip()
                if s:
                    description_val = s
                    break

        for col in param_cols:
            val = wide_row[col]
            if pd.isna(val):  # skip empty
                continue

            rows.append({
                "lu_code": key_val,
                "lu_cdl": "",
                "lu_nlcd": "",
                "description": description_val,
                "group": "",
                "column": col,
                "parlbnd": "",
                "parubnd": "",
                "parval1": val,
                "units": _units_for_column(col),
                "notes": "",
                "ref": "",
                "drained_condition": "",
            })

    # Return a DataFrame even if rows is empty
    return pd.DataFrame(rows, columns=LONG_COLUMNS)