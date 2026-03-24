from __future__ import annotations

import pandas as pd
from typing import Literal, List, Dict, Union
from .expand import expand_row

Value = Union[int, float]

def build_wide(
    df_long: pd.DataFrame,
    primary_key: Literal["cdl", "nlcd"],
    include_families: List[str],
    include_explicit: List[str],
    hsg_count: int = 7,
    condition_filter: str | None = None,
) -> pd.DataFrame:
    """Build the wide output from normalized/validated long dataframe.

    Args:
        df_long: normalized & validated long dataframe (with parval1 rounded).
        primary_key: 'cdl' or 'nlcd' key to use in wide table.
        include_families: list of family names to expand (cn, rz, max_net_infil).
        include_explicit: singleton column names to pass through.
        hsg_count: expected HSG count (default 7). Supports 7 or 4.
        condition_filter: if provided ('drained'/'undrained'), include only rows with that condition.

    Returns:
        wide dataframe with leading key column and selected parameters.
    """
    key_col = "lu_cdl" if primary_key == "cdl" else "lu_nlcd"

    # Filter rows based on chosen key & optional condition
    df = df_long.copy()
    df = df[df[key_col].astype(str).str.len() > 0]  # drop rows with empty chosen key
    if condition_filter:
        df = df[df["condition"] == condition_filter]

    # Collect rows into a dict keyed by chosen primary key
    rows: Dict[str, Dict[str, Value]] = {}

    for _, r in df.iterrows():
        k = r[key_col]
        expanded = expand_row(r, include_families, include_explicit)
        if not expanded:
            continue
        rows.setdefault(k, {})
        rows[k].update(expanded)  # duplicate-prevention is handled earlier

    if not rows:
        return pd.DataFrame(columns=[key_col])

    wide = pd.DataFrame.from_dict(rows, orient="index").reset_index().rename(columns={"index": key_col})

    # If hsg_count == 4, drop indices 5..7
    if hsg_count == 4:
        drop_cols = [c for c in wide.columns if c.endswith(("_5", "_6", "_7"))]
        wide = wide.drop(columns=drop_cols)

    # Order columns: key first, families expanded, then singletons
    fam_cols: List[str] = []
    for fam in include_families:
        prefix = {"cn": "cn_", "rz": "rz_", "max_net_infil": "max_net_infil_"}[fam]
        rng = range(1, 5) if hsg_count == 4 else range(1, 8)
        fam_cols += [f"{prefix}{i}" for i in rng if f"{prefix}{i}" in wide.columns]

    sing_cols = [c for c in include_explicit if c in wide.columns]
    cols = [key_col] + fam_cols + sing_cols

    remaining = [c for c in wide.columns if c not in cols]
    return wide[cols + remaining]