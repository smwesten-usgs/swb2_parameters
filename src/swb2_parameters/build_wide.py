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
    drained_condition_filter: str | None = None,
) -> pd.DataFrame:
    """Construct the SWB2‑ready *wide* table from the canonical *long* table.

    Leading key column is always **`lu_code`** to satisfy SWB2.
    We source `lu_code` from either `lu_cdl` or `lu_nlcd` depending on the
    selector’s `primary_key`. A single **`description`** column is also carried
    forward into wide by choosing the first non-empty description per `lu_code`.

    Args:
        df_long: Normalized & validated long DataFrame (with `parval1` rounded).
        primary_key: "cdl" or "nlcd"; selects the source key for `lu_code`.
        include_families: Families to expand (e.g., ['cn','rz','max_net_infil']).
        include_explicit: Singleton columns to pass through unchanged.
        hsg_count: HSG count (usually 7; 4 supported by dropping 5..7).
        drained_condition_filter: If provided, include only rows with this drained condition.

    Returns:
        Wide DataFrame with `lu_code`, `description`, requested families, and singletons.
    """
    src_key_col = "lu_cdl" if primary_key == "cdl" else "lu_nlcd"

    # Filter rows based on chosen source key & optional drained condition
    df = df_long.copy()
    df = df[df[src_key_col].astype(str).str.len() > 0]
    if drained_condition_filter:
        df = df[df["drained_condition"] == drained_condition_filter]

    rows: Dict[str, Dict[str, Value | str]] = {}
    for _, r in df.iterrows():
        k = r[src_key_col]
        expanded = expand_row(r, include_families, include_explicit)
        rows.setdefault(k, {})
        # Carry over a single description (first non-empty wins)
        desc = str(r.get("description", "")).strip()
        if desc and not rows[k].get("description"):
            rows[k]["description"] = desc
        if expanded:
            rows[k].update(expanded)

    if not rows:
        return pd.DataFrame(columns=["lu_code", "description"])

    wide = pd.DataFrame.from_dict(rows, orient="index").reset_index().rename(columns={"index": "lu_code"})

    if hsg_count == 4:
        drop_cols = [c for c in wide.columns if c.endswith(("_5", "_6", "_7"))]
        wide = wide.drop(columns=drop_cols)

    fam_cols: List[str] = []
    for fam in include_families:
        prefix = {"cn": "cn_", "rz": "rz_", "max_net_infil": "max_net_infil_"}[fam]
        rng = range(1, 5) if hsg_count == 4 else range(1, 8)
        fam_cols += [f"{prefix}{i}" for i in rng if f"{prefix}{i}" in wide.columns]

    sing_cols = [c for c in include_explicit if c in wide.columns]
    cols = ["lu_code", "description"] + fam_cols + sing_cols

    remaining = [c for c in wide.columns if c not in cols]
    return wide[cols + remaining]