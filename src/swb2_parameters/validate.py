from __future__ import annotations

import pandas as pd
import numpy as np


def validate_duplicates(df: pd.DataFrame) -> None:
    """Error on duplicate triplets (lu_cdl, lu_nlcd, column)."""
    dup_mask = df.duplicated(subset=["lu_cdl", "lu_nlcd", "column"], keep=False)
    if dup_mask.any():
        dups = df.loc[dup_mask, ["lu_cdl", "lu_nlcd", "column"]].drop_duplicates()
        raise ValueError(
            "Duplicate rows detected for keys:\n"
            f"{dups.to_string(index=False)}\n"
            "Each (lu_cdl, lu_nlcd, column) must be unique."
        )


def _require_bounds(row: pd.Series) -> None:
    """Ensure both bounds are present and numeric; parlbnd <= parubnd."""
    lb = row["parlbnd"]
    ub = row["parubnd"]
    if pd.isna(lb) or pd.isna(ub):
        raise ValueError(f"Missing bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']}).")
    if not np.isfinite(lb) or not np.isfinite(ub):
        raise ValueError(f"Non-numeric bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']}).")
    if lb > ub:
        raise ValueError(f"Lower bound > upper bound for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']}).")


def _compute_parval1(row: pd.Series) -> float:
    """Compute parval1 as mean(lb, ub) if missing; else validate in-range."""
    lb, ub, val = row["parlbnd"], row["parubnd"], row["parval1"]
    if pd.isna(val):
        return (lb + ub) / 2.0
    if val < lb or val > ub:
        raise ValueError(f"parval1 out of bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']}).")
    return val


def compute_and_round(df: pd.DataFrame) -> pd.DataFrame:
    """Compute parval1 where missing and apply family rounding rules.

    - CN -> integer
    - Others -> 2 decimals
    """
    df = df.copy()
    vals = []
    for _, row in df.iterrows():
        _require_bounds(row)
        val = _compute_parval1(row)
        if row["column"] == "cn":
            val = int(round(val))
        else:
            val = round(float(val), 2)
        vals.append(val)
    df["parval1"] = vals
    return df