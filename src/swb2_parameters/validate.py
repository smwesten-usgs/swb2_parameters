from __future__ import annotations

import pandas as pd
import numpy as np


def validate_duplicates(df: pd.DataFrame) -> None:
    """Error on duplicate triplets `(lu_cdl, lu_nlcd, column)`.

    Canonicalize keys (lowercase, trimmed, NaNs → '') and report any
    combinations appearing more than once across all provided long tables.

    Args:
        df: Normalized long-table DataFrame.

    Raises:
        ValueError: If any duplicate `(lu_cdl, lu_nlcd, column)` combinations exist.
    """
    keys = df.copy()
    for c in ("lu_cdl", "lu_nlcd", "column"):
        keys[c] = keys[c].astype(str).str.strip().str.lower()
        keys[c] = keys[c].replace({"nan": ""})

    dup_keys = (
        keys.groupby(["lu_cdl", "lu_nlcd", "column"], dropna=False)
            .size()
            .reset_index(name="count")
    )
    dup_keys = dup_keys[dup_keys["count"] > 1]

    if not dup_keys.empty:
        preview = dup_keys.head(20).to_string(index=False)
        raise ValueError(
            "Duplicate (lu_cdl, lu_nlcd, column) combinations detected.\n"
            f"{preview}\n"
            "Each (lu_cdl, lu_nlcd, column) must be unique across all provided long tables."
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
    """Compute `parval1` as mean(lb, ub) if missing; else validate in-range."""
    lb, ub, val = row["parlbnd"], row["parubnd"], row["parval1"]
    if pd.isna(val):
        return (lb + ub) / 2.0
    if val < lb or val > ub:
        raise ValueError(f"parval1 out of bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']}).")
    return val


def compute_and_round(df: pd.DataFrame) -> pd.DataFrame:
    """Compute `parval1` (A value) and apply rounding policy per family.

    Policy:
      - Require both bounds (`parlbnd`, `parubnd`) and `parlbnd <= parubnd`.
      - If `parval1` missing, compute mean(lb, ub).
      - Validate `parval1 ∈ [lb, ub]`.
      - Round: `cn` → integer; others → 2 decimals.

    Args:
        df: Normalized long-table DataFrame.

    Returns:
        A copy with `parval1` computed/rounded.

    Raises:
        ValueError: On missing/non-numeric/ill-ordered bounds, or out-of-bound `parval1`.
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