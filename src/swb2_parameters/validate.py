"""Validation: duplicates, bounds, rounding."""
from __future__ import annotations

import pandas as pd
import numpy as np

FAMILIES = {"cn", "rz", "max_net_infil"}
FAMILY_DECIMALS = {"cn": 0, "rz": 2, "max_net_infil": 2}


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and round parameter values.

    Args:
        df: Materialized long-form DataFrame (code-specific rows only).

    Returns:
        DataFrame with parval1 updated per rounding rules.

    Raises:
        ValueError: On duplicates, bounds violations, or invalid FIXED families.
    """
    df = df.copy()

    # Duplicate check on (lu_code, column)
    dup_cols = ["lu_code", "column"]
    dupes = df[df.duplicated(subset=dup_cols, keep=False)]
    if not dupes.empty:
        examples = dupes[dup_cols].drop_duplicates().head(5).to_string(index=False)
        raise ValueError(f"Duplicate (lu_code, column) entries:\n{examples}")

    # Determine mode per row
    fixed_mask = df["fixed_parval1"].astype(str).str.strip() != ""

    # FIXED mode: families not allowed
    fixed_families = df[fixed_mask & df["column"].isin(FAMILIES)]
    if not fixed_families.empty:
        bad = fixed_families[["lu_code", "column"]].head(3).to_string(index=False)
        raise ValueError(f"Families cannot use FIXED mode:\n{bad}")

    # FIXED mode: use fixed_parval1 as the value
    df.loc[fixed_mask, "parval1"] = df.loc[fixed_mask, "fixed_parval1"]

    # BOUNDED mode
    bounded = ~fixed_mask
    if bounded.any():
        bdf = df.loc[bounded].copy()

        # Coerce parval1 to numeric for bounded rows
        bdf["parval1"] = pd.to_numeric(bdf["parval1"], errors="coerce")

        # Bounds check (only where bounds are provided)
        has_bounds = bdf["parlbnd"].notna() & bdf["parubnd"].notna()
        if has_bounds.any():
            bnd = bdf[has_bounds]
            bad_bounds = bnd[bnd["parlbnd"] > bnd["parubnd"]]
            if not bad_bounds.empty:
                examples = bad_bounds[["lu_code", "column", "parlbnd", "parubnd"]].head(3).to_string(index=False)
                raise ValueError(f"parlbnd > parubnd:\n{examples}")

            out_of_range = bnd[
                bnd["parval1"].notna() &
                ((bnd["parval1"] < bnd["parlbnd"]) | (bnd["parval1"] > bnd["parubnd"]))
            ]
            if not out_of_range.empty:
                examples = out_of_range[["lu_code", "column", "parval1", "parlbnd", "parubnd"]].head(3).to_string(index=False)
                raise ValueError(f"parval1 out of bounds:\n{examples}")

        # Rounding
        for idx, row in bdf.iterrows():
            col_name = row["column"]
            nd = row["num_decimals"]
            val = row["parval1"]

            if pd.isna(val):
                continue

            if col_name in FAMILIES:
                decimals = int(nd) if not pd.isna(nd) else FAMILY_DECIMALS[col_name]
                df.at[idx, "parval1"] = round(val, decimals)
            else:
                # Singleton: round only if num_decimals specified
                if not pd.isna(nd):
                    df.at[idx, "parval1"] = round(val, int(nd))
                else:
                    df.at[idx, "parval1"] = val

    return df
