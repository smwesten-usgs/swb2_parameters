# src/swb2_parameters/validate.py
"""
Validation and rounding logic for LONG tables.

Implements the agreed authoring model:
- Each row is either BOUNDED (uses parlbnd/parubnd/parval1) or FIXED (uses fixed_parval1).
- Families (cn, rz, max_net_infil) in FIXED mode are NOT supported and will fail.
- For BOUNDED rows, optional `num_decimals` controls rounding:
    * Families: if num_decimals provided -> use it; else defaults (cn=0; rz/max_net_infil=2).
    * Singletons: if num_decimals provided -> use it; else no rounding.
- For FIXED singletons, bounds are bypassed and the authored value is passed through unchanged.
- After validation/rounding, the effective value is written back into `parval1`
  (downstream expansion/wide assembly reads from `parval1`).
"""

from __future__ import annotations
import pandas as pd
import numpy as np


# ------------------------------------------------------------------------------
# Duplicate detection
# ------------------------------------------------------------------------------
def validate_duplicates(df: pd.DataFrame) -> None:
    """Fail if duplicates exist in (lu_cdl, lu_nlcd, column), case-insensitive.

    Canonicalizes keys to lowercase, trims whitespace, and treats NaN as empty string.
    Raises a ValueError with a short preview if any duplicate triplets are found.
    """
    keys = df.copy()
    for c in ("lu_cdl", "lu_nlcd", "column"):
        keys[c] = keys[c].astype(str).str.strip().str.lower()
        keys[c] = keys[c].replace({"nan": ""})
    dup = (
        keys.groupby(["lu_cdl", "lu_nlcd", "column"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    dup = dup[dup["count"] > 1]
    if not dup.empty:
        preview = dup.head(20).to_string(index=False)
        raise ValueError(
            "Duplicate (lu_cdl, lu_nlcd, column) combinations detected.\n"
            f"{preview}\n"
            "Each (lu_cdl, lu_nlcd, column) must be unique across all provided long tables."
        )


# ------------------------------------------------------------------------------
# Bounds helpers (used in BOUNDED mode for families and singletons)
# ------------------------------------------------------------------------------
def _require_bounds(row: pd.Series) -> None:
    """Ensure both bounds are present and numeric; parlbnd <= parubnd."""
    lb = row["parlbnd"]
    ub = row["parubnd"]
    if pd.isna(lb) or pd.isna(ub):
        raise ValueError(
            f"Missing bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']})."
        )
    if not np.isfinite(lb) or not np.isfinite(ub):
        raise ValueError(
            f"Non-numeric bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']})."
        )
    if lb > ub:
        raise ValueError(
            f"Lower bound > upper bound for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']})."
        )


def _compute_parval1(row: pd.Series) -> float:
    """Compute `parval1` as mean(lb, ub) if missing; else validate in-range and numeric."""
    lb, ub, val = row["parlbnd"], row["parubnd"], row["parval1"]
    if val in ("", None) or pd.isna(val):
        return (lb + ub) / 2.0
    try:
        f = float(val)
    except (TypeError, ValueError):
        raise ValueError(
            f"Non-numeric parval1 for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']})."
        )
    if f < lb or f > ub:
        raise ValueError(
            f"parval1 out of bounds for {row['column']} @ ({row['lu_cdl']}, {row['lu_nlcd']})."
        )
    return f


# ------------------------------------------------------------------------------
# Core: compute_and_round (bounded vs fixed modes)
# ------------------------------------------------------------------------------
def compute_and_round(df: pd.DataFrame) -> pd.DataFrame:
    """Compute/validate `parval1` per row and apply rounding policy.

    Modes:
        FIXED mode (fixed_parval1 non-empty):
            - Families ('cn','rz','max_net_infil'): NOT supported -> raise ValueError.
            - Singletons (any type): bypass bounds; pass through the authored fixed value unchanged.
        BOUNDED mode (else):
            - Require bounds for both families and singletons; compute/validate `parval1` numeric and in-range.
            - Apply `num_decimals` if provided (integer >= 0):
                * Families: use provided nd; else defaults (cn -> 0; rz/max_net_infil -> 2).
                * Singletons: use provided nd; else no rounding.

    After processing, the effective value is written back into `parval1` (int/float/str as applicable).
    """
    df = df.copy()
    out_vals = []
    families = {"cn", "rz", "max_net_infil"}

    for _, row in df.iterrows():
        col = str(row["column"]).strip().lower()
        fixed = str(row.get("fixed_parval1", "")).strip()
        nd = row.get("num_decimals", "")

        # -----------------------
        # FIXED mode
        # -----------------------
        if fixed != "":
            if col in families:
                raise ValueError(
                    f"fixed_parval1 not supported for family '{col}' @ "
                    f"({row['lu_cdl']}, {row['lu_nlcd']})."
                )
            # Singletons: pass through authored fixed value (string/float/int) unchanged.
            out_vals.append(fixed)
            continue

        # -----------------------
        # BOUNDED mode
        # -----------------------
        _require_bounds(row)
        eff = _compute_parval1(row)  # numeric effective value from bounds

        # parse num_decimals if provided
        if  not np.isnan(nd):
            try:
                if float(nd).is_integer():
                    nd = int(nd)
                    if nd < 0:
                        raise ValueError
                else:
                    raise ValueError(
                        f"num_decimals must be an integer."
                    )    
            except Exception:
                raise ValueError(
                    f"num_decimals must be integer >= 0 @ "
                    f"({row['lu_cdl']}, {row['lu_nlcd']})."
                )
        else:
            nd = None

        # Families: apply defaults unless overridden by num_decimals
        if col == "cn":
            d = 0 if nd is None else nd
            # CN integer when d==0; allow d>0 if author explicitly requests decimals
            #val = round(float(eff), d)
            val = int(round(eff)) if d == 0 else round(float(eff), d)
            out_vals.append(val)
        elif col in {"rz", "max_net_infil"}:
            d = 2 if nd is None else nd
            out_vals.append(round(float(eff), d))
        else:
            # Singleton (bounded): round only if num_decimals provided; else preserve numeric value
            if nd is None:
                out_vals.append(eff)
            else:
                fmt_str = f"{eff:.{nd}f}"
                out_vals.append(fmt_str)
                #out_vals.append(round(float(eff), nd))

    df["parval1"] = out_vals
    return df