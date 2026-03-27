# src/swb2_parameters/io.py
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import tomllib
from typing import Iterable

# ----------------------------------------------------------------------
# Canonical LONG schema (UPDATED)
# - Adds `fixed_parval1` and `num_decimals`
# - We intentionally DO NOT globally coerce `parval1` to numeric here;
#   it is handled per-row in validate.compute_and_round()
# ----------------------------------------------------------------------
REQUIRED_LONG_COLS = [
    "lu_cdl", "lu_nlcd", "description", "group", "column",
    "parlbnd", "parubnd", "parval1",
    "units", "notes", "ref", "drained_condition",
    # NEW:
    "fixed_parval1", "num_decimals",
]


def load_selector(path: str | Path) -> dict:
    """Load and validate the run-selector TOML.

    The selector governs forward/reverse transformations and includes:
      - `primary_key`: which long key to source the wide `lu_code` from ("cdl" or "nlcd").
      - `include_families`: families to expand (`cn`, `rz`, `max_net_infil`).
      - `include_explicit`: singleton columns to pass through unchanged.
      - `hsg_count`: number of HSG entries per family (usually 7).

    Args:
        path: Path to the selector TOML file.

    Returns:
        A dict with normalized keys and defaults applied.

    Raises:
        ValueError: If `primary_key` is not "cdl" or "nlcd".
    """
    with open(path, "rb") as f:
        sel = tomllib.load(f)
    sel.setdefault("primary_key", "cdl")
    sel.setdefault("include_families", ["cn", "rz", "max_net_infil"])
    sel.setdefault("include_explicit", [])
    sel.setdefault("hsg_count", 7)
    pk = sel["primary_key"]
    if pk not in ("cdl", "nlcd"):
        raise ValueError("selector.primary_key must be 'cdl' or 'nlcd'")
    return sel


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a long-table frame to the canonical schema and dtypes.

    Changes/Policy:
    - Ensures all required columns exist.
    - Trims whitespace and converts missing string values to empty strings.
    - Coerces numeric columns (`parlbnd`, `parubnd`) to float.
    - IMPORTANT: `parval1` is *not* globally coerced here; it remains object (string/float/int)
      so that the validator can apply per-row logic for bounded vs fixed mode and rounding
      (including `num_decimals`).

    Args:
        df: Raw long-table DataFrame.

    Returns:
        A copy of `df` with canonical columns and normalized types.
    """
    df = df.copy()

    # Ensure required columns exist
    for c in REQUIRED_LONG_COLS:
        if c not in df.columns:
            if c in ("parlbnd", "parubnd"):
                df[c] = np.nan
            else:
                df[c] = ""

    # Normalize missing -> empty strings for string-like columns
    str_cols = [
        "lu_cdl", "lu_nlcd", "description", "group", "column",
        "units", "notes", "ref", "drained_condition",
        "parval1", "fixed_parval1", "num_decimals",
    ]
    for c in str_cols:
        df[c] = df[c].astype(object).where(df[c].notna(), "").astype(str).str.strip()

    # Coerce ONLY bounds to numeric globally
    for c in ("parlbnd", "parubnd", "num_decimals"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def _detect_tab_delimited(path: str | Path) -> None:
    """Fail fast if the input is not truly tab-delimited (TSV).

    Args:
        path: Path to the file being validated.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or appears space-delimited.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Long table not found: {p}")
    with p.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            header = line.strip()
            if header:
                break
        else:
            raise ValueError(f"Long table is empty: {p}")
    if "\t" not in header and " " in header:
        raise ValueError(
            f"File '{p.name}' appears to be space-delimited, not tab-delimited.\n"
            "Please re-save as tab-delimited (TSV) and retry."
        )


def load_long_files(paths: Iterable[str | Path]) -> pd.DataFrame:
    """Load and concatenate one or more human-edited *long* TSVs.

    Also normalizes family names:
      - `max_net_infiltration` → `max_net_infil` (preferred concise form),
    and normalizes `drained_condition` to {'drained','undrained'} with default 'drained'.

    Args:
        paths: Iterable of file paths to long-table TSVs.

    Returns:
        A single DataFrame with canonical schema and normalized values.

    Raises:
        FileNotFoundError: If `paths` is empty.
        ValueError: If any file is not tab-delimited (TSV).
    """
    frames = []
    for p in paths:
        _detect_tab_delimited(p)
        df = pd.read_csv(p, sep="\t")
        frames.append(_ensure_columns(df))
    if not frames:
        raise FileNotFoundError("No long tables provided.")
    out = pd.concat(frames, ignore_index=True)

    # Family name normalization
    out["column"] = (
        out["column"]
        .str.lower()
        .replace({"max_net_infiltration": "max_net_infil"})
    )

    # Normalize drained condition
    out["drained_condition"] = (
        out["drained_condition"]
        .str.lower()
        .map({"drained": "drained", "undrained": "undrained"})
        .fillna("drained")
    )

    return out