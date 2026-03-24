from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path
import tomllib
from typing import Iterable

REQUIRED_LONG_COLS = [
    "lu_cdl", "lu_nlcd", "group", "column",
    "parlbnd", "parubnd", "parval1",
    "units", "notes", "ref", "condition",
]


def _detect_tab_delimited(path: str | Path) -> None:
    """Fail fast if the file header appears space-delimited instead of tab-delimited."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Long table not found: {p}")
    # Read the first non-empty line
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
            "Please re-save as tab-delimited (TSV) and retry.\n"
        )


def load_selector(path: str | Path) -> dict:
    """Load the TOML selector.

    Returns:
        dict: {
            'primary_key': 'cdl'|'nlcd',
            'include_families': list[str],
            'include_explicit': list[str],
            'hsg_count': int
        }
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
    """Add missing canonical columns; coerce dtypes."""
    df = df.copy()
    for c in REQUIRED_LONG_COLS:
        if c not in df.columns:
            if c in ("parlbnd", "parubnd", "parval1"):
                df[c] = np.nan
            else:
                df[c] = ""
    # Strings & whitespace
    for c in ("lu_cdl", "lu_nlcd", "group", "column", "units", "notes", "ref", "condition"):
        df[c] = df[c].astype(str).str.strip()
    # Numerics
    for c in ("parlbnd", "parubnd", "parval1"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_long_files(paths: Iterable[str | Path]) -> pd.DataFrame:
    """Load and concatenate one or more long TSV files.

    Args:
        paths: iterable of file paths.

    Returns:
        DataFrame with normalized columns and dtypes.
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
        .replace({"max_net_infiltration": "max_net_infil"})  # adopt concise family name everywhere
    )
    # Condition normalization with default
    out["condition"] = (
        out["condition"]
        .str.lower()
        .map({"drained": "drained", "undrained": "undrained"})
        .fillna("drained")
    )
    return out