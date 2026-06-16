"""I/O: load and normalize long-form TSV files."""
from __future__ import annotations

import pandas as pd
from pathlib import Path

LONG_COLUMNS = [
    "lu_code", "description", "group", "column",
    "parlbnd", "parubnd", "parval1", "units", "notes", "ref",
    "drained_condition", "fixed_parval1", "num_decimals",
]

FAMILY_ALIASES = {"max_net_infiltration": "max_net_infil"}


def load_long(paths: list[str | Path]) -> pd.DataFrame:
    """Load and concatenate one or more long-form TSV files.

    Args:
        paths: Paths to tab-delimited long-form parameter files.

    Returns:
        Combined DataFrame with normalized columns.

    Raises:
        ValueError: If a file is not tab-delimited.
        FileNotFoundError: If no paths provided.
    """
    if not paths:
        raise FileNotFoundError("No input files provided.")

    frames = []
    for p in paths:
        p = Path(p)
        # Delimiter guard
        with p.open("r", encoding="utf-8") as f:
            header = f.readline()
        if "\t" not in header and " " in header:
            raise ValueError(f"{p.name} is not tab-delimited.")

        df = pd.read_csv(p, sep="\t", dtype=str).fillna("")
        df.columns = df.columns.str.strip().str.lower()
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    # Ensure all expected columns exist
    for col in LONG_COLUMNS:
        if col not in combined.columns:
            combined[col] = ""

    # Normalize family names in 'column' field
    combined["column"] = combined["column"].str.strip().str.lower()
    combined["column"] = combined["column"].replace(FAMILY_ALIASES)

    # Normalize drained_condition
    combined["drained_condition"] = (
        combined["drained_condition"].str.strip().str.lower()
    )
    combined.loc[combined["drained_condition"] == "", "drained_condition"] = "drained"

    # Coerce bounds to numeric
    combined["parlbnd"] = pd.to_numeric(combined["parlbnd"], errors="coerce")
    combined["parubnd"] = pd.to_numeric(combined["parubnd"], errors="coerce")

    # Normalize num_decimals to numeric (int or NaN)
    combined["num_decimals"] = pd.to_numeric(
        combined["num_decimals"], errors="coerce"
    )

    return combined
