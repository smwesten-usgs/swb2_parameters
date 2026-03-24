from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from swb2_parameters.io import load_long_files, load_selector
from swb2_parameters.validate import validate_duplicates, compute_and_round
from swb2_parameters.build_wide import build_wide


def main() -> int:
    """CLI entry point for long→wide builder."""
    ap = argparse.ArgumentParser(
        description="SWB2 long→wide parameter builder (CDL/NLCD, families, singletons).",
        add_help=True,
    )
    # Only define options; treat all non-option args as TSVs
    ap.add_argument("--selector", required=True, help="Path to TOML selector.")
    ap.add_argument("--outdir", default=".", help="Output directory (default current).")
    ap.add_argument("--outfile", default="params_wide.tsv", help="Output filename (TSV).")
    ap.add_argument("--condition", choices=["drained", "undrained"], help="Filter rows by condition (optional).")

    # Parse known options; everything else is assumed to be TSV paths
    args, tsv_paths = ap.parse_known_args()

    if not tsv_paths:
        ap.error("Provide one or more TSV long tables as positional arguments.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load selector
    sel = load_selector(args.selector)
    primary_key = sel["primary_key"]
    include_families = sel["include_families"]
    include_explicit = sel["include_explicit"]
    hsg_count = int(sel.get("hsg_count", 7))

    # Load & normalize long table(s) from positional args
    df_long = load_long_files(tsv_paths)

    # Validate duplicates
    validate_duplicates(df_long)

    # Compute parval1 & rounding
    df_long = compute_and_round(df_long)

    # Build wide
    wide = build_wide(
        df_long=df_long,
        primary_key=primary_key,
        include_families=include_families,
        include_explicit=include_explicit,
        hsg_count=hsg_count,
        condition_filter=args.condition,
    )

    # Write
    out_path = outdir / args.outfile
    wide.to_csv(out_path, sep="\t", index=False)
    print(f"Wrote: {out_path}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
