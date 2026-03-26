from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from swb2_parameters.io import load_long_files, load_selector
from swb2_parameters.validate import validate_duplicates, compute_and_round
from swb2_parameters.build_wide import build_wide
from swb2_parameters.reverse import make_long_from_wide
from swb2_parameters.group_templates import materialize_group_templates


def main() -> int:
    """CLI entry point for long↔wide transformations.

    Modes:
      • Default (forward): positional args are *long* TSVs → build *wide* TSV (with `lu_code`).
      • Reverse (`--to-long`): positional args are *wide* TSVs → emit baseline *long* TSV.

    Behavior:
      • Anything on the command line that isn’t an option is treated as a TSV path.
      • `--selector` and `--outdir` are parsed as normal.
      • Forward output’s leading key column is `lu_code` (sourced from `lu_cdl` or `lu_nlcd` per selector).
      • Reverse output includes `lu_code` and leaves `lu_cdl`/`lu_nlcd` empty.

    Returns:
      Process exit status (0 on success).
    """
    ap = argparse.ArgumentParser(
        description="SWB2 parameter builder: long→wide (default) or wide→long (--to-long).",
        add_help=True,
    )
    ap.add_argument("--selector", required=True, help="Path to TOML selector.")
    ap.add_argument("--outdir", default=".", help="Output directory (default current).")
    ap.add_argument("--outfile", default="params_wide.tsv", help="Output filename (TSV).")
    ap.add_argument("--groups", help="Path to groups.tsv (columns: lu_cdl, lu_nlcd, group). Optional.")
    ap.add_argument(
        "--drained-condition",
        dest="drained_condition",
        choices=["drained", "undrained"],
        help="Filter rows by drained condition (optional; forward mode only).",
    )
    ap.add_argument(
        "--to-long",
        action="store_true",
        help="Reverse: positional inputs are wide TSVs; emit a baseline long TSV.",
    )

    # Parse known options; everything else is assumed to be TSV paths
    args, tsv_paths = ap.parse_known_args()
    if not tsv_paths:
        ap.error("Provide one or more TSV files as positional arguments.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load selector
    sel = load_selector(args.selector)
    primary_key = sel["primary_key"]
    include_families = sel["include_families"]
    include_explicit = sel["include_explicit"]
    hsg_count = int(sel.get("hsg_count", 7))

    if not args.to_long:
        # ---------- Forward: long → wide ----------
        df_long = load_long_files(tsv_paths)
        
        df_long = materialize_group_templates(
            df_long=df_long,
            primary_key=primary_key,
            groups_path=args.groups,  # may be None → only ALL applies
        )

        validate_duplicates(df_long)
        df_long = compute_and_round(df_long)

        wide = build_wide(
            df_long=df_long,
            primary_key=primary_key,
            include_families=include_families,
            include_explicit=include_explicit,
            hsg_count=hsg_count,
            drained_condition_filter=args.drained_condition,
        )

        out_path = outdir / args.outfile
        wide.to_csv(out_path, sep="\t", index=False)
        print(f"Wrote: {out_path}")

    else:
        # ---------- Reverse: wide → long ----------
        frames = [pd.read_csv(p, sep="\t") for p in tsv_paths]
        df_wide = pd.concat(frames, ignore_index=True)

        long_df = make_long_from_wide(
            df_wide=df_wide,
            include_families=include_families,
            include_explicit=include_explicit,
        )

        out_name = (
            "params_long_from_wide.tsv"
            if args.outfile == "params_wide.tsv"
            else args.outfile
        )
        out_path = outdir / out_name
        long_df.to_csv(out_path, sep="\t", index=False)
        print(f"Wrote: {out_path}")

    return 0

if __name__ == "__main__":
    main()