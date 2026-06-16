"""CLI entry point: swb2-parameters."""
from __future__ import annotations

import argparse
from pathlib import Path

from swb2_parameters.core import load_long, load_groups, materialize, build_wide


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build SWB2 wide lookup table(s) from long-form TSV(s).",
    )
    ap.add_argument("inputs", nargs="+", help="One or more long-form TSV files.")
    ap.add_argument("--groups", help="Groups TSV (lu_code → group mapping).")
    ap.add_argument("--families", nargs="*", default=None,
                    help="Family names to expand (default: cn rz max_net_infil). Pass none to disable.")
    ap.add_argument("--outfile", default="params_wide.tsv", help="Output filename.")
    ap.add_argument("--outdir", default=".", help="Output directory.")
    args = ap.parse_args()

    # Load
    df = load_long(args.inputs)

    # Groups
    groups = load_groups(args.groups) if args.groups else None

    # Materialize templates
    df = materialize(df, groups)

    # Families: None means use defaults, [] means no expansion
    families = args.families  # already a list or None

    # Build wide
    wide = build_wide(df, families=families)

    # Write
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / args.outfile
    wide.to_csv(out_path, sep="\t", index=False)
    print(f"Wrote {out_path} ({len(wide)} rows × {len(wide.columns)} cols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
