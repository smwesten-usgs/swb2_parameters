"""CLI entry point: swb2-parameters (subcommand-based)."""
from __future__ import annotations

import argparse
from pathlib import Path

from swb2_parameters.config import load_config
from swb2_parameters.io import load_long
from swb2_parameters.groups import materialize_groups
from swb2_parameters.validate import validate
from swb2_parameters.build import build_tables


def cmd_build(args: argparse.Namespace) -> int:
    """Execute the build (long -> wide) pipeline."""
    config = load_config(args.config)
    df = load_long(args.inputs)
    df = materialize_groups(df, config)
    df = validate(df)
    tables = build_tables(df, config)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for name, wide in tables.items():
        out_path = outdir / f"{name}.tsv"
        wide.to_csv(out_path, sep="\t", index=False)
        print(f"Wrote {out_path} ({len(wide)} rows x {len(wide.columns)} cols)")

    return 0


def cmd_reverse(args: argparse.Namespace) -> int:
    """Execute the reverse (wide -> long) pipeline."""
    print("reverse: not yet implemented")
    return 0


def cmd_pest_export(args: argparse.Namespace) -> int:
    """Execute the PEST++ export pipeline."""
    print("pest-export: not yet implemented")
    return 0


def main() -> int:
    """Main entry point with subcommands."""
    ap = argparse.ArgumentParser(
        prog="swb2-parameters",
        description="TOML-driven parameter table builder for SWB2.",
    )
    sub = ap.add_subparsers(dest="command", required=True)

    # build
    p_build = sub.add_parser("build", help="Build wide tables from long-form TSVs.")
    p_build.add_argument("inputs", nargs="+", help="Long-form TSV file(s).")
    p_build.add_argument("--config", required=True, help="Project TOML config.")
    p_build.add_argument("--outdir", default=".", help="Output directory.")
    p_build.set_defaults(func=cmd_build)

    # reverse
    p_rev = sub.add_parser("reverse", help="Generate long-form baseline from wide tables.")
    p_rev.add_argument("inputs", nargs="+", help="Wide TSV file(s).")
    p_rev.add_argument("--config", required=True, help="Project TOML config.")
    p_rev.add_argument("--outdir", default=".", help="Output directory.")
    p_rev.set_defaults(func=cmd_reverse)

    # pest-export
    p_pest = sub.add_parser("pest-export", help="Generate PEST++ template files.")
    p_pest.add_argument("--config", required=True, help="Project TOML config.")
    p_pest.add_argument("--outdir", default=".", help="Output directory.")
    p_pest.set_defaults(func=cmd_pest_export)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
