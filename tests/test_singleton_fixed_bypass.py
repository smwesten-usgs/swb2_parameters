import sys
import subprocess
import textwrap
from pathlib import Path
import pandas as pd
import pytest


def test_singleton_fixed_bypass(tmp_path):
    """
    End-to-end CLI test:
    - Selector includes an explicit singleton 'application_scheme'.
    - LONG TSV uses fixed_parval1='pivot' (no bounds).
    - Expect: WIDE contains 'application_scheme' == 'pivot' for lu_code='24'.
    """

    # --- Arrange: selector TOML ---
    selector = tmp_path / "selector.toml"
    selector.write_text(textwrap.dedent("""
        primary_key = "cdl"
        include_families = []
        include_explicit = ["application_scheme"]
        hsg_count = 7
    """).strip(), encoding="utf-8")

    # --- Arrange: LONG TSV (singleton in FIXED mode) ---
    long_tsv = tmp_path / "params_long.tsv"
    df = pd.DataFrame([{
        "lu_cdl": "24",
        "lu_nlcd": "",
        "description": "code24",
        "group": "",
        "column": "application_scheme",
        "parlbnd": "",                # bounds intentionally blank (fixed mode bypasses them)
        "parubnd": "",
        "parval1": "",                # unused in fixed mode
        "fixed_parval1": "pivot",     # the authored fixed value (string)
        "num_decimals": "",           # not used in fixed mode
        "units": "",
        "notes": "",
        "ref": "",
        "drained_condition": "drained",
    }])
    df.to_csv(long_tsv, sep="\t", index=False)

    # --- Act: run CLI to build WIDE ---
    outdir = tmp_path / "out"
    outdir.mkdir()
    outfile = "wide.tsv"

    cmd = [
        sys.executable, "-m", "swb2_parameters.cli",
        "--selector", str(selector),
        "--outdir", str(outdir),
        "--outfile", outfile,
        str(long_tsv),
    ]
    # subprocess.run raises CalledProcessError if the CLI exits non-zero
    subprocess.run(cmd, check=True)

    # --- Assert: value passed through unchanged ---
    wide_path = outdir / outfile
    wide = pd.read_csv(wide_path, sep="\t")

    # We expect exactly one row for lu_code='24'
    rows_24 = wide.loc[wide["lu_code"].astype(str) == "24"]
    assert not rows_24.empty, "Expected lu_code='24' in WIDE output, but found none."

    val = rows_24.iloc[0]["application_scheme"]
    assert val == "pivot", f"Expected application_scheme='pivot', got {val!r}"