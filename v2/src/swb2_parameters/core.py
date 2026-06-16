"""Core pipeline: load long TSVs, materialize groups, expand families, emit wide.

Long-form schema (TSV, minimal required columns):
    lu_code     — land-use code (blank for template rows)
    group       — group name: "ALL", a named group, or blank for code-specific
    parameter   — column name in the wide output (or family name for expansion)
    value       — the parameter value

Optional metadata columns (carried in long, not emitted to wide):
    description, units, notes, ref, parlbnd, parubnd

Family expansion:
    If `parameter` is a registered family name (e.g., "cn") AND value is a single
    number, the aligner expands it to cn_1..cn_7. If the user instead provides
    cn_1, cn_2, ... cn_7 as individual singletons, no expansion occurs.

Group template precedence (per parameter, per lu_code):
    code-specific > named group > ALL
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List


# ---------------------------------------------------------------------------
# Aligners (expand A-soil value to 7 HSG columns)
# ---------------------------------------------------------------------------

def _expand_cn(a: float, drained: bool = True) -> Dict[str, float]:
    b = min(37.8 + 0.622 * a, 100.0)
    c = min(58.9 + 0.411 * a, 100.0)
    d = min(67.2 + 0.328 * a, 100.0)
    if drained:
        ad, bd, cd = a, b, c
    else:
        ad = bd = cd = d
    vals = [a, b, c, d, ad, bd, cd]
    return {f"cn_{i+1}": round(v) for i, v in enumerate(vals)}


def _expand_rz(a: float, factors=(1.25, 1.0, 0.666), drained: bool = True) -> Dict[str, float]:
    b = a * factors[0]
    c = a * factors[1]
    d = a * factors[2]
    if drained:
        ad, bd, cd = a, b, c
    else:
        ad = bd = cd = d
    vals = [a, b, c, d, ad, bd, cd]
    return {f"rz_{i+1}": round(v, 2) for i, v in enumerate(vals)}


def _expand_max_net_infil(a: float, factors=(0.15, 0.06, 0.03), drained: bool = True) -> Dict[str, float]:
    b = a * factors[0]
    c = a * factors[1]
    d = a * factors[2]
    if drained:
        ad, bd, cd = a, b, c
    else:
        ad = bd = cd = d
    vals = [a, b, c, d, ad, bd, cd]
    return {f"max_net_infil_{i+1}": round(v, 2) for i, v in enumerate(vals)}


FAMILY_EXPANDERS = {
    "cn": _expand_cn,
    "rz": _expand_rz,
    "max_net_infil": _expand_max_net_infil,
}


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_long(paths: List[str | Path]) -> pd.DataFrame:
    """Load and concatenate one or more long-form TSVs.

    Validates tab-delimited format. Normalizes column names to lowercase.
    Accepts both v2 schema (lu_code/parameter/value) and v1 schema
    (lu_cdl/column/parval1), mapping v1 names to v2 automatically.
    """
    # Map old column names → new
    COL_ALIASES = {
        "lu_cdl": "lu_code",
        "column": "parameter",
        "parval1": "value",
    }

    frames = []
    for p in paths:
        p = Path(p)
        # Quick delimiter check
        with p.open("r", encoding="utf-8") as f:
            header = f.readline()
        if "\t" not in header and " " in header:
            raise ValueError(f"{p.name} is not tab-delimited.")
        df = pd.read_csv(p, sep="\t", dtype=str).fillna("")
        df.columns = df.columns.str.strip().str.lower()
        # Apply aliases
        df = df.rename(columns=COL_ALIASES)
        # Ensure required columns exist
        for col in ("lu_code", "parameter", "value"):
            if col not in df.columns:
                df[col] = ""
        if "group" not in df.columns:
            df["group"] = ""
        if "description" not in df.columns:
            df["description"] = ""
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No input files provided.")
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Materialize group templates
# ---------------------------------------------------------------------------

def load_groups(path: str | Path) -> Dict[str, str]:
    """Load groups TSV → dict mapping lu_code (str) → group name."""
    df = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    df.columns = df.columns.str.strip().str.lower()
    # Use whichever code column is present
    code_col = "lu_cdl" if "lu_cdl" in df.columns else "lu_code"
    mapping = {}
    for _, row in df.iterrows():
        code = row[code_col].strip()
        grp = row["group"].strip()
        if code and grp:
            mapping[code] = grp
    return mapping


def _normalize_code(val: str) -> str:
    """Normalize code strings: '24.0' → '24', strip whitespace."""
    val = val.strip()
    if not val:
        return ""
    try:
        f = float(val)
        if f.is_integer():
            return str(int(f))
    except ValueError:
        pass
    return val


def materialize(df: pd.DataFrame, groups: Dict[str, str] | None = None) -> pd.DataFrame:
    """Expand template rows (ALL / named group) into per-code rows.

    Returns a DataFrame with only code-specific rows (no templates),
    with group precedence applied: code-specific > named group > ALL.
    """
    df = df.copy()
    df["lu_code"] = df["lu_code"].apply(_normalize_code)
    df["group"] = df["group"].str.strip().str.lower()
    df["parameter"] = df["parameter"].str.strip().str.lower()

    # Partition
    code_rows = df[df["lu_code"] != ""].copy()
    template_rows = df[(df["lu_code"] == "") & (df["group"] != "")].copy()

    if template_rows.empty:
        return code_rows

    # All codes present in the data
    all_codes = sorted(code_rows["lu_code"].unique())
    if not all_codes:
        return code_rows

    groups = groups or {}

    # Build effective values: dict[(code, parameter)] → row data
    # Start with ALL templates, overlay named groups, then code-specific
    effective: Dict[tuple, dict] = {}

    # 1) ALL templates
    all_tmpls = template_rows[template_rows["group"] == "all"]
    for _, t in all_tmpls.iterrows():
        param = t["parameter"]
        for code in all_codes:
            effective[(code, param)] = t.to_dict()

    # 2) Named group templates
    named_tmpls = template_rows[template_rows["group"] != "all"]
    for _, t in named_tmpls.iterrows():
        grp = t["group"]
        param = t["parameter"]
        codes_in_group = [c for c, g in groups.items() if g.lower() == grp]
        for code in codes_in_group:
            if code in all_codes:  # only materialize for codes present in data
                effective[(code, param)] = t.to_dict()

    # 3) Code-specific rows win
    for _, r in code_rows.iterrows():
        effective[(r["lu_code"], r["parameter"])] = r.to_dict()

    # Rebuild DataFrame
    rows = []
    for (code, param), data in effective.items():
        row = dict(data)
        row["lu_code"] = code
        row["parameter"] = param
        row["group"] = ""
        rows.append(row)

    result = pd.DataFrame(rows)
    # Keep only columns from original
    for col in df.columns:
        if col not in result.columns:
            result[col] = ""
    return result[df.columns.tolist()].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Build wide table
# ---------------------------------------------------------------------------

def build_wide(df: pd.DataFrame, families: List[str] | None = None) -> pd.DataFrame:
    """Convert materialized long-form to wide (one row per lu_code).

    Args:
        df: Materialized long-form DataFrame (code-specific rows only).
        families: List of family names to expand via aligners.
                  If None, defaults to ["cn", "rz", "max_net_infil"].
                  Pass [] to disable all expansion.
    """
    if families is None:
        families = list(FAMILY_EXPANDERS.keys())

    wide: Dict[str, Dict[str, object]] = {}

    for _, row in df.iterrows():
        code = row["lu_code"]
        param = row["parameter"]
        value = row["value"].strip() if isinstance(row["value"], str) else row["value"]

        if code not in wide:
            wide[code] = {"lu_code": code}

        # Carry description — always overwrite (code-specific rows processed after templates)
        if "description" in row.index:
            desc = row["description"].strip() if isinstance(row["description"], str) else ""
            if desc:
                wide[code]["description"] = desc

        # Try family expansion
        if param in families and param in FAMILY_EXPANDERS:
            try:
                a_val = float(value)
                expanded = FAMILY_EXPANDERS[param](a_val)
                wide[code].update(expanded)
                continue
            except (ValueError, TypeError):
                pass  # Not a number — treat as singleton

        # Singleton: try numeric conversion, fall back to string
        try:
            value = float(value)
            if value == int(value) and "." not in str(row["value"]):
                value = int(value)
        except (ValueError, TypeError):
            pass

        wide[code][param] = value

    if not wide:
        return pd.DataFrame(columns=["lu_code", "description"])

    result = pd.DataFrame(wide.values())

    # Order columns: lu_code, description first, then sorted parameter columns
    lead = ["lu_code", "description"]
    lead = [c for c in lead if c in result.columns]
    rest = sorted([c for c in result.columns if c not in lead])
    result = result[lead + rest]

    # Sort by lu_code (numeric if possible)
    try:
        result["_sort"] = result["lu_code"].astype(float)
        result = result.sort_values("_sort").drop(columns="_sort")
    except (ValueError, TypeError):
        result = result.sort_values("lu_code")

    return result.reset_index(drop=True)
