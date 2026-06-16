"""Group template materialization (ALL / named groups)."""
from __future__ import annotations

import pandas as pd
from swb2_parameters.config import Config


def _normalize_code(val: str) -> str:
    """Normalize code strings: '24.0' -> '24', strip whitespace."""
    val = str(val).strip()
    if not val:
        return ""
    try:
        f = float(val)
        if f.is_integer():
            return str(int(f))
    except ValueError:
        pass
    return val


def materialize_groups(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Expand template rows into per-code rows using group definitions.

    Template rows have lu_code blank and a non-empty 'group'.
    Precedence: ALL -> named group -> code-specific.

    Args:
        df: Long-form DataFrame (loaded and normalized).
        config: Project configuration with groups.

    Returns:
        DataFrame with only code-specific rows (templates removed).
    """
    df = df.copy()
    df["lu_code"] = df["lu_code"].apply(_normalize_code)
    df["group"] = df["group"].str.strip().str.lower()

    # Partition into code-specific vs template rows
    code_rows = df[df["lu_code"] != ""].copy()
    template_rows = df[(df["lu_code"] == "") & (df["group"] != "")].copy()

    if template_rows.empty:
        return code_rows.reset_index(drop=True)

    all_codes = sorted(code_rows["lu_code"].unique())
    if not all_codes:
        return code_rows.reset_index(drop=True)

    # Build code->group mapping from config (normalized codes)
    code_to_group: dict[str, str] = {}
    for grp_name, codes in config.groups.items():
        for code in codes:
            code_to_group[_normalize_code(str(code))] = grp_name.lower()

    # Effective values: (code, column) -> row dict
    effective: dict[tuple[str, str], dict] = {}

    # 1) ALL templates
    all_tmpls = template_rows[template_rows["group"] == "all"]
    for _, t in all_tmpls.iterrows():
        col = t["column"]
        for code in all_codes:
            effective[(code, col)] = t.to_dict()

    # 2) Named group templates
    named_tmpls = template_rows[template_rows["group"] != "all"]
    for _, t in named_tmpls.iterrows():
        grp = t["group"]
        col = t["column"]
        for code in all_codes:
            if code_to_group.get(code) == grp:
                effective[(code, col)] = t.to_dict()

    # 3) Code-specific rows win
    for _, r in code_rows.iterrows():
        effective[(r["lu_code"], r["column"])] = r.to_dict()

    # Rebuild DataFrame
    rows = []
    for (code, col), data in effective.items():
        row = dict(data)
        row["lu_code"] = code
        row["column"] = col
        row["group"] = ""
        rows.append(row)

    result = pd.DataFrame(rows)
    # Preserve original column order
    for col in df.columns:
        if col not in result.columns:
            result[col] = ""
    return result[df.columns.tolist()].reset_index(drop=True)
