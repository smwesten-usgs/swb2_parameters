from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict, List


def _is_blank_or_na(val: str | float | None) -> bool:
    """Return True if value is empty string or literal 'NA' (case-insensitive) or None."""
    if val is None:
        return True
    s = str(val).strip()
    return s == "" or s.lower() == "na"

def _normalize_key_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Normalize the chosen key column to canonical strings for matching.

    Policy:
    - NA → "" (empty).
    - Strip whitespace; map literal 'NA'/'na'/'None'/'none' → "".
    - If numeric:
        * If integral (e.g., '24', '24.0') → '24'.
        * Else (e.g., '24.5') → hard fail (ValueError): non-integer code not allowed.
    - If non-numeric → return trimmed string unchanged (e.g., '24A').
    """
    s = df[col].astype(object).where(~df[col].isna(), "")
    s = s.astype(str).str.strip().replace({
        "NA": "", "na": "", "None": "", "none": ""
    })

    def _canon(v: str) -> str:
        if v == "":
            return ""
        try:
            f = float(v)
            # Treat '24.0' as integer; reject '24.5'
            if f.is_integer():
                return str(int(f))
            # strict policy: fail on non-integer numerics
            raise ValueError(
                f"Non-integer numeric code '{v}' encountered in '{col}'. "
                "Lookup tables must use integer codes."
            )
        except ValueError:
            # Non-numeric or alpha-numeric codes are allowed, return as-is
            return v

    # Apply _canon element-wise and return a Series
    out = s.apply(_canon)

    # (Optional) tiny debug to confirm type + a few values
    print("[_normalize_key_col] type:", type(out), "sample:", out.head(5).tolist())

    return out

def _load_groups(groups_path: str | Path, primary_key: str) -> Dict[str, str]:
    """Load groups.tsv and return a mapping: code -> group for the chosen primary key.

    groups.tsv columns: lu_cdl, lu_nlcd, group

    Args:
        groups_path: path to TSV
        primary_key: 'cdl' or 'nlcd'

    Returns:
        dict mapping code (string) -> group (string)

    Raises:
        ValueError: if a code appears in multiple groups.
    """
    key_col = "lu_cdl" if primary_key == "cdl" else "lu_nlcd"
    gdf = pd.read_csv(groups_path, sep="\t")
    # Normalize key and group
    gdf[key_col] = _normalize_key_col(gdf, key_col)
    gdf["group"] = gdf["group"].astype(str).str.strip()

    # Keep only rows with a non-empty key and non-empty group
    gdf = gdf[(gdf[key_col] != "") & (gdf["group"] != "")].copy()

    # Detect multiple group membership for any code (hard fail)
    dup = (
        gdf.groupby(key_col)["group"]
           .nunique()
           .reset_index(name="group_count")
    )
    bad = dup[dup["group_count"] > 1]
    if not bad.empty:
        codes = ", ".join(bad[key_col].tolist()[:20])
        raise ValueError(
            f"groups.tsv error: codes belong to multiple groups (hard fail). Examples: {codes}"
        )

    # Build mapping code -> group
    # If the same code appears multiple times with the same group, keep one.
    mapping = {}
    for _, row in gdf.iterrows():
        code = row[key_col]
        grp = row["group"]
        mapping[code] = grp
    return mapping


def materialize_group_templates(
    df_long: pd.DataFrame,
    primary_key: str,
    groups_path: str | Path | None = None,
) -> pd.DataFrame:
    """Materialize template rows (group/ALL) into per-code rows prior to validation & rounding.

    Template row definition:
      - lu_cdl and lu_nlcd both blank/NA
      - group is non-empty (e.g., 'ALL' or 'smgrain')
      - column, bounds, value provided (strict bounds enforced later by compute_and_round)

    Overlay rules per column:
      1) ALL applies to all codes present in long under the chosen primary_key.
      2) Named group overwrites ALL baseline for codes in that group.
      3) Code-specific rows (with a non-empty key) win over any template-derived value.

    Hard-fail conditions:
      - groups.tsv contains any code in multiple groups.
      - More than one template row exists for the same (group, column).

    Args:
        df_long: concatenated long DataFrame (already normalized via io.load_long_files).
        primary_key: 'cdl' or 'nlcd' (which domain is being used for codes).
        groups_path: TSV path defining code->group memberships; if None, only ALL templates apply.

    Returns:
        Expanded DataFrame: original code-specific rows plus materialized per-code rows
        from template rows (ALL and named group), with the chosen key column set.

    Notes:
        - Only the chosen key column is set in materialized rows; the other key remains blank.
        - Description/units/notes/ref/drained_condition from the template are carried through.
    """
    key_col = "lu_cdl" if primary_key == "cdl" else "lu_nlcd"

    df = df_long.copy()

    # Normalize chosen key column to canonical strings (Series)
    key_series = _normalize_key_col(df, key_col)
    # Assign back so downstream logic uses canonical values
    df[key_col] = key_series
    
    print("[call-site] type:", type(key_series))
    print("[call-site] first 10:", key_series.head(10).tolist())

    code_rows = df[key_series != ""].copy()
    tmpl_rows = df[(key_series == "") & (df["group"].astype(str).str.strip() != "")].copy()

    # If no templates, return original
    if tmpl_rows.empty:
        return df

    # Detect duplicate templates for same (group, column) -> hard fail
    dups = (
        tmpl_rows.assign(group_norm=tmpl_rows["group"].astype(str).str.strip().str.lower(),
                         column_norm=tmpl_rows["column"].astype(str).str.strip().str.lower())
                 .groupby(["group_norm", "column_norm"])
                 .size()
                 .reset_index(name="count")
    )
    bad = dups[dups["count"] > 1]
    if not bad.empty:
        preview = bad.head(20).to_string(index=False)
        raise ValueError(
            "Template conflict: more than one template row for the same (group, column).\n"
            f"{preview}"
        )

    # Codes in scope (present in long) for the chosen domain
    codes_in_scope = sorted(set(code_rows[key_col].astype(str).str.strip()))

    # Build code->group map if provided
    code_to_group: Dict[str, str] = {}
    if groups_path:
        code_to_group = _load_groups(groups_path, primary_key)

    # Prepare container for materialized rows
    mats: List[pd.DataFrame] = []

    # 1) ALL templates → baseline for all codes
    all_templates = tmpl_rows[tmpl_rows["group"].astype(str).str.strip().str.lower() == "all"]
    if not all_templates.empty:
        for _, t in all_templates.iterrows():
            t_dict = t.to_dict()
            for code in codes_in_scope:
                row = t_dict.copy()
                row[key_col] = code
                # other key stays blank
                row["lu_cdl" if key_col == "lu_nlcd" else "lu_nlcd"] = ""
                mats.append(pd.DataFrame([row]))

    # 2) Named group templates → overwrite baseline for codes in that group
    named_templates = tmpl_rows[tmpl_rows["group"].astype(str).str.strip().str.lower() != "all"]
    if not named_templates.empty:
        for _, t in named_templates.iterrows():
            grp = str(t["group"]).strip()
            t_dict = t.to_dict()
            # Find codes in this group (restrict to codes_in_scope)
            codes_for_group = [c for c, g in code_to_group.items() if g == grp and c in codes_in_scope]
            # If groups.tsv missing this group, skip silently (no members)
            for code in codes_for_group:
                row = t_dict.copy()
                row[key_col] = code
                row["lu_cdl" if key_col == "lu_nlcd" else "lu_nlcd"] = ""
                mats.append(pd.DataFrame([row]))

    # If nothing materialized, return original
    if not mats:
        return df

    materialized = pd.concat(mats, ignore_index=True)

    # Combine: start with ALL+group materialized, then overlay code-specific (code-specific wins)
    # Since validate_duplicates runs later, we must ensure we don't create multiple identical rows.
    #combined = pd.concat([materialized, code_rows], ignore_index=True)
    combined = pd.concat([code_rows, materialized], ignore_index=True)
    # Keep original column order
    combined = combined.reindex(columns=df.columns)

    return combined
