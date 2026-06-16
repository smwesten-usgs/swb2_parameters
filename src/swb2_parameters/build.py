"""Build wide tables: expand families via aligners, assemble per TOML [tables.*]."""
from __future__ import annotations

import pandas as pd
from swb2_parameters.config import Config


def _expand_cn(a: float, hsg_count: int, drained: bool = True) -> dict[str, float]:
    """Expand A-soil CN to HSG indices."""
    b = min(37.8 + 0.622 * a, 100.0)
    c = min(58.9 + 0.411 * a, 100.0)
    d = min(67.2 + 0.328 * a, 100.0)
    if drained:
        ad, bd, cd = a, b, c
    else:
        ad = bd = cd = d
    vals = [a, b, c, d, ad, bd, cd][:hsg_count]
    return {f"cn_{i+1}": round(v) for i, v in enumerate(vals)}


def _expand_rz(a: float, hsg_count: int, drained: bool = True) -> dict[str, float]:
    """Expand A-soil root-zone depth to HSG indices."""
    b = a * 1.25
    c = a * 1.0
    d = a * 0.666
    if drained:
        ad, bd, cd = a, b, c
    else:
        ad = bd = cd = d
    vals = [a, b, c, d, ad, bd, cd][:hsg_count]
    return {f"rz_{i+1}": round(v, 2) for i, v in enumerate(vals)}


def _expand_max_net_infil(a: float, hsg_count: int, drained: bool = True) -> dict[str, float]:
    """Expand A-soil max net infiltration to HSG indices."""
    b = a * 0.15
    c = a * 0.06
    d = a * 0.03
    if drained:
        ad, bd, cd = a, b, c
    else:
        ad = bd = cd = d
    vals = [a, b, c, d, ad, bd, cd][:hsg_count]
    return {f"max_net_infil_{i+1}": round(v, 2) for i, v in enumerate(vals)}


_EXPANDERS = {
    "cn": _expand_cn,
    "rz": _expand_rz,
    "max_net_infil": _expand_max_net_infil,
}


def build_tables(df: pd.DataFrame, config: Config) -> dict[str, pd.DataFrame]:
    """Build wide tables from validated long-form data per TOML table definitions.

    Args:
        df: Validated, materialized long-form DataFrame.
        config: Project configuration.

    Returns:
        Dict mapping table name to wide DataFrame.
    """
    hsg_count = config.hsg_count

    # Filter to rows that have a value in lu_code
    df = df[df["lu_code"].astype(str).str.strip() != ""].copy()

    results = {}
    for table_name, spec in config.tables.items():
        wide: dict[str, dict[str, object]] = {}

        for _, row in df.iterrows():
            code = str(row["lu_code"]).strip()
            col_name = str(row["column"]).strip()
            drained = str(row.get("drained_condition", "drained")).strip() == "drained"

            # Check if this column belongs to this table
            is_family = col_name in spec.families
            is_singleton = col_name in spec.singletons
            if not is_family and not is_singleton:
                continue

            if code not in wide:
                wide[code] = {"lu_code": code}

            # Carry description (always overwrite — code-specific rows override templates)
            desc = str(row.get("description", "")).strip()
            if desc:
                wide[code]["description"] = desc

            val = row["parval1"]

            if is_family:
                try:
                    a_val = float(val)
                except (ValueError, TypeError):
                    continue
                expander = _EXPANDERS[col_name]
                expanded = expander(a_val, hsg_count, drained)
                wide[code].update(expanded)
            else:
                # Singleton: try numeric, fall back to string
                try:
                    num = float(val)
                    if num == int(num) and "." not in str(val):
                        num = int(num)
                    wide[code][col_name] = num
                except (ValueError, TypeError):
                    wide[code][col_name] = val

        if not wide:
            results[table_name] = pd.DataFrame(columns=["lu_code", "description"])
            continue

        result = pd.DataFrame(wide.values())

        # Column ordering: lu_code, description, families (sorted), singletons (as declared)
        lead = ["lu_code", "description"]
        lead = [c for c in lead if c in result.columns]

        fam_cols = []
        for fam in spec.families:
            for i in range(1, hsg_count + 1):
                col = f"{fam}_{i}"
                if col in result.columns:
                    fam_cols.append(col)

        sing_cols = [c for c in spec.singletons if c in result.columns]
        ordered = lead + fam_cols + sing_cols
        rest = [c for c in result.columns if c not in ordered]
        result = result[ordered + rest]

        # Sort by lu_code (numeric if possible)
        try:
            result["_sort"] = result["lu_code"].astype(float)
            result = result.sort_values("_sort").drop(columns="_sort")
        except (ValueError, TypeError):
            result = result.sort_values("lu_code")

        results[table_name] = result.reset_index(drop=True)

    return results
