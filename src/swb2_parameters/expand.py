from __future__ import annotations

from typing import Dict, List, Union
import pandas as pd

from swb2_parameters.aligners.curve_number_aligner import curve_number_aligner
from swb2_parameters.aligners.root_zone_aligner import root_zone_aligner
from swb2_parameters.aligners.net_infiltration_aligner import net_infiltration_aligner

SOIL_ORDER = ["a", "b", "c", "d", "ad", "bd", "cd"]
INDEX_MAP = {k: i + 1 for i, k in enumerate(SOIL_ORDER)}

SingletonValue = Union[int, float]


def _expand_cn(a_value: float, drained_condition: str) -> Dict[str, int]:
    """Expand CN A-value to `cn_1..cn_7` (integers)."""
    d = curve_number_aligner(a_value, condition=drained_condition)
    return {f"cn_{INDEX_MAP[k]}": int(round(d[f"cn_{k}"])) for k in SOIL_ORDER}


def _expand_rz(a_value: float, drained_condition: str) -> Dict[str, float]:
    """Expand root-zone A-value to `rz_1..rz_7` (2 decimals)."""
    d = root_zone_aligner(a_value, condition=drained_condition)
    return {f"rz_{INDEX_MAP[k]}": float(d[f"rz_{k}"]) for k in SOIL_ORDER}


def _expand_max_net_infil(a_value: float, drained_condition: str) -> Dict[str, float]:
    """Expand net infiltration A-value to `max_net_infil_1.._7` (2 decimals)."""
    d = net_infiltration_aligner(a_value, dict_prefix="max_net_infil_", condition=drained_condition)
    return {f"max_net_infil_{INDEX_MAP[k]}": float(d[f"max_net_infil_{k}"]) for k in SOIL_ORDER}


def expand_row(row: pd.Series, families: List[str], singletons: List[str]) -> Dict[str, SingletonValue]:
    """Expand a single long-table row to its wide columns.

    Families (`cn`, `rz`, `max_net_infil`) are expanded to HSG indices 1..7.
    Singletons are passed through unchanged (no HSG expansion).

    Args:
        row: Long-table row with at least `column`, `parval1`, `drained_condition`.
        families: Family names to expand (subset of {'cn','rz','max_net_infil'}).
        singletons: Singleton column names to pass through (e.g., 'kcb_mid').

    Returns:
        A dict of wide columns for this row (may be empty if `column` not requested).
    """
    fam = row["column"]
    val = row["parval1"]
    cond = row["drained_condition"]

    out: Dict[str, SingletonValue] = {}
    if fam not in families and fam not in singletons:
        return out

    if fam == "cn":
        out.update(_expand_cn(val, cond))
    elif fam == "rz":
        out.update(_expand_rz(val, cond))
    elif fam == "max_net_infil":
        out.update(_expand_max_net_infil(val, cond))
    else:
        out[fam] = val
    return out