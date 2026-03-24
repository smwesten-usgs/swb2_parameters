from __future__ import annotations

from typing import Dict, List, Union
import pandas as pd

# Aligners packaged inside swb2_parameters.aligners
from swb2_parameters.aligners.curve_number_aligner import curve_number_aligner
from swb2_parameters.aligners.root_zone_aligner import root_zone_aligner
from swb2_parameters.aligners.net_infiltration_aligner import net_infiltration_aligner

SOIL_ORDER = ["a", "b", "c", "d", "ad", "bd", "cd"]
INDEX_MAP = {k: i + 1 for i, k in enumerate(SOIL_ORDER)}  # 'a'->1, ..., 'cd'->7

SingletonValue = Union[int, float]


def _expand_cn(a_value: float, condition: str) -> Dict[str, int]:
    """Expand CN A-value to cn_1..cn_7 (integers)."""
    d = curve_number_aligner(a_value, condition=condition)
    out = {}
    for key in SOIL_ORDER:
        col = f"cn_{INDEX_MAP[key]}"
        out[col] = int(round(d[f"cn_{key}"]))
    return out


def _expand_rz(a_value: float, condition: str) -> Dict[str, float]:
    """Expand root-zone A-value to rz_1..rz_7 (2 decimals)."""
    d = root_zone_aligner(a_value, condition=condition)
    out = {}
    for key in SOIL_ORDER:
        col = f"rz_{INDEX_MAP[key]}"
        out[col] = float(d[f"rz_{key}"])
    return out


def _expand_max_net_infil(a_value: float, condition: str) -> Dict[str, float]:
    """Expand net infiltration A-value to max_net_infil_1.._7 (2 decimals)."""
    d = net_infiltration_aligner(a_value, dict_prefix="max_net_infil_", condition=condition)
    out = {}
    for key in SOIL_ORDER:
        col = f"max_net_infil_{INDEX_MAP[key]}"
        out[col] = float(d[f"max_net_infil_{key}"])
    return out


def expand_row(row: pd.Series, families: List[str], singletons: List[str]) -> Dict[str, SingletonValue]:
    """Expand a single long-table row into wide columns for selected families & singletons.

    Args:
        row: dataframe row with fields including column, parval1, condition.
        families: families to expand ('cn', 'rz', 'max_net_infil').
        singletons: singleton column names to pass through unchanged.

    Returns:
        dict mapping output column names to expanded values.
    """
    fam = row["column"]
    val = row["parval1"]
    cond = row["condition"]
    out: Dict[str, SingletonValue] = {}

    # Skip if not requested
    if fam not in families and fam not in singletons:
        return out

    if fam == "cn":
        out.update(_expand_cn(val, cond))
    elif fam == "rz":
        out.update(_expand_rz(val, cond))
    elif fam == "max_net_infil":
        out.update(_expand_max_net_infil(val, cond))
    else:
        # Singleton -> pass through unchanged
        out[fam] = val
    return out