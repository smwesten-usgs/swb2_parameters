import numpy as np

def curve_number_aligner(curve_number_a,
                         dict_prefix='cn_',
                         condition='drained'):
    """Implementation of the 'curve number aligner'.

    Based on Hawkins et al. (2009). Maximum theoretical CN for 'A' soil is 77
    (not enforced here).

    Args:
        curve_number_a (numeric): SCS curve number for hydrologic soil group "A".
        dict_prefix (str): prefix for returned dict keys (default 'cn_').
        condition (str): 'drained' or 'undrained'.

    Returns:
        dict: curve numbers for b-d and dual-classification soil groups.
    """
    curve_number_b = np.round(min([37.8 + 0.622 * curve_number_a, 100.]), decimals=2)
    curve_number_c = np.round(min([58.9 + 0.411 * curve_number_a, 100.]), decimals=2)
    curve_number_d = np.round(min([67.2 + 0.328 * curve_number_a, 100.]), decimals=2)

    if condition == 'drained':
        curve_number_ad = curve_number_a
        curve_number_bd = curve_number_b
        curve_number_cd = curve_number_c
    else:
        curve_number_ad = curve_number_d
        curve_number_bd = curve_number_d
        curve_number_cd = curve_number_d

    return {
        f"{dict_prefix}a": curve_number_a,
        f"{dict_prefix}b": curve_number_b,
        f"{dict_prefix}c": curve_number_c,
        f"{dict_prefix}d": curve_number_d,
        f"{dict_prefix}ad": curve_number_ad,
        f"{dict_prefix}bd": curve_number_bd,
        f"{dict_prefix}cd": curve_number_cd,
    }