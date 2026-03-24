import numpy as np

def net_infiltration_aligner(max_net_infil_a,
                             dict_prefix='max_net_infil_',
                             net_infil_factors=[0.15, 0.06, 0.03],
                             condition='drained'):
    """Enforce structure on max net infiltration across soil groups.

    Args:
        max_net_infil_a (float): A-soil max net infiltration (in/day).
        dict_prefix (str): prefix for keys (default 'max_net_infil_').
        net_infil_factors (list[float]): factors for B, C, D (default [0.15, 0.06, 0.03]).
        condition (str): 'drained' or 'undrained'.

    Returns:
        dict: infiltration for b-d and dual-classification soil groups.
    """
    ni_b = np.round(max_net_infil_a * net_infil_factors[0], decimals=2)
    ni_c = np.round(max_net_infil_a * net_infil_factors[1], decimals=2)
    ni_d = np.round(max_net_infil_a * net_infil_factors[2], decimals=2)

    if condition == 'drained':
        ni_ad = max_net_infil_a
        ni_bd = ni_b
        ni_cd = ni_c
    else:
        ni_ad = ni_d
        ni_bd = ni_d
        ni_cd = ni_d

    return {
        f"{dict_prefix}a": max_net_infil_a,
        f"{dict_prefix}b": ni_b,
        f"{dict_prefix}c": ni_c,
        f"{dict_prefix}d": ni_d,
        f"{dict_prefix}ad": ni_ad,
        f"{dict_prefix}bd": ni_bd,
        f"{dict_prefix}cd": ni_cd,
    }