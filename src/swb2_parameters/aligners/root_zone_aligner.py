import numpy as np

def root_zone_aligner(root_zone_depth_a,
                      dict_prefix='rz_',
                      root_zone_factors=[1.25, 1.0, 0.666],
                      condition='drained'):
    """Enforce structure on root zone depths across soil groups.

    Factors roughly follow Thornthwaite & Mather (1957) relations.

    Args:
        root_zone_depth_a (float): A-soil root zone depth (feet).
        dict_prefix (str): prefix for keys (default 'rz_').
        root_zone_factors (list[float]): factors for B, C, D (default [1.25, 1.0, 0.666]).
        condition (str): 'drained' or 'undrained'.

    Returns:
        dict: root zone depths for b-d and dual-classification soil groups.
    """
    rz_b = np.round(root_zone_depth_a * root_zone_factors[0], decimals=2)
    rz_c = np.round(root_zone_depth_a * root_zone_factors[1], decimals=2)
    rz_d = np.round(root_zone_depth_a * root_zone_factors[2], decimals=2)

    if condition == 'drained':
        rz_ad = root_zone_depth_a
        rz_bd = rz_b
        rz_cd = rz_c
    else:
        rz_ad = rz_d
        rz_bd = rz_d
        rz_cd = rz_d

    return {
        f"{dict_prefix}a": root_zone_depth_a,
        f"{dict_prefix}b": rz_b,
        f"{dict_prefix}c": rz_c,
        f"{dict_prefix}d": rz_d,
        f"{dict_prefix}ad": rz_ad,
        f"{dict_prefix}bd": rz_bd,
        f"{dict_prefix}cd": rz_cd,
    }