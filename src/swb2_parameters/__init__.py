"""
SWB2 parameter utilities: long→wide transformer.

Reads one or more human-edited *long* TSVs, validates and computes parval1,
expands families (cn, rz, max_net_infil) to HSG indices 1..7 using aligners,
and writes a *wide* TSV for model runs.
"""
__version__ = "0.1.0"