# swb2_parameters
[WIP] Attempt to streamline and standardize SWB2 parameter set creation.

---

## Quickstart

```bash
# From the repo root
pip install -e .

# Build a wide (run-time) table from one or more long TSVs
swb2-parameters params_long.tsv --selector output_table_selections.toml --outdir outputs

# Build a long table from a wide table (baseline for editing)
swb2-parameters params_wide.tsv --selector output_table_selections.toml --to-long --outdir outputs