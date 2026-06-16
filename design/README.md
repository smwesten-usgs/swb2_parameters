# swb2_parameters — LONG→WIDE builder (with reverse) for SWB2

This repository converts **human-edited LONG tables** (TSV) into **SWB2-ready WIDE tables**, and provides a **reverse** path to bootstrap LONG tables from an existing WIDE table. It also supports **group-level templates** (e.g., `ALL`, `smgrain`) that materialize into per‑code rows prior to validation.

- Start with `README.md` for orientation & quickstart.
- Use `bootstrap_prompt.txt` to ground Copilot in the repo’s rules.
- Consult `NOTES.md` for design decisions, assumptions, and progress.

---

## Quickstart

### 1) Install (editable)
```bash
pip install -e .
```

### 2) Prepare inputs
- One or more **LONG** TSV files (canonical columns).
- A **selector** TOML (primary_key, families to expand, explicit singletons, hsg_count).
- Optionally, a **groups.tsv** file (columns: `lu_cdl`, `lu_nlcd`, `group`) for named-group templates.

### 3) Run (LONG → WIDE)
```bash
swb2-parameters params_long.tsv \
  --selector output_table_selections.toml \
  --groups groups.tsv \
  --outfile params_wide.tsv
```

**Selector TOML (example)**
```toml
# Choose the leading key in the wide output
primary_key = "cdl" # or "nlcd"

# Families to expand to HSG indices
include_families = ["cn", "rz", "max_net_infil"]

# Singletons to pass through unchanged
include_explicit = [
  "growing_season_interception",
  "nongrowing_season_interception",
  "kcb_ini", "kcb_mid", "kcb_end", "kcb_min",
  "depletion_fraction", "rew_1", "rew_2",
]

# Hydrologic soil group count
hsg_count = 7
```

### 4) Reverse (WIDE → LONG baseline)
```bash
swb2-parameters params_wide.tsv \
  --selector output_table_selections.toml \
  --to-long \
  --outfile params_long_from_wide.tsv
```

---

## Key concepts (short)

- **Canonical LONG schema** (TSV): `lu_cdl`, `lu_nlcd`, `description`, `group`, `column`,
  `parlbnd`, `parubnd`, `parval1`, `units`, `notes`, `ref`, `drained_condition`.
  Duplicates forbidden; bounds required; rounding: `cn` ints, others 2 decimals.
  Families (`cn`, `rz`, `max_net_infil`) are **A‑soil** values; singletons pass through.  
  [1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

- **Aligners** expand A‑soil values to HSG indices `1..7`; dual classes follow `drained_condition`.
  `cn` cast to integers; other families 2 decimals. Default HSG count = 7 (4 supported).  
  [1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

- **Group templates**:
  - Templates (blank chosen key + non‑empty `group`) materialize into per-code rows before validation.
  - Precedence: `ALL` → named group → code-specific.
  - **Canonicalization rule:** normalize chosen key column and **assign back** before materialization.  
    Prevents `"24.0"` vs `"24"` mismatches.  
    [1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

- **Reverse mode**:
  - Detect `lu_code*` and `description*`; emit one LONG row per parameter column; leave CDL/NLCD blank; no auto‑mapping.  
  [1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

---

## Repo structure (typical)
```
src/swb2_parameters/
  cli.py # entry point (long→wide; --to-long)
  io.py # loaders, TSV guards, selector parsing, normalization
  build_wide.py # expansion + assembly into wide
  expand.py # row-level family/singleton expansion
  aligners/ # cn, rz, max_net_infil aligners
  reverse.py # wide→long baseline
  group_templates.py # materialize ALL/named templates into per-code rows
docs/
  README.md
  NOTES.md
  bootstrap_prompt.txt
example/
  params_long.tsv
  groups.tsv
  output_table_selections.toml
```
[1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

---

## Conventions & tests
- **TSV only** (tab-delimited); fail fast on space-delimited.
- **Docstrings** (Google-style) for significant functions; minimal, composable patches.
- **pytest** targets: delimiter guard, duplicates, bounds/rounding, family expansion, reverse detection, group template precedence.
- Optional diagnostics: warn/fail when `include_explicit` requests columns missing after materialization.  
[1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

---

## Roadmap
- Add test coverage for group precedence (ALL vs named vs code-specific), canonicalization, and selector filtering.
- Constraints stubs (CN/infiltration monotonicity; optional strict).
- Optional helpers (`--coerce-whitespace-tsv`; manifest with timestamps + input hashes).  
[1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)

---

## References
- Data contracts, aligners, CLI behavior, and reverse rules are grounded in the current project docs.  
- Document roles and separation follow the guidance in `ABOUT_COPILOT_FILES.md`.  
[1](https://doimspp-my.sharepoint.com/personal/smwesten_usgs_gov/Documents/Microsoft%20Copilot%20Chat%20Files/README.md)
