# PEST++ Export Layer — Design Document

## Goal

Add a module to `swb2_parameters` that takes the existing wide-table output (or long-form TSVs directly) and produces PEST++-ready artifacts:

1. **Template files** (`.tpl`) for `lu_lookup.txt` and `irr_lookup.txt`
2. **Parameter data CSV** (`par_data.csv` fragment) with `parnme`, `partrans`, `parval1`, `parlbnd`, `parubnd`, `pargp`
3. **Parameter group CSV** (`pargp_data.csv`)

No pyemu dependency. No model execution. Pure file generation from the parameter tables that `swb2_parameters` already knows how to produce.

---

## Motivation

Every SWB2+PEST++ project hand-builds template files and parameter bounds CSVs. The Michigan workflow did it with ~200 lines of fragile string manipulation in `05_setup_pest.py`. The resulting `.tpl` files and `par_data.csv` depend entirely on the lookup table structure, which is standardized across all SWB2 models. This should be a one-command operation.

---

## Inputs

1. **Wide-form lookup tables** (the output of `swb2-parameters` LONG→WIDE pipeline):
   - `lu_lookup.txt` (tab-delimited, first row = header, columns: `lu_code`, `description`, then parameter columns)
   - `irr_lookup.txt` (same structure)

   OR equivalently, the **long-form TSVs** + selector TOML (and the tool runs the full pipeline internally).

2. **A configuration** specifying:
   - Which columns are **fixed** (written literally, not parameterized) — e.g., `growing_season_interception`, `nongrowing_season_interception`, date-like fields, categorical fields
   - Which columns are **parameterized** (get template markers + par_data rows)
   - Parameter group assignments (default: `lu_lookup`, `irr_lookup`)
   - `partrans` setting per group or per family (default: `none` for lookup params)
   - Bounds strategy: use long-form `parlbnd`/`parubnd` if available, else apply a default multiplier (e.g., ±10%)

---

## Outputs

### 1. Template Files

**`lu_lookup.txt.tpl`** — identical structure to `lu_lookup.txt` but:
- First line: `ptf ~`
- Header line unchanged
- Data lines: fixed columns written as literals; parameterized columns replaced with `~ parnme ~` markers

**Parameter naming convention:**
```
{table}_{param}:{lu_code}
```
Examples: `lu_cn_2:1`, `lu_rz_3:141`, `irr_kcb_mid:1`, `irr_tew_5:28`

This is shorter and more robust than the Michigan workflow's original `lu_lu_cn_2:1:corn` scheme (which broke on land-use names containing spaces/slashes).

**Field width:** The marker width (total characters between and including the `~` delimiters) is the **only** mechanism PEST++ provides for controlling numeric output format. See "PEST++ Template Substitution Mechanics" below for details. A uniform marker width of 20 characters is recommended — this yields ~16 digits of precision, which is far more than any SWB2 parameter requires. The minimum is `len(parnme) + 4` (for `~ ` + name + ` ~`).

### 2. Working Lookup Tables

**`lu_lookup.txt`** — same structure as the wide table but with numeric values formatted to match the template marker width. This ensures PEST++ substitution produces a file with identical column alignment.

### 3. Parameter Data CSV

**`par_data.csv`** with columns matching PEST++ version 2 external format:
```
parnme,partrans,parchglim,parval1,parlbnd,parubnd,pargp,scale,offset,dercom
lu_cn_1:1,none,factor,69.5,59.075,79.925,lu_lookup,1.0,0.0,1
lu_cn_2:1,none,factor,79.5,67.575,91.425,lu_lookup,1.0,0.0,1
irr_kcb_mid:1,none,factor,1.15,0.92,1.38,irr_lookup,1.0,0.0,1
...
```

**Bounds sourcing (priority order):**
1. If long-form TSV has `parlbnd`/`parubnd` for this (lu_code, parameter) → use those
2. Else apply configurable default: `parlbnd = parval1 * (1 - margin)`, `parubnd = parval1 * (1 + margin)` where margin defaults to 0.15 (15%)
3. Special handling for parameters that must remain positive or have physical caps (e.g., CN ≤ 100)

**Zero-value handling:** Parameters with `parval1 = 0` get `partrans = fixed` (can't multiply zero by anything useful).

### 4. Parameter Group CSV

**`pargp_data.csv`:**
```
pargpnme,inctyp,derinc,derinclb,forcen,derincmul,dermthd,splitthresh,splitreldiff,splitaction
lu_lookup,relative,0.01,0.0,switch,2.0,parabolic,1e-05,0.5,smaller
irr_lookup,relative,0.01,0.0,switch,2.0,parabolic,1e-05,0.5,smaller
```

---

## CLI Interface

```bash
# From wide tables directly:
swb2-parameters pest-export \
  --lu-table lu_lookup.txt \
  --irr-table irr_lookup.txt \
  --fixed-columns "growing_season_interception,nongrowing_season_interception" \
  --bounds-margin 0.15 \
  --outdir ./pest_files/

# From long-form (runs full pipeline then exports):
swb2-parameters pest-export \
  --long params_long.tsv \
  --selector output_table_selections.toml \
  --groups groups.tsv \
  --fixed-columns "growing_season_interception,nongrowing_season_interception" \
  --outdir ./pest_files/
```

**Output files:**
```
pest_files/
├── lu_lookup.txt.tpl
├── lu_lookup.txt
├── irr_lookup.txt.tpl
├── irr_lookup.txt
├── par_data.csv
└── pargp_data.csv
```

---

## Configuration (TOML)

An optional `pest_export.toml` can specify everything instead of CLI flags:

```toml
[tables]
lu = "lu_lookup.txt"
irr = "irr_lookup.txt"

[fixed_columns]
# These columns are written literally (not parameterized)
lu = [
    "growing_season_interception",
    "nongrowing_season_interception",
]
irr = [
    "irrigation_start",
    "irrigation_end",
    "application_amount",
    "first_day_of_growing_season",
    "planting_date",
    "fraction_irrigation_from_gw",
    "application_scheme",
    "irrigation_application_efficiency",
    "last_day_of_growing_season",
    "l_fallow",
    "irrigation_length",
    "l_ini",
    "l_mid",
    "l_dev",
    "na",
]

[bounds]
default_margin = 0.15       # ±15% for params without explicit bounds
cn_max = 100                # physical cap for curve numbers

[groups]
lu_pargp = "lu_lookup"
irr_pargp = "irr_lookup"

[naming]
# Template marker width (minimum characters for alignment)
min_marker_width = 17
```

---

## PEST++ Template Substitution Mechanics

PEST++ provides **no per-parameter format control**. The template marker width IS the format specification. Understanding this is essential for generating correct `.tpl` files.

### How PEST++ Writes Values Into Templates

1. The **field width** = total characters between and including the two `~` delimiters. Example: `~ lu_cn_2:1        ~` = 20 characters.
2. PEST++ writes the parameter value into that exact character count using a C-style `%.*g` format, choosing precision to fill the available width.
3. If `fill_tpl_zeros = True` (the PEST++ default), trailing zeros pad the value to consume the full field width (e.g., `1.0` → `1.00000000000000000`).
4. If the number is too large for the field, PEST++ switches to scientific notation.
5. There is **no option** to specify fixed-point vs. scientific, number of decimal places, or significant digits on a per-parameter basis.

### Available PEST++ Options (complete list)

| Option | Default | Effect |
|--------|---------|--------|
| `fill_tpl_zeros` | `True` | Pad substituted values with trailing zeros to fill the marker width |
| `tpl_force_decimal` | `False` | Force a decimal point even for integer-valued parameters |

That's it. No precision control, no format strings, no per-field overrides.

### Implications for SWB2 Lookup Tables

- SWB2 reads lookup tables as **free-format floats** separated by tabs. Column alignment is irrelevant — only the tab delimiter matters.
- A substituted value of `69.5000000000000000` is functionally identical to `69.5` from SWB2's perspective.
- The only hard constraint: the marker must be wide enough to hold the largest parameter value that could be substituted. For CN values (max 100), a 20-character marker provides ample room.
- The Michigan workflow used marker widths of 58 (irr) and 89 (lu) characters — this was driven by long parameter names (`lu_lu_cn_2:1:corn`), not by any formatting need. With shorter names (`lu_cn_2:1`), 20 characters is sufficient.

### Recommended Approach

Use a **uniform marker width of 20 characters** for all lookup table parameters:
- Minimum required: `len(longest_parnme) + 4` (for the `~ ` prefix and ` ~` suffix)
- 20 characters accommodates names up to 16 characters (e.g., `irr_kcb_mid:195` = 15 chars)
- Provides ~16 digits of numeric precision in the substituted value
- Keeps template files readable and reasonably sized

If a parameter name exceeds 16 characters, increase the marker width accordingly. The tool should compute `marker_width = max(20, longest_parnme_in_table + 4)` per table.

---

## Design Decisions

1. **No pyemu dependency.** The export is pure string/DataFrame manipulation. This keeps the package lightweight and avoids version conflicts on HPC systems.

2. **Parameter names are short and machine-friendly.** `lu_cn_2:1` not `lu_lu_cn_2:1:corn`. The lu_code is the unique identifier; the crop name is redundant and causes problems with spaces/slashes.

3. **Fixed columns are explicit.** Rather than trying to auto-detect which columns are dates or categories, the user declares them. This is safer and more transparent.

4. **Bounds come from the long-form TSV when available.** The long-form already has `parlbnd`/`parubnd` — that's the scientifically-informed source. The margin-based fallback is for quick-start cases where you just want something reasonable.

5. **The working `.txt` files have fixed-width numeric fields.** This is required because PEST++ substitutes values into the exact character positions defined by the template markers. If the `.txt` has `79.5` (4 chars) but the template marker is 17 chars wide, PEST++ will write `       79.50000` — which only works if the original `.txt` also has 17-char-wide fields.

6. **Output is deterministic.** Same inputs → same outputs, byte-for-byte. No timestamps, no random elements.

---

## Integration with Existing Code

The export layer lives alongside the existing pipeline:

```
src/swb2_parameters/
  cli.py              # add 'pest-export' subcommand
  pest_export.py      # NEW: template generation + par_data assembly
  ...existing modules...
```

`pest_export.py` operates on the **wide DataFrame** (output of `build_wide()`). It doesn't need to know about aligners, groups, or long-form schemas — it just sees a table with `lu_code`, `description`, and parameter columns.

---

## Future Extensions (Not in Scope Now)

- **AWC grid parameterization:** Generating `awc_inst0_grid.csv.tpl` + AWC par_data rows from a raster. This requires geostatistics (pyemu dependency) and is better suited to the future `swb2-pestify` tool.
- **Observation extraction via `DUMP_VARIABLES`:** SWB2 natively supports point-based output via the control file directive `DUMP_VARIABLES COORDS <x> <y> ID <site_name>`. This causes SWB2 to write daily values of all important state variables for that location to a CSV — no NetCDF post-processing needed. A future tool could auto-generate these directives from an Ameriflux site CSV (or any point observation source), eliminating the entire `get_comparison_aet()` post-processing step from the forward run. The resulting CSVs would be directly usable as PEST++ observation files with minimal formatting.
- **Observation extraction (NetCDF fallback):** For basin-scale observations (streamflow), NetCDF spatial clipping is still required. But for point observations, `DUMP_VARIABLES` is the cleaner path.
- **`forward_run.py` generation:** Templated model-command script. Needs to know about the model directory layout.
- **Phi factor / weighting logic:** Observation-side concern, not parameter-side.

---

## Test Plan

1. Round-trip: wide table → `.tpl` + `.txt` → simulate PEST++ substitution (write parval1 back into template) → verify result matches `.txt` byte-for-byte
2. Parameter naming: verify no spaces, slashes, or characters that break PEST++ name rules (max 200 chars, alphanumeric + `_:.-`)
3. Bounds: verify parlbnd ≤ parval1 ≤ parubnd for all emitted rows
4. Zero handling: verify parval1=0 → partrans=fixed
5. Fixed columns: verify they appear as literals in `.tpl`, not as markers
6. Marker width: verify all markers in `.tpl` have identical width per table
7. Integration: feed Michigan lower-peninsula lookup tables through the pipeline, compare output against the hand-built Hovenweep versions
