# NOTES — swb2_parameters (design & decisions)

This notebook captures the *why* behind the code so future work is fast and consistent.

---

## Context & goals
We maintain **human-edited long tables** as the canonical source and **programmatically** generate the **wide** tables required by SWB2. We also provide a **reverse** path to bootstrap long tables from a wide table when needed. Key goals: preserve CDL/NLCD land-use handling; choose the primary key at run time and emit SWB2's `lu_code`; keep the long→wide transformation deterministic, validated, and minimal; keep reverse helpful but simple—no automatic CDL/NLCD remapping.

---

## Assumptions & units
- **Units (US Imperial)**: interception (**in**), root‑zone depth (**ft**), max net infiltration (**in/day**).
- CN is dimensionless and capped at 100 in the aligner; we cast to integers (or explicit `num_decimals`) in wide for family outputs.
- RZ expansion uses multiplicative factors (~**1.25, 1.0, 0.666** for B, C, D rel. to A).
- Max net infiltration expansion uses factors **0.15, 0.06, 0.03** (B, C, D rel. to A).
- **HSG count** defaults to **7** (A, B, C, D, A/D, B/D, C/D); 4 is supported by dropping 5..7 in WIDE.

---

## Canonical data contracts

### Long table (source of truth)
Columns (TSV):  
`lu_cdl`, `lu_nlcd`, `description`, `group`, `column`,
`parlbnd`, `parubnd`, `parval1`, `units`, `notes`, `ref`, `drained_condition`,
**`fixed_parval1`, `num_decimals`**.

Rules:
- Error on duplicate `(lu_cdl, lu_nlcd, column)`; require bounds; `parlbnd ≤ parubnd`; `parval1 ∈ [lb, ub]` for **BOUNDED** rows.
- **Two modes per row**:
  - **FIXED mode**: `fixed_parval1` non‑empty → bounds and rounding bypassed for singletons; **families (`cn`, `rz`, `max_net_infil`) in FIXED mode are NOT supported** and must fail.
  - **BOUNDED mode**: `fixed_parval1` empty → require bounds; `parval1` must be numeric and within bounds; rounding then applied.
- **Rounding** in BOUNDED mode:
  - Families: defaults `cn=0` decimals (integer), `rz=2`, `max_net_infil=2`.
  - **`num_decimals`** (integer ≥ 0) overrides the default rounding for **any BOUNDED row**:
    - For families: use `num_decimals` exactly.
    - For singletons: use `num_decimals` if provided; else **no rounding**.
- Loader: do **not** globally coerce `parval1`; **do** coerce `parlbnd`/`parubnd` to numeric; normalize family names (`max_net_infiltration` → `max_net_infil`) and `drained_condition` to `{drained, undrained}` with default `drained`.
- `num_decimals` is normalized on load; invalid values (non‑integer, negative) should raise early or be sanitized to a canonical integer string.

### Wide table (run-time)
- Leading key: **`lu_code`** (sourced from CDL or NLCD per selector) and a single **`description`**.
- Families expanded to `_1.._7` via aligners; **bounds/refs do not appear in WIDE**.

### Reverse (baseline long)
- Detect `lu_code` column; copy to long `lu_code`; leave `lu_cdl`/`lu_nlcd` empty.
- Detect `description*` columns; take first non‑empty per row; write to long `description`.
- Emit one long row per wide parameter column (skip keys & `description*`); set `fixed_parval1=""`, `num_decimals=""` in the baseline output.

---

## Aligners & expansion
- Families (`cn`, `rz`, `max_net_infil`) expand A‑soil value to HSG indices `1..7` using local aligners.
- Dual classes follow `drained_condition`: in drained mode, AD/BD/CD take A/B/C values; in undrained, duals take D.
- Rounding policy in wide follows the **effective value** computed in validation (defaults or `num_decimals` override for families).

---

## Group templates (materialization)
**Definition:** A template row has the chosen key column blank (`lu_cdl` if primary key=cdl; `lu_nlcd` if nlcd), a non‑empty `group`, and a valid `(column, bounds, parval1)` for BOUNDED rows.  
**Precedence:** (1) ALL applies to all codes present in LONG (chosen domain). (2) Named group overwrites ALL for its members (from `groups.tsv`). (3) Code-specific rows win over templates.

**Canonicalization (operational rule):**
- Normalize the chosen key column to canonical strings **and assign it back** to the DataFrame (`df[key_col] = normalized_series`) *before* partitioning into code rows vs. templates. This prevents mismatches like `"24.0"` vs `"24"`.
- Apply the same normalization to the groups file’s key column when loading `groups.tsv`.

**Diagnostics (optional):**
- If a requested singleton in the selector’s `include_explicit` is missing from the built wide frame, emit a warning (or fail in strict mode). This helps surface template/selection mismatches early.

---

## Design decisions & trade‑offs
- **Fail‑fast** delimiter guard (TSV required) to avoid subtle parsing errors.
- **No automatic remapping** in reverse: preserve `lu_code`, let users fill `lu_cdl`/`lu_nlcd`.
- **Single `drained_condition`** column (normalized to {drained, undrained}).
- **No bounds in wide**: keep run-time tables thin; bounds live in long.

---

## Progress log (high level)
- ✅ Long→wide pipeline with selector (primary key, families, singletons, HSG count).
- ✅ Aligners wired (CN cap to 100; RZ & infiltration factors; dual-class handling; rounding).
- ✅ Reverse mode (detect `lu_code*` and `description*`).
- ✅ Duplicate detection; bounds checks; family rounding policy implemented.
- ✅ **New:** BOUNDED vs FIXED authoring model in validation:
  - `fixed_parval1` introduced; families fixed → fail; singletons fixed → pass-through.
  - `num_decimals` controls rounding for BOUNDED rows (families & singletons).
- ✅ Loader updated: `parval1` left as object; `num_decimals` normalized at read-time.

---

## Next steps
- Add **pytest** coverage for:
  - singleton FIXED mode passthrough,
  - family FIXED mode failure,
  - BOUNDED rows with `num_decimals` overrides (families & singletons),
  - max_net_infil consistency across many land-use codes.
- Add constraints stubs (e.g., CN and infiltration monotonicity; optional strict).
- Optional helpers (`--coerce-whitespace-tsv`; manifest with timestamps + input hashes).

---

## Gotchas
- Wide tables that include extra metadata fields (beyond `description`) will be treated as parameters in reverse unless excluded—keep wide clean.
- In reverse, long rows have `column` equal to the **wide column name** (e.g., `cn_2`). Forward expects **family names** in canonical long rows; other rows are ignored unless explicitly selected.
- Template group names must match `groups.tsv` **exactly** (e.g., `smgrain` vs `small_grains`).
- If the chosen primary key column is numeric in the TSV, normalize and assign back; otherwise you may see mismatches (`"24.0"` vs `"24"`) and missing materializations.