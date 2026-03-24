# swb2_parameters
[WIP] Attempt to streamline and standardize SWB2 parameter set creation.

Long→wide parameter builder for SWB2. Human-edited **long** TSV(s) are the source
of truth; this package expands family parameters (CN, root zone, max net infiltration)
to HSG indices and writes a **wide** TSV for model runs.

## Install

```bash
pip install -e .
