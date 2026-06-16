# REFERENCES — swb2_parameters

Curated links for libraries, hydrologic concepts, and external resources referenced in the **swb2_parameters** project.

## Startup References
### SWB2 Codebase & Hydrology Context
- **Primary SWB2 repository (current code):**  
  https://github.com/smwesten-usgs/swb2
- USGS Soil Water Balance Code report describing SWB2 code operation:
  https://pubs.usgs.gov/publication/tm6A59


## Python & Packaging
- PEP 621 – Project metadata in `pyproject.toml`: https://peps.python.org/pep-0621/
- setuptools documentation: https://setuptools.pypa.io/
- `src/` layout background: https://packaging.python.org/en/latest/discussions/src-layout/
- TOML specification (used for selector files): https://toml.io/en/

## Data Handling & Validation
- Pandas documentation (DataFrame operations, grouping, dtype normalization):  
  https://pandas.pydata.org/docs/
- NumPy documentation (numeric conversion, NaN handling):  
  https://numpy.org/doc/


### Curve Number & Soil Hydrologic Groups
- Curve Number (CN) method background:  
  https://www.nrcs.usda.gov/resources/guides-and-instructions/hydrologic-soil-cover-complex
- Hydrologic Soil Groups (A/B/C/D and dual groups):  
  https://www.nrcs.usda.gov/resources/data-and-reports/hydrologic-soil-groups

### Soil Moisture, Root Zone, and Infiltration Concepts
- Soil/root-zone depth reference material:  
  https://www.nrcs.usda.gov/resources/data-and-reports/soil-survey-information
- Infiltration & runoff primer:  
  https://www.usgs.gov/special-topics/water-science-school/science/infiltration-and-runoff

## Land-Use Datasets (Used by `lu_cdl` / `lu_nlcd`)
- Cropland Data Layer (CDL):  
  https://www.nass.usda.gov/Research_and_Science/Cropland/Release/
- National Land Cover Database (NLCD):  
  https://www.mrlc.gov/data

## CLI, File Formats, and Standards
- `argparse` documentation (CLI used by `cli.py`):  
  https://docs.python.org/3/library/argparse.html
- TSV vs. CSV (RFC 4180 background):  
  https://www.rfc-editor.org/rfc/rfc4180

## Testing & Development Workflow
- Pytest documentation (recommended for delimiter guard, duplication, template precedence tests):  
  https://docs.pytest.org/en/stable/
- Temporary directories / fixtures:  
  https://docs.pytest.org/en/stable/how-to/tmp_path.html

## Documentation (Future Enhancements)
- Sphinx documentation:  
  https://www.sphinx-doc.org/
- Napoleon extension (Google/NumPy docstrings):  
  https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html
- Furo theme:  
  https://pradyunsg.me/furo/
- MyST Markdown for Sphinx:  
  https://myst-parser.readthedocs.io/
