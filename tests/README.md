Here is a recommended update for your **`tests/README.md`** that reflects the new `uv` execution framework, the addition of the `hfATLAS` integration tests, and the relative data paths.

```markdown
# Background
Integration tests for the data processing pipeline across the RaFTS modeling framework. 

The test suite now covers two primary pathways:
1. **Legacy NLDI/CAMELS Workflow**: Tests the `rafts_prep`, R-based `proc.attr.hydfab`, and `rafts_algo` packages.
2. **hfATLAS Workflow**: A pure-Python pipeline leveraging `rafts_prep` and `rafts_algo` to perform direct hydrofabric geometry extraction, model training, dynamic prediction, donor-receiver pairing, parameter regionalization, and mapping.

The current version of the input data required to run these tests is bundled inside `testdata_20250901.zip`.

# Steps for running the RaFTS integration tests

### 1. Download and Extract Test Data
Those with access to the NOAA Google Drive may download the integration test input data (`testdata_20250901.zip`) from the following link:
[https://drive.google.com/drive/folders/1JDtJKSfbmtBBp1nkBdNMwzS_EvFYkvpC?usp=drive_link](https://drive.google.com/drive/folders/1JDtJKSfbmtBBp1nkBdNMwzS_EvFYkvpC?usp=drive_link)

Extract the contents into the `tests/data/` directory so that the relative paths inside the test configuration files (e.g., `../../data/...`) resolve correctly. If you extract the data elsewhere, you must manually update the `file_io` and `hydrofabric_io` sections within the `tests/config/xssa/` and `tests/config/hfatl/` YAML files.

### 2. Run the Tests via `uv`
RaFTS manages its Python environment and dependencies via `uv` centered in the `pkg/` directory. All testing commands should be executed from within `pkg/` to ensure the correct virtual environment is utilized.

Navigate to the package directory:
```bash
cd path/to/git/rafts/pkg/

```

**To run the entire integration test suite:**

```bash
uv run pytest ../tests/

```

**To run specific workflows independently:**

* **hfATLAS Full Workflow (5 Steps):**
Tests pure-Python data ingestion, parallelized algorithm training, dynamic prediction, donor-receiver pairing, parameter regionalization (SQLite/GPKG), and map generation.
```bash
uv run pytest ../tests/test_rafts_hfatl_full_workflow.py -v

```


* **Legacy xSSA Workflow (4 Steps):**
Tests the core workflow utilizing NLDI feature metadata and `proc.attr.hydfab` integrations.
```bash
uv run pytest ../tests/test_rafts_prep_to_pred.py -v

```



> **Note on Debugging:** If a test fails, you can stream the standard output and standard error from the Python subprocesses directly to your terminal by running pytest with the `-s` flag (e.g., `uv run pytest ../tests/test_rafts_hfatl_full_workflow.py -v -s`).
