import subprocess
import pytest
from pathlib import Path

# Set paths relative to the repository root (assuming pytest is run from the root)
HERE = Path(__file__).resolve().parent
DIR_REPO = HERE.parent
DIR_CONFIG = DIR_REPO / "tests" / "config" / "hfatl"
DIR_DATA = DIR_REPO / "tests" / "data"

# Python execution paths
DIR_PREP = DIR_REPO / "pkg" / "rafts_prep" / "rafts_prep" / "flow"
DIR_PY = DIR_REPO / "pkg" / "rafts_algo" / "rafts_algo" / "flow"

# Test configurations
PREP_CONFIG = DIR_CONFIG / "hfatl_prep_config.yaml"
ATTR_CONFIG = DIR_CONFIG / "hfatl_attr_config.yaml"
ALGO_CONFIG = DIR_CONFIG / "hfatl_algo_config.yaml"
PRED_CONFIG = DIR_CONFIG / "hfatl_pred_config.yaml"

def run_uv_command(script_path, *args):
    """Executes the pipeline script and fails the test if the subprocess fails.

    Runs with cwd=DIR_CONFIG so the relative paths inside the hfatl_*.yaml
    configs (e.g. '../../data/...') resolve consistently across flow scripts,
    some of which resolve relative paths against the config file's own
    directory and some of which resolve them against the process cwd.
    """
    cmd = [
        "uv", "run",
        "--project", str(DIR_REPO / "pkg"),
        "python", str(script_path)
    ] + list(args)

    result = subprocess.run(
        cmd,
        cwd=DIR_CONFIG,
        capture_output=True,
        text=True,
        check=False
    )

    if result.returncode != 0:
        pytest.fail(f"Pipeline step failed: {' '.join(cmd)}\nSTDERR: {result.stderr}")

    return result

class TestRegionalizationPipeline:
    """
    Integration test for the hfATLAS regionalization pipeline using subset data:
    - Training: 30 gages / 1797 divides (hfv4_x30.gpkg + integ_test_predictors_x300.parquet)
    - Prediction: 9 gages / 280 divides (rapid_test_9_locs_v4.gpkg + jul26_cal_hf4_predictors_9locations_final.parquet)
    """

    def test_step_1_prepare_test_dataset(self):
        """1. Ingest the gage-level predictor fixture, registering dataset metadata/geometry."""
        script = DIR_CONFIG / "prep_regn_test_agg.py"
        run_uv_command(script, str(PREP_CONFIG))

    def test_step_2_grab_attributes(self):
        """2. Area-weight-aggregate divide-level attributes up to the 30 training gages."""
        script = DIR_PREP / "rafts_agg_hfatl_basin.py"
        run_uv_command(
            script,
            "--path_prep_config", str(PREP_CONFIG),
            "--path_attr_config", str(ATTR_CONFIG)
        )

    def test_step_3_train_algorithms(self):
        """3. Train the clustering algorithms (chunk size 4) on the 30 training gages."""
        script = DIR_PY / "rafts_proc_algo_pool.py"
        run_uv_command(script, str(ALGO_CONFIG), "--chunk_size", "4")

    def test_step_4_perform_prediction(self):
        """4. Perform cluster predictions directly on the 280 rapid-test divides."""
        script = DIR_PY / "rafts_pred_algo.py"
        run_uv_command(script, str(PRED_CONFIG))

    def test_step_5_pair_donor_receivers(self):
        """5. Pair the donor-receivers for the rapid-test prediction locations."""
        script = DIR_PY / "rafts_pair_donors.py"
        run_uv_command(script, str(PRED_CONFIG))

    def test_step_6_map_predictions(self):
        """6. Map the predictions onto a static map for visual verification."""
        script = DIR_PY / "rafts_map_pred_hfatl.py"
        run_uv_command(script, str(PRED_CONFIG))

    def test_step_7_write_params_to_gpkg(self):
        """7. Compile regionalized parameters into the rapid-test subset GPKG."""
        script = DIR_PY / "rafts_regn_params_gpkg.py"
        run_uv_command(script, str(PRED_CONFIG))
