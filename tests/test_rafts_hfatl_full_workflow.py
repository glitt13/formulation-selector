import shutil
import sqlite3
import subprocess
import pytest
import pandas as pd
import geopandas as gpd
import pyogrio
import xarray as xr
import yaml
from pathlib import Path

# Set paths relative to the repository root (assuming pytest is run from the root)
HERE = Path(__file__).resolve().parent
DIR_REPO = HERE.parent
DIR_CONFIG = DIR_REPO / "tests" / "config" / "hfatl"
DIR_DATA = DIR_REPO / "tests" / "data"
DIR_RUN = DIR_DATA / "run_data"

# Python execution paths
DIR_PREP = DIR_REPO / "pkg" / "rafts_prep" / "rafts_prep" / "flow"
DIR_PY = DIR_REPO / "pkg" / "rafts_algo" / "rafts_algo" / "flow"

# Test configurations
PREP_CONFIG = DIR_CONFIG / "hfatl_prep_config.yaml"
ATTR_CONFIG = DIR_CONFIG / "hfatl_attr_config.yaml"
ALGO_CONFIG = DIR_CONFIG / "hfatl_algo_config.yaml"
PRED_CONFIG = DIR_CONFIG / "hfatl_pred_config.yaml"

# Dataset identity and fixture shape, per hfatl_prep_config.yaml / hfatl_attr_config.yaml
DATASET = "hfatl_test_x30"
N_X300_LOCS = 300  # rows in integ_test_predictors_x300.parquet, ingested wholesale by step 1
N_TRAIN_GAGES = 30  # of those 300, the subset with a mapping in hfv4_x30.gpkg
N_TRAIN_DIVIDES = 1797  # divide-level rows in hfv4_x30.gpkg mapping to the 30 training gages
N_TRAIN_ATTRS = 19  # len(attr_select.hfatl_vars) in hfatl_attr_config.yaml
N_PRED_DIVIDES = 280  # rows in jul26_cal_hf4_predictors_9locations_final.parquet
ALGOS = {  # algo name -> expected number of clusters (k)
    "kmeans_k3": 3,
    "kmeans_k5": 5,
    "gower_agglomerative_k4": 4,
    "gower_agglomerative_k6": 6,
}

DIR_STD_BASE = DIR_RUN / "input" / "user_data_std" / DATASET
DIR_ATTRS = DIR_RUN / "input" / "attrs_hfatl" / DATASET / "all"
DIR_ALGOS = DIR_RUN / "output" / "trained_algorithms" / DATASET
DIR_PREDS = DIR_RUN / "output" / "algorithm_predictions" / DATASET
DIR_REGN = DIR_RUN / "output" / "regionalization" / DATASET
DIR_VIZ = DIR_RUN / "output" / "data_visualizations" / DATASET
PATH_REGN_GPKG = DIR_RUN / "output" / "regionalization" / "hfatl" / "rapid_test_9_locs_v4_regn.gpkg"

# Read the training attribute names from the same config the pipeline itself
# reads, rather than hardcoding a second, driftable copy of the list.
with open(ATTR_CONFIG) as _f:
    _attr_cfg = yaml.safe_load(_f)
TRAIN_ATTRS = next(
    x['hfatl_vars'] for x in _attr_cfg['attr_select'] if 'hfatl_vars' in x
)
assert len(TRAIN_ATTRS) == N_TRAIN_ATTRS

# Training and prediction featureSource are deliberately distinct here:
# training is on USGS gage-basin-aggregated attributes (hfatl_prep_config.yaml's
# col_schema), while prediction runs directly on raw, unaggregated divide-level
# hfATLAS data (jul26_cal_hf4_predictors_9locations_final.parquet) -- the two
# are independent scales, not the same tag reused. rafts_pred_algo.py used to
# silently inherit the training-time value for prediction output regardless
# (it read featureSource from the prep config linked via name_prep_config,
# with nothing prediction-specific); it now prefers the prediction config's
# own featureSource (hfatl_pred_config.yaml) when set, falling back to the
# training-time value only for configs that don't set one.
EXPECTED_TRAIN_FEATURESOURCE = "hfv4_test_id"
EXPECTED_PRED_FEATURESOURCE = "hfv4_divides_raw"


@pytest.fixture(scope="class", autouse=True)
def clean_run_data():
    """Remove generated pipeline output before (and after) the test class runs.

    Without this, a stale output directory from a previous run (or a previous
    failed attempt) can make a broken step look like it passed, since later
    steps may still find usable files left over from an earlier, different
    configuration. This bit the initial version of this test: a leftover
    dataset directory from an unrelated prep script masked a broken config
    for several steps.
    """
    shutil.rmtree(DIR_RUN, ignore_errors=True)
    yield
    shutil.rmtree(DIR_RUN, ignore_errors=True)


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

    Each step asserts on the actual output artifacts it should have produced,
    not just on the subprocess exit code, so a step that runs but silently
    produces wrong/empty output fails here instead of surfacing as a
    confusing failure (or a false pass) in a later step.
    """

    def test_step_1_prepare_test_dataset(self):
        """1. Ingest the gage-level predictor fixture wholesale, registering dataset metadata."""
        script = DIR_CONFIG / "prep_regn_test_agg.py"
        run_uv_command(script, str(PREP_CONFIG))

        nc_files = list(DIR_STD_BASE.glob("*.nc"))
        assert nc_files, f"No standardized .nc dataset written to {DIR_STD_BASE}"

        ds = xr.open_dataset(nc_files[0])
        assert ds.sizes.get("gage_id") == N_X300_LOCS, (
            f"Expected {N_X300_LOCS} ingested locations, got {ds.sizes.get('gage_id')}"
        )

    def test_step_2_grab_attributes(self):
        """2. Area-weight-aggregate divide-level attributes up to the 30 training gages."""
        script = DIR_PREP / "rafts_agg_hfatl_basin.py"
        run_uv_command(
            script,
            "--path_prep_config", str(PREP_CONFIG),
            "--path_attr_config", str(ATTR_CONFIG)
        )

        path_attrs = DIR_ATTRS / "attr_all.parquet"
        assert path_attrs.exists(), f"Aggregated attributes not written to {path_attrs}"

        df = pd.read_parquet(path_attrs)
        expected_cols = {"featureID", "featureSource", "data_source", "dl_timestamp", "attribute", "value"}
        assert expected_cols.issubset(df.columns), f"Missing expected columns: {expected_cols - set(df.columns)}"
        assert df["featureID"].nunique() == N_TRAIN_GAGES, (
            f"Expected {N_TRAIN_GAGES} aggregated gages, got {df['featureID'].nunique()}"
        )
        assert df["attribute"].nunique() == N_TRAIN_ATTRS, (
            f"Expected {N_TRAIN_ATTRS} aggregated attributes, got {df['attribute'].nunique()}"
        )
        # `.isna().all()` (any single non-NaN value anywhere passes) would let a
        # mostly-broken aggregation slip through; this fixture is clean, so
        # expect no NaNs at all.
        n_na = df["value"].isna().sum()
        assert n_na == 0, f"Expected no NaN aggregated attribute values, got {n_na}"
        assert set(df["featureSource"].unique()) == {EXPECTED_TRAIN_FEATURESOURCE}, (
            f"Expected featureSource == {EXPECTED_TRAIN_FEATURESOURCE!r}, got {df['featureSource'].unique()}"
        )

        gpkg_files = list(DIR_STD_BASE.glob("*_loc.gpkg"))
        assert gpkg_files, f"No companion _loc.gpkg written to {DIR_STD_BASE}"
        gdf = gpd.read_file(gpkg_files[0])
        assert gdf.shape[0] == N_TRAIN_DIVIDES, (
            f"Expected {N_TRAIN_DIVIDES} divide-level rows in companion GPKG, got {gdf.shape[0]}"
        )
        assert gdf["featureID"].nunique() == N_TRAIN_GAGES, (
            f"Expected {N_TRAIN_GAGES} unique gages in companion GPKG, got {gdf['featureID'].nunique()}"
        )

    def test_step_3_train_algorithms(self):
        """3. Train the clustering algorithms (chunk size 4) on the 30 training gages."""
        script = DIR_PY / "rafts_proc_algo_pool.py"
        run_uv_command(script, str(ALGO_CONFIG), "--chunk_size", "4")

        for algo in ALGOS:
            path_joblib = DIR_ALGOS / f"algo_{algo}_cluster_labels__{DATASET}.joblib"
            assert path_joblib.exists(), f"Trained model not found: {path_joblib}"

        path_eval = DIR_ALGOS / f"algo_eval_{DATASET}.csv"
        assert path_eval.exists(), f"Algorithm evaluation summary not found: {path_eval}"
        df_eval = pd.read_csv(path_eval)
        assert df_eval.shape[0] == len(ALGOS), (
            f"Expected {len(ALGOS)} rows in algo eval summary, got {df_eval.shape[0]}"
        )
        # build_schema_rslt_eval_df's clustering-specific columns.
        expected_cols = {"algorithm", "type", "metric", "dataset", "file_pipe", "algo",
                          "silhouette_score", "davies_bouldin_score"}
        assert expected_cols.issubset(df_eval.columns), (
            f"Missing expected eval columns: {expected_cols - set(df_eval.columns)}"
        )
        assert set(df_eval["algorithm"]) == set(ALGOS.keys()), (
            f"Expected eval rows for exactly {set(ALGOS.keys())}, got {set(df_eval['algorithm'])}"
        )
        # Both scores are mathematically undefined outside these ranges -- a
        # garbage value (e.g. from a broken labeling) would otherwise pass as
        # "just some float."
        assert df_eval["silhouette_score"].between(-1, 1).all(), (
            f"silhouette_score out of the valid [-1, 1] range: {df_eval['silhouette_score'].tolist()}"
        )
        assert (df_eval["davies_bouldin_score"] >= 0).all(), (
            f"davies_bouldin_score must be non-negative: {df_eval['davies_bouldin_score'].tolist()}"
        )

    def test_step_4_perform_prediction(self):
        """4. Perform cluster predictions directly on the 280 rapid-test divides."""
        script = DIR_PY / "rafts_pred_algo.py"
        run_uv_command(script, str(PRED_CONFIG))

        for algo, k in ALGOS.items():
            path_pred = DIR_PREDS / f"pred_{algo}_cluster_labels__{DATASET}.parquet"
            assert path_pred.exists(), f"Predictions not found: {path_pred}"

            df = pd.read_parquet(path_pred)
            assert df.shape[0] == N_PRED_DIVIDES, (
                f"{algo}: expected {N_PRED_DIVIDES} predicted divides, got {df.shape[0]}"
            )
            assert df["featureID"].nunique() == N_PRED_DIVIDES, f"{algo}: duplicate divide predictions found"
            assert not df["prediction"].isna().any(), f"{algo}: NaN cluster label predictions found"
            assert df["prediction"].nunique() <= k, (
                f"{algo}: expected at most {k} distinct cluster labels, got {df['prediction'].nunique()}"
            )
            # build_schema_df_pred's remaining required columns/values. Deliberately
            # NOT the training featureSource (EXPECTED_TRAIN_FEATURESOURCE) --
            # confirms rafts_pred_algo.py picks up hfatl_pred_config.yaml's own
            # featureSource rather than silently inheriting the training-time value.
            assert set(df["featureSource"].unique()) == {EXPECTED_PRED_FEATURESOURCE}, (
                f"{algo}: expected featureSource == {EXPECTED_PRED_FEATURESOURCE!r}, got {df['featureSource'].unique()}"
            )
            assert set(df["resp_var"].unique()) == {"cluster_labels"}, f"{algo}: unexpected resp_var value(s)"
            assert set(df["dataset"].unique()) == {DATASET}, f"{algo}: unexpected dataset value(s)"
            assert set(df["algo"].unique()) == {algo}, f"{algo}: 'algo' column doesn't match its own filename"
            name_algo_vals = df["name_algo"].unique()
            assert len(name_algo_vals) == 1 and name_algo_vals[0].endswith(".joblib") and algo in name_algo_vals[0], (
                f"{algo}: unexpected 'name_algo' value(s): {name_algo_vals}"
            )

    def test_step_5_pair_donor_receivers(self):
        """5. Pair the donor-receivers for the rapid-test prediction locations."""
        script = DIR_PY / "rafts_pair_donors.py"
        run_uv_command(script, str(PRED_CONFIG))

        # The valid donor pool: featureIDs actually present in the trained-on,
        # basin-aggregated attribute table (read fresh here rather than
        # trusting a hardcoded ID format).
        df_train_attrs = pd.read_parquet(DIR_ATTRS / "attr_all.parquet")
        valid_donor_ids = set(df_train_attrs["featureID"].astype(str))

        for algo in ALGOS:
            path_pred = DIR_PREDS / f"pred_{algo}_cluster_labels__{DATASET}.parquet"
            path_donors = DIR_REGN / f"donor_pairs_{algo}_cluster_labels__{DATASET}.csv"
            path_receivers = DIR_REGN / f"receiver_params_{algo}_cluster_labels__{DATASET}.csv"
            assert path_donors.exists(), f"Donor pairs not found: {path_donors}"
            assert path_receivers.exists(), f"Receiver params not found: {path_receivers}"

            # dtype=str for donor_id: it's the training gage ID, which for USGS
            # sites is all-digit with a meaningful leading zero (e.g.
            # '03050000') -- pd.read_csv would otherwise infer int64 and
            # silently drop it, which very nearly produced a false failure
            # here (the value on disk is correct; only an untyped read of it
            # wasn't).
            df_donors = pd.read_csv(path_donors, dtype={"donor_id": str})
            assert df_donors.shape[0] == N_PRED_DIVIDES, (
                f"{algo}: expected {N_PRED_DIVIDES} donor pairs, got {df_donors.shape[0]}"
            )
            assert not df_donors["donor_id"].isna().any(), f"{algo}: unpaired (NaN donor_id) receivers found"
            assert (df_donors["distance_to_donor"] >= 0).all(), f"{algo}: negative distance_to_donor found"
            assert set(df_donors["donor_id"].astype(str)).issubset(valid_donor_ids), (
                f"{algo}: donor_pairs references donor_id(s) outside the trained-on gage set: "
                f"{set(df_donors['donor_id'].astype(str)) - valid_donor_ids}"
            )

            df_receivers = pd.read_csv(path_receivers, dtype={"donor_id": str})
            assert df_receivers.shape[0] == N_PRED_DIVIDES, (
                f"{algo}: expected {N_PRED_DIVIDES} receiver rows, got {df_receivers.shape[0]}"
            )
            # receiver_params carries the donor's actual attribute values, not
            # just the pairing -- previously only shape[0] was checked here.
            missing_attrs = [a for a in TRAIN_ATTRS if a not in df_receivers.columns]
            assert not missing_attrs, f"{algo}: receiver_params missing attribute columns: {missing_attrs}"
            all_na_attrs = [a for a in TRAIN_ATTRS if df_receivers[a].isna().all()]
            assert not all_na_attrs, f"{algo}: receiver_params attribute columns entirely NaN: {all_na_attrs}"

            # Cross-file consistency: the same 280 featureIDs should flow
            # unchanged from step 4's predictions through donor_pairs and
            # receiver_params -- each file has so far only been checked for
            # row *count* in isolation, which wouldn't catch a set mismatch
            # of the same size (e.g. duplicated IDs offsetting dropped ones).
            pred_ids = set(pd.read_parquet(path_pred)["featureID"].astype(str))
            donor_receiver_ids = set(df_donors["receiver_id"].astype(str))
            receiver_ids = set(df_receivers["featureID"].astype(str))
            assert pred_ids == donor_receiver_ids == receiver_ids, (
                f"{algo}: featureID sets differ across pipeline steps -- "
                f"pred-only: {pred_ids - donor_receiver_ids - receiver_ids}, "
                f"donor_pairs-only: {donor_receiver_ids - pred_ids}, "
                f"receiver_params-only: {receiver_ids - pred_ids}"
            )
            # Same donor_id for the same receiver in both files.
            donor_map_a = df_donors.set_index("receiver_id")["donor_id"].astype(str)
            donor_map_b = df_receivers.set_index("featureID")["donor_id"].astype(str)
            mismatched = donor_map_a[donor_map_a != donor_map_b.reindex(donor_map_a.index)]
            assert mismatched.empty, (
                f"{algo}: donor_pairs and receiver_params disagree on donor_id for receivers: "
                f"{mismatched.index.tolist()}"
            )

    def test_step_6_map_predictions(self):
        """6. Map the predictions onto a static map for visual verification."""
        script = DIR_PY / "rafts_map_pred_hfatl.py"
        run_uv_command(script, str(PRED_CONFIG))

        for algo in ALGOS:
            path_png = DIR_VIZ / f"prediction_map_{DATASET}_cluster_labels_{algo}_test.png"
            assert path_png.exists(), f"Prediction map not found: {path_png}"
            assert path_png.stat().st_size > 0, f"Prediction map is empty: {path_png}"

    def test_step_7_write_params_to_gpkg(self):
        """7. Compile regionalized parameters into the rapid-test subset GPKG."""
        script = DIR_PY / "rafts_regn_params_gpkg.py"
        run_uv_command(script, str(PRED_CONFIG))

        assert PATH_REGN_GPKG.exists(), f"Regionalized GPKG not found: {PATH_REGN_GPKG}"

        layers = [lyr for lyr, _geom_type in pyogrio.list_layers(PATH_REGN_GPKG)]
        param_layers = [lyr for lyr in layers if "gower_agglomerative_k4" in lyr]
        assert param_layers, (
            f"No regionalized-parameters layer found for algo_select='gower_agglomerative_k4' "
            f"in {PATH_REGN_GPKG} (layers found: {layers}). This is exactly the failure mode "
            f"caught while writing this test: algo_select must match a trained algorithm name "
            f"exactly (e.g. 'gower_agglomerative_k4', not 'gower_agglomerative_4'), or this step "
            f"silently writes nothing new."
        )

        # hfatl_pred_config.yaml's algo_select is the single literal string
        # 'gower_agglomerative_k4' (not a wildcard), so rafts_regn_params_gpkg.py
        # is expected to filter out the other three trained algos entirely --
        # confirm that filtering actually excludes them, rather than assuming
        # it and only ever inspecting the one layer that does get written.
        other_algos = [a for a in ALGOS if a != "gower_agglomerative_k4"]
        unexpected_layers = [lyr for lyr in layers for a in other_algos if a in lyr]
        assert not unexpected_layers, (
            f"algo_select='gower_agglomerative_k4' should have excluded {other_algos}, "
            f"but found layer(s) for them: {unexpected_layers}"
        )

        gdf_params = gpd.read_file(PATH_REGN_GPKG, layer=param_layers[0])
        assert gdf_params.shape[0] == N_PRED_DIVIDES, (
            f"Expected {N_PRED_DIVIDES} rows in regionalized-parameters layer, got {gdf_params.shape[0]}"
        )
        assert "donor_id" in gdf_params.columns, "Regionalized-parameters layer missing 'donor_id' column"
        assert not gdf_params["donor_id"].isna().any(), "Regionalized-parameters layer has unpaired receivers"

        # The layer should carry the donor's actual attribute values through
        # to the final GPKG, not just the pairing -- previously unchecked here.
        missing_attrs = [a for a in TRAIN_ATTRS if a not in gdf_params.columns]
        assert not missing_attrs, f"Regionalized-parameters layer missing attribute columns: {missing_attrs}"

        # update_database() registers each written table in gpkg_contents via
        # register_gpkg_attributes_table() -- confirm that registration
        # actually happened rather than only checking the layer is readable
        # via geopandas (which doesn't depend on gpkg_contents at all).
        with sqlite3.connect(PATH_REGN_GPKG) as conn:
            registered = pd.read_sql(
                "SELECT table_name FROM gpkg_contents WHERE table_name = ?", conn, params=(param_layers[0],)
            )
        assert not registered.empty, (
            f"Layer {param_layers[0]!r} was written but never registered in gpkg_contents"
        )
