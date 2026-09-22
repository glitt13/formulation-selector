#!/bin/bash

# RaFTS overarching processing script for regionalization testing datasets with hfATLAS data
# Instructions:
# Make this script executable using: chmod +x regn_all_proc.sh
# Run by calling in terminal: ./regn_all_proc.sh

# -----------------------------------------------------------------------------
# ERROR HANDLING
# -----------------------------------------------------------------------------
# -e: Exit immediately if a command exits with a non-zero status.
# -u: Treat unset variables as an error and exit immediately.
# -o pipefail: Ensure errors in piped commands are caught.
set -euo pipefail || { echo "..."; exit 1; }

# -----------------------------------------------------------------------------
# UNIVERSAL PATH DEFINITIONS
# -----------------------------------------------------------------------------
echo "Using system home directory as basis for all paths: $HOME"
DIR_REPO="$HOME/git/rafts"
DIR_CONFIG="${DIR_REPO}/scripts/workflow_configs/regn_fy26_hfv4_conus_clst"
DIR_PREP="${DIR_REPO}/pkg/rafts_prep/rafts_prep/flow"
DIR_PY="${DIR_REPO}/pkg/rafts_algo/rafts_algo/flow"

echo "Running processing from $DIR_CONFIG"

# -----------------------------------------------------------------------------
# EXECUTION WORKFLOW
# -----------------------------------------------------------------------------
# Define the unique prefixes for each model to iterate over
MODELS=("casam" "cfex" "cfes" "sac" "top")

for MODEL in "${MODELS[@]}"; do
    echo "========================================================================"
    echo "Starting execution of hfATLAS parameter regionalization for: ${MODEL}"
    echo "========================================================================"

    # Define the yaml configurations for the current model in the loop
    PREP_CONF="regn_${MODEL}_agg_prep_config.yaml"
    ATTR_CONF="regn_${MODEL}_attr_config.yaml"
    ALGO_CONF="regn_${MODEL}_algo_config.yaml"
    PRED_CONF="regn_${MODEL}_pred_config_hf4.yaml"

    # 1. Prepare the initial dataset
    echo "--> Preparing the initial dataset..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_CONFIG}/prep_regn_test_agg.py" "${DIR_CONFIG}/${PREP_CONF}" || {
        echo "ERROR: Dataset preparation failed for ${MODEL}. Exiting."
        exit 1
    }

    # 2. Run the hfATLAS standardized prep script
    echo "--> Grabbing attributes..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PREP}/rafts_agg_hfatl_basin.py" \
        --path_prep_config "${DIR_CONFIG}/${PREP_CONF}" \
        --path_attr_config "${DIR_CONFIG}/${ATTR_CONF}" || {
        echo "ERROR: Attribute grabbing failed for ${MODEL}. Exiting."
        exit 1
    }

    # 3. Train the algorithms 
    echo "--> Training & testing algorithms..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/rafts_proc_algo_pool.py" "${DIR_CONFIG}/${ALGO_CONF}" --chunk_size 4 || {
        echo "ERROR: Algorithm training failed for ${MODEL}. Exiting."
        exit 1
    }

    # 4. Perform the prediction
    echo "--> Performing cluster predictions..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/rafts_pred_algo.py" "${DIR_CONFIG}/${PRED_CONF}" || {
        echo "ERROR: Cluster predictions failed for ${MODEL}. Exiting."
        exit 1
    }

    # 5. Pair the donor-receivers
    echo "--> Performing donor-receiver pairing..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/rafts_pair_donors.py" "${DIR_CONFIG}/${PRED_CONF}" || {
        echo "ERROR: Donor-receiver pairings failed for ${MODEL}. Exiting."
        exit 1
    }

    # 6. Map the predictions
    echo "--> Plotting the cluster predictions on static map..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/rafts_map_pred_hfatl.py" "${DIR_CONFIG}/${PRED_CONF}" || {
        echo "ERROR: Prediction mapping failed for ${MODEL}. Exiting."
        exit 1
    }

    # 6b. Map donor-receiver pairings for a configured region (e.g. a state)
    # No-op (exit 0) unless the model's task_type is 'clustering' (donor-receiver
    # pairing is unsupervised-only, same constraint rafts_pair_donors.py enforces)
    # and its pred_config sets `donor_map_states` -- safe to run unconditionally here.
    echo "--> Mapping donor-receiver pairings (if configured)..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/rafts_map_donor_receiver.py" "${DIR_CONFIG}/${PRED_CONF}" || {
        echo "ERROR: Donor-receiver mapping failed for ${MODEL}. Exiting."
        exit 1
    }

    # 7. Write Parameters to Compiled GeoPackage
    echo "--> Compiling regionalized parameters into master GPKG..."
    uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/rafts_regn_params_gpkg.py" "${DIR_CONFIG}/${PRED_CONF}" || {
        echo "ERROR: GPKG compilation failed for ${MODEL}. Exiting."
        exit 1
    }
done

# -----------------------------------------------------------------------------
# OPTIONAL QA CHECKS
# -----------------------------------------------------------------------------
# Post-hoc sanity checks (scripts/qa/) confirming the attrs<->crosswalk<->SQLite
# divide_id chain has no silent gaps. Diagnostic only -- always exits 0, so it
# never trips this script's `set -e` regardless of what it finds. Run once per
# workflow (not once per model): the compiled SQLite output directory is shared
# across every formulation, so any single model's pred_config covers it. Uses
# PRED_CONF as left by the last loop iteration above.
echo "========================================================================"
echo "Running optional QA checks..."
echo "========================================================================"
"${DIR_REPO}/scripts/qa/run_qa_checks.sh" "${DIR_CONFIG}/${PRED_CONF}"

echo ""
echo "========================================================================"
echo "Running optional CROSSWALK GAP DIAGNOSIS (spatial root-cause analysis)..."
echo "========================================================================"
# Spatially-aware diagnostic: for any HUC12s missing from the crosswalk,
# determines whether a hydrofabric divide actually exists there (actionable
# gap) or if the region is structurally divide-less (water/island/closed basin).
# For actionable gaps, identifies whether the divide was never crosswalked or
# was assigned elsewhere, and validates the assignment against a majority-share rule.
#
# Runs CONUS-wide sizing pass automatically; add --states FL to scope to Florida
# (or any other states), or --states WA OR for multi-state regions.
#
# Outputs CSVs to ~/noaa/regionalization/data/output/analysis/<dataset_name>/.
# Results are diagnostic only (always exit 0) and do not gate the workflow.
# To skip this step, comment it out or pass a different path to regn_all_proc.sh.
"${DIR_REPO}/scripts/qa/crosswalk_gap_diagnosis/run_crosswalk_gap_diagnosis.sh" "${DIR_CONFIG}/${PRED_CONF}"

echo "========================================================================"
echo "SUCCESS: Finished ALL hfATLAS regionalization predictions!"
echo "========================================================================"