##!/bin/bash

# RaFTS processing the xSSA process sensitivity dataset with hfATLAS data
# Instructions:
# Make this script executable using: chmod +x xssangencerf_proc_rafts_all.sh
# Run by calling in terminal: ./xssangencerf_proc_rafts_all.sh

# -----------------------------------------------------------------------------
# ERROR HANDLING
# -----------------------------------------------------------------------------
# -e: Exit immediately if a command exits with a non-zero status.
# -u: Treat unset variables as an error and exit immediately.
# -o pipefail: Ensure errors in piped commands are caught.
set -euo pipefail || { echo "..."; exit 1; }

# -----------------------------------------------------------------------------
# PATH DEFINITIONS
# -----------------------------------------------------------------------------
echo "Using system home directory as basis for all paths: $HOME"
DIR_REPO="$HOME/git/formulation-selector"
DIR_CONFIG="${DIR_REPO}/scripts/eval_ingest/xssangencerf_hfa"
DIR_PREP="${DIR_REPO}/pkg/fs_prep/fs_prep/flow"
DIR_PY="${DIR_REPO}/pkg/fs_algo/fs_algo/flow"

echo "Running processing from $DIR_CONFIG"

# -----------------------------------------------------------------------------
# EXECUTION WORKFLOW
# -----------------------------------------------------------------------------

# 1. Prepare the initial dataset
echo "Starting execution of hfATLAS parameter regionalization scripts..."
echo "--> Preparing the initial dataset..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_CONFIG}/prep_xssaus_metrics.py" "${DIR_CONFIG}/xssangencerf_prep_config.yaml" || {
    echo "ERROR: Dataset preparation failed. Exiting."
    exit 1
}

# 2 Aggregate they hydrofabric based on the gage_id (CAMELS basins subset)
echo "--> Aggregating the hydrofabric attributes based on the gage_id (CAMELS basins subset)..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_PREP}/fs_agg_hfatl_basin.py" \
    --path_prep_config "${DIR_CONFIG}/xssangencerf_prep_config.yaml" \
    --path_attr_config "${DIR_CONFIG}/xssangencerf_attr_config.yaml" || {
    echo "ERROR: Hydrofabric aggregation failed. Exiting."
    exit 1
}
echo "Hydrofabric aggregation completed successfully!"  


# 3. Train the algorithms 
echo "--> Training & testing algorithms..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_PY}/fs_proc_algo_pool.py" "${DIR_CONFIG}/xssangencerf_algo_config.yaml" --chunk_size 4 || {
    echo "ERROR: Algorithm training failed. Exiting."
    exit 1
}
echo "Algorithm training completed successfully!"

echo "========================================================================"
echo "SUCCESS: Finished the hfATLAS training of xSSA!"
echo "========================================================================"