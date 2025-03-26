# RaFTS processing the xssaus dataset, from data acquisition to 

# Instructions:
# Make this script executable using
# chmod +x xssaus_proc_rafts_all.sh
# Run by calling in terminal ./xssaus_proc_rafts_all.sh

#!/bin/bash
echo "Using system home directory as basis for all paths: $HOME"
DIR_REPO="$HOME/git/formulation-selector/" # The system-specific path to the formulation-selector repo
DIR_CONFIG="${DIR_REPO}/scripts/eval_ingest/xssa_us/"
DIR_PY="${DIR_REPO}/pkg/fs_algo/fs_algo/"
DIR_R="${DIR_REPO}/pkg/proc.attr.hydfab/flow/"

echo "Running processing from $DIR_CONFIG"

# Prepare the initial dataset here (only needs to happen once)
# "${DIR_CONFIG}prep_xssaus_metrics.py" "${DIR_CONFIG}xssaus_prep_config.yaml"

# Print a message to indicate the script is running
echo "Starting execution of xssa_us process sensitivity scripts..."

# Run the R script to grab attributes
echo "Grabbing attributes"
Rscript "${DIR_R}fs_attrs_grab.R" "${DIR_CONFIG}xssaus_attr_config.yaml"
echo "Attribute grabbing completed!"

# Run attribute transformer
echo "Generating transformed attributes"
python3 "${DIR_PY}fs_tfrm_attrs.py" "${DIR_CONFIG}xssaus_attrs_tform.yaml"
echo "Attribute transformations completed!"

# Train the algorithms
echo "Training & testing algorithms..."
python3 "${DIR_PY}fs_proc_algo_viz.py" "${DIR_CONFIG}xssaus_attr_config.yaml"
echo "Algorithm training completed!"

# Print a message to indicate all scripts have finished executing
echo "Attribute grabbing, transformation, and algorithm training executed successfully!"