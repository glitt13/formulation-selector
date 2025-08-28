@echo off
REM RaFTS processing the xssaus dataset, aka the Mai et al 2022 Process
REM Sensitivities, specific to CONUS locations. This is specific to the uncertainty
REM quantification approach first tested in 2025 Aug.

REM Instructions:
REM 1. Save this file as "xssaus_proc_rafts_all_uncn.bat"
REM 2. Open an Anaconda Prompt.
REM 3. Navigate to the directory where you saved this file.
REM 4. Run the script by typing its name and pressing Enter: xssaus_proc_rafts_all_uncn.bat

echo "Using system home directory as basis for all paths: %USERPROFILE%"

REM --- Set Directory Paths ---
REM The system-specific path to the formulation-selector repo
set "DIR_REPO=%USERPROFILE%\git\formulation-selector\"
set "DIR_CONFIG=%DIR_REPO%scripts\eval_ingest\xssa_us\"
set "DIR_PRED=%DIR_REPO%scripts\prediction\rfc_locs\"
set "DIR_PY=%DIR_REPO%pkg\fs_algo\fs_algo\flow\"
set "DIR_R=%DIR_REPO%pkg\proc.attr.hydfab\flow\"

echo "Running processing from %DIR_CONFIG%"

REM --- Prepare the initial dataset here (only needs to happen once) ---
python "%DIR_CONFIG%prep_xssaus_metrics.py" "%DIR_CONFIG%xssaus_prep_config.yaml"

REM --- Start Main Processing ---
echo "Starting execution of xssa_us process sensitivity scripts..."

REM Run the R script to grab attributes
echo "Grabbing attributes"
Rscript "%DIR_R%fs_attrs_grab.R" "%DIR_CONFIG%xssaus_attr_config.yaml"
echo "Attribute grabbing completed!"

REM Run attribute transformer
echo "Generating transformed attributes"
python "%DIR_PY%fs_tfrm_attrs.py" "%DIR_CONFIG%xssaus_attrs_tform.yaml"
echo "Attribute transformations completed!"

REM Train the algorithms
echo "Training & testing algorithms..."
python "%DIR_PY%fs_proc_algo_viz.py" "%DIR_CONFIG%xssaus_algo_config_uncn.yaml"
echo "Algorithm training completed!"

echo "Attribute grabbing, transformation, and algorithm training executed successfully!"

REM --- Prediction Steps ---

REM 4.1 Identify which locations will be used for prediction, and generate the attributes
echo "Retrieve prediction location attribute data and geometry data"
REM CAUTION: The following Rscript has some custom dependencies in identifying which locations need predicting. Refer to script for details.
Rscript "%DIR_PRED%gen_pred_locs_xssaus_map.R" "%DIR_CONFIG%xssaus_pred_config_uncn.yaml"
echo "Acquired prediction location attribute data and geometry data"

REM 4.2 Perform transformations on prediction locations
echo "Transforming prediction location attribute data"
python "%DIR_PY%fs_tfrm_attrs.py" "%DIR_CONFIG%xssaus_attrs_tform.yaml"
echo "Transformed prediction location attribute data"

REM 4.3 Perform the prediction
echo "Performing process predictions"
python "%DIR_PY%fs_pred_algo.py" "%DIR_CONFIG%xssaus_pred_config_uncn.yaml"

REM 4.4 Map the predictions (static map)
echo "Plotting the process predictions on static map"
python "%DIR_CONFIG%fs_proc_viz_xssaus.py" "%DIR_CONFIG%xssaus_pred_config_uncn.yaml"
echo "Completed prediction mapping"

echo "Finished the xSSA process sensitivity mapping predictions"

pause
