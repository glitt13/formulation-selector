"""Perform response variable predictions using trained algorithms that have been trained
using fs_proc_algo_viz.py. Refers to prediction configuration file, which should have also
been used in a user-generated, custom script that generates the predictor data to be used 
in this prediction script.

Usage:
    >>> python fs_pred_algo.py "/path/to/datasetshortname_pred_config.yaml"

# Changelog/Contributions:
2024 Originally created, GL
2025-06-10 Generalize to specify featureID and featureSource columns in the prediction output, GL
2025-10-10 refactor to renamed fs_algo modules, GL
2025-11-20 Integrated validation using schemas.py and pydantic_schemas.py, Soroush Sorourian with the help of AI.
2025-11-25 refactor: move schemas to package structure and adjust import logic, [Soroush Sorourian/AI]
2025-11-26 feat: Dynamically load valid metrics for schema validation, Soroush Sorourian
2025-11-26 refactor: Replaced dynamic metric loading logic with fs_algo.utils.get_valid_metrics, Soroush Sorourian
2025-12-01 refactor: Formalized loading, processing, and writing into utility functions, Soroush Sorourian with the help of AI.
"""

import argparse
import joblib
import fs_algo.utils as fsutil
import pandas as pd
from pathlib import Path
import forestci as fci
from sklearn.model_selection import train_test_split
from logging.handlers import MemoryHandler
import logging
import fs_prep.proc_eval_metrics as pem
import numpy as np

# Imports for validation
import importlib.util
import sys

# from fs_algo.schemas.pydantic_schemas import ModelMetadata # Used to validate loaded pipeline
# import fs_algo.schemas.schemas as schemas 

# import warnings
# import pandera as pa
# from pandera import Column, DataFrameSchema, Index, Check
# from typing import List, Dict
    
# Predict values and evaluate predictions
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the prediction config file')
    parser.add_argument('path_pred_config', type=str, help='Path to the YAML configuration file specific for prediction.')
    #parser.add_argument('path_algo_config', type=str, help='Path to the YAML configuration file specific for algorithm training')
    # NOTE pred_config should contain the path for path_algo_config
    args = parser.parse_args()

    path_pred_config = Path(args.path_pred_config).expanduser() # Path(f'~/git/formulation-selector/scripts/eval_ingest/xssa/xssa_pred_config.yaml').expanduser()     
    
    config_dir = path_pred_config.parent    
    arg_val = False # Default validation flag

    # Call the new utility function to load metrics based on the pred config path
    valid_metrics = fsutil.get_valid_metrics(path_pred_config)
    
    # --- Commence logging before creating the log file
    memory_handler = MemoryHandler(capacity=30)
    # Get the root logger and add the memory handler to it
    # The root logger is the ancestor of all other loggers
    root_logger = logging.getLogger()
    root_logger.addHandler(memory_handler)
    root_logger.setLevel(logging.INFO) # Set the level to capture INFO messages
    logging.info(f"Running fs_pred_algo.py with \
                {path_pred_config.parent / path_pred_config.name} config file")
    # ---
    
    # --- Conditionally load schemas ---
    if args.validate:
        arg_val = True
        # schema_file = config_dir / "schemas.py"
    
        # if not schema_file.exists():
        #     # Fallback logic for schema file not found
        #     logging.error(f"No schema file found at expected location: {schema_file}")
        #     raise FileNotFoundError(f"No schema file found at expected location: {schema_file}")
    
        # # Dynamically import schemas.py
        # logging.info(f"Loading schemas from {schema_file}")
        # spec = importlib.util.spec_from_file_location("schemas", str(schema_file))
        # schemas = importlib.util.module_from_spec(spec)
        # sys.modules["schemas"] = schemas
        # spec.loader.exec_module(schemas)
        # logging.info("✅ Schemas loaded successfully.")
        logging.info("Schema validation enabled. Using statically imported schemas from fs_algo.schemas.")
        
    pred_cfg = fsutil.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()
    
    # READ fs_categories.yaml if uncn_bnd_pred is True
    uncn_bnd_pred = pred_cfg.pred_cfg_dict.get('uncn_bnd_pred')
    logging.info("Reading uncertainty bounds from fs_categories.yaml...")

    fs_catg_uncn = pem._conv_ls_dicts_df_long()
    logging.info("Successfully loaded uncertainty bounds.")

    #%%  READ CONTENTS FROM THE ATTRIBUTE CONFIG
    path_attr_config = fsutil.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_attr_config'))
    path_algo_config = fsutil.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_algo_config'))

    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest

    # ---------- Generate path to the log file & initialize logging -----------
    path_log = pem.std_path_log(dir_input=dir_base, 
                                path_config=path_pred_config,
                            script='fs_pred_algo')
    logging.basicConfig(level=logging.INFO, 
                        filename=path_log, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filemode='w', 
                        force = True) # overwrite log file when force=T

    # We need to find the new FileHandler that basicConfig created and set it
    # as the target for our MemoryHandler - aka we can now put previous logs
    # into the log file now that it has been created
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            memory_handler.setTarget(handler)
            memory_handler.flush()
            break  
    logging.info(f"Writing logs to {path_log}")
    root_logger.removeHandler(memory_handler) # Remove the pre-file logger
    # -------------------------------------------------------------------------

    # Initialize algo configuration class for extracting attributes
    algo_cfig = fsutil.AlgoConfigParser(path_algo_config)
    algo_cfig._read_algo_config()

    name_attr_csv = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["name_attr_csv"]
    colname_attr_csv = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["colname_attr_csv"]

    # Determine whether random forest confidence intervals computed during model training:
    forestci = algo_cfig.algo_cfg_unc_dict["algo_unc_dict"]["uncertainty_cfg"].get('forestci',{})
    if len(forestci)>0:
        fci_flag = forestci[0].get('fci_flag',False)
    else:
        fci_flag = False
    # Attributes needed for prediction:
    attrs_sel = fsutil._id_attrs_sel_wrap(attr_cfig=attr_cfig,
                    path_cfig=path_attr_config,
                    name_attr_csv = name_attr_csv,
                    colname_attr_csv = colname_attr_csv)

    #%% ESTABLISH ALGORITHM FILE I/O
    dir_out = fsutil.fs_save_algo_dir_struct(dir_base).get('dir_out')
    dir_out_alg_base = fsutil.fs_save_algo_dir_struct(dir_base).get('dir_out_alg_base')
    #%% PREDICTION FILE'S COMIDS (IMPLICIT ASSUMPTION: Each dataset processes the same IDS)
    path_meta_pred = pred_cfg.pred_cfg_dict.get('path_meta')
    comid_pred_col = pred_cfg.pred_cfg_dict.get('pred_file_comid_colname')
    write_type = pred_cfg.pred_cfg_dict.get('write_type')
    ds_type = pred_cfg.pred_cfg_dict.get('ds_type')
    
    #%% prediction config
    resp_vars = pred_cfg.pred_cfg_dict.get('algo_response_vars')
    algos = pred_cfg.pred_cfg_dict.get('algo_type')

    #%% Run prediction
    for ds in datasets:
        # f-string formatting of the attribute metadata's filepath
        path_pred_locs = fsutil.build_pred_locs_path(path_meta_template=path_meta_pred, dir_std_base=dir_std_base, 
                                                    ds=ds,ds_type=ds_type, write_type=write_type)

        comids_pred = fsutil._read_pred_comid(path_pred_locs, comid_pred_col )

        #%%  Read in predictor variable data (aka basin attributes) 
        # Read and validate the predictor variable data (basin attributes) generated by proc.attr.hydfab
        df_attr = fsutil.read_validated_input_attributes(
            dir_db_attrs=dir_db_attrs, 
            comids_pred=comids_pred, 
            attrs_sel=attrs_sel, 
            read_type='filename',
            arg_val=arg_val
        )

        new_df_attr = df_attr[['featureID', 'attribute', 'value']]
        # Convert into wide format for model training
        df_attr_wide = new_df_attr.pivot(index='featureID', columns = 'attribute', values = 'value')

        map_feat_srce_feat_id = df_attr[['featureID','featureSource']].drop_duplicates()

        # Run predictions & save output
        dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))
        logging.info(f"PREDICTING algorithm for {ds}")
        for resp_var in resp_vars:
            min_lim = None
            max_lim = None
            metric_bounds = fs_catg_uncn[fs_catg_uncn['var'] == resp_var]            
            if not metric_bounds.empty:
                min_lim = metric_bounds['min_lim'].iloc[0]
                max_lim = metric_bounds['max_lim'].iloc[0]
                logging.warning(f"   Applying bounds for '{resp_var}': min={min_lim}, max={max_lim}")
            else:
                logging.warning(f"   No bounds found for '{resp_var}'. Predictions will not be clipped.")

            for algo in algos:
                path_algo = fsutil.std_algo_path(dir_out_alg_ds, algo=algo, metric=resp_var, dataset_id=ds)
                
                # --- Pipeline loading and validation ---
                pipeline_data = fsutil.load_validated_pipeline(path_algo, arg_val=arg_val)
                
                pipe = pipeline_data['pipeline']
                X_train_shape = pipeline_data['X_train_shape']  # Retrieve X_train.shape


                feat_names = list(pipe.feature_names_in_)
                df_attr_sub = df_attr_wide[feat_names]

                # Remove na values
                df_attr_sub_rmna = df_attr_sub.dropna()
                if df_attr_sub_rmna.shape[0] < df_attr_sub.shape[0]:

                    ids_na = set(df_attr_sub.index) - set(df_attr_sub_rmna.index)
                    text_join = '\n'.join(ids_na)
                    msg_rm_na = f"Removing the following featureIDs from prediction due " + \
                     f"to NA values:\n{text_join}"
                    logging.warning(msg_rm_na)

                # Perform prediction
                resp_pred = pipe.predict(df_attr_sub_rmna)
                # Unconditionally warn if any predictions fall out of the physical range.
                fsutil._warn_if_out_of_bounds(
                    predictions=resp_pred,
                    feature_ids=df_attr_sub_rmna.index,
                    min_lim=min_lim,
                    max_lim=max_lim,
                    resp_var=resp_var,
                    correction_is_active=uncn_bnd_pred,
                    prediction_type="values"
                )
                # --- Apply bounds to the primary prediction value (resp_pred) ---
                if uncn_bnd_pred:
                    resp_pred = fsutil.clip_predictions(resp_pred, min_lim, max_lim)

                # Initialize DataFrame for storing results
                df_pred = pd.DataFrame({'featureID': df_attr_sub_rmna.index, 'prediction': resp_pred, 'resp_var': resp_var, 'dataset': ds, 'algo': algo, 'name_algo': Path(path_algo).name})
        
                # If using RandomForest, calculate confidence intervals using forestci
                if algo == 'rf' and forestci:
                    rf_algo = pipe.named_steps['randomforestregressor']  # Use the correct step name
                    forest_ci = fci.random_forest_error(forest=rf_algo, X_train_shape=X_train_shape, X_test=df_attr_sub_rmna.to_numpy())
                    df_pred['forestci'] = forest_ci
        
                # If MAPIE is available, compute prediction intervals
                mapie_alpha = pred_cfg.pred_cfg_dict.get('mapie_alpha')
                if 'mapie' in pipeline_data and mapie_alpha:
                    mapie = pipeline_data['mapie']
                    y_pred_mapie, y_pis = mapie.predict(df_attr_sub_rmna, alpha=mapie_alpha)
        
                    fsutil._warn_if_out_of_bounds(
                        predictions=y_pis,
                        feature_ids=df_attr_sub_rmna.index,
                        min_lim=min_lim,
                        max_lim=max_lim,
                        resp_var=resp_var,
                        correction_is_active=uncn_bnd_pred,
                        prediction_type="intervals"
                    )
                    # Apply bounds if the flag was set and bounds were found
                    if uncn_bnd_pred and (min_lim is not None or max_lim is not None):
                        y_pis = fsutil.clip_pis(y_pis, min_lim, max_lim)
                        
                    # Rename columns based on self.mapie_alpha values
                    for i, alpha in enumerate(mapie_alpha):
                        df_pred[f'mapie_lower_{alpha:.2f}'] = y_pis[:, 0, i]
                        df_pred[f'mapie_upper_{alpha:.2f}'] = y_pis[:, 1, i]
                elif mapie_alpha and 'mapie' not in pipeline_data:
                    logging.warning("MAPIE prediction interval estimation is not available in the " \
                    "trained algorithm pipeline, but mapie_alpha is specified in the prediction config file." \
                    "If prediction uncertainty desired, re-run the algorithm training fs_proc_algo_viz.py, " \
                    "with mapie specified in the Uncertainty section of the algo config file.")

                path_pred_out = fsutil.std_pred_path(dir_out,algo=algo,metric=resp_var,dataset_id=ds)

                # Update prediction file with the featureID-featureSource mapping (and NA-filling)             
                df_pred_mrge = pd.merge(df_pred, map_feat_srce_feat_id, how='right', on='featureID')
                df_pred_mrge.fillna(value={'resp_var': resp_var,'dataset': ds, 'algo': algo,
                                            'name_algo':Path(path_algo).name},inplace=True)
                # col_order = ['featureID', 'featureSource', 'prediction', 'resp_var', 'dataset', 'algo', 'name_algo']
                # df_pred_mrge = df_pred_mrge[col_order]
                col_order = ['featureID', 'featureSource', 'prediction']
                
                # Find and sort any uncertainty columns that were added to the dataframe
                uncertainty_cols = sorted([
                    col for col in df_pred_mrge.columns 
                    if col.startswith('forestci') or col.startswith('mapie_')
                ])
                
                # Define the trailing metadata columns
                meta_cols = ['resp_var', 'dataset', 'algo', 'name_algo']
                
                # Combine all column lists for the final order
                final_col_order = col_order + uncertainty_cols + meta_cols
                
                # Reorder the dataframe
                df_pred_mrge = df_pred_mrge[final_col_order]

                # --- Validation and file writing of the output dataframe ---
                fsutil.write_validated_prediction_output(
                    df_pred_mrge=df_pred_mrge, 
                    path_pred_out=path_pred_out, 
                    arg_val=arg_val, 
                    valid_metrics=valid_metrics, 
                    mapie_alpha=mapie_alpha
                )
                
                logging.info(f"   Completed {algo} prediction of {resp_var}")
    logging.info(f"FINISHED algorithm prediction for {path_pred_config.name}")
    logging.shutdown()
