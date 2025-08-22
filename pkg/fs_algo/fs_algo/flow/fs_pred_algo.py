"""Perform response variable predictions using trained algorithms that have been trained
using fs_proc_algo_viz.py. Refers to prediction configuration file, which should have also
been used in a user-generated, custom script that generates the predictor data to be used 
in this prediction script.

Usage:
    >>> python fs_pred_algo.py "/path/to/datasetshortname_pred_config.yaml"

# Changelog/Contributions:
2024 Originally created, GL
2025-06-10 Generalize to specify featureID and featureSource columns in the prediction output, GL

"""

import argparse
import joblib
import fs_algo.fs_algo_train_eval as fsate
import pandas as pd
from pathlib import Path
import forestci as fci
from sklearn.model_selection import train_test_split
from logging.handlers import MemoryHandler
import logging
import fs_prep.proc_eval_metrics as pem

# Predict values and evaluate predictions
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the prediction config file')
    parser.add_argument('path_pred_config', type=str, help='Path to the YAML configuration file specific for prediction.')
    #parser.add_argument('path_algo_config', type=str, help='Path to the YAML configuration file specific for algorithm training')
    # NOTE pred_config should contain the path for path_algo_config
    args = parser.parse_args()

    path_pred_config = Path(args.path_pred_config).expanduser() # Path(f'~/git/formulation-selector/scripts/workflow_configs/legacy/xssa/xssa_pred_config.yaml') 
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
    pred_cfg = fsate.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()

    #%%  READ CONTENTS FROM THE ATTRIBUTE CONFIG
    path_attr_config = fsate.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_attr_config'))
    path_algo_config = fsate.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_algo_config'))
    
    # READ fs_categories_uncn.yaml
    print("Reading uncertainty bounds from fs_categories_uncn.yaml...")
    uncn_config = pem._read_std_config_uncn()
    fs_catg_uncn = pem._conv_ls_dicts_df_long_uncn(uncn_config)
    print("Successfully loaded uncertainty bounds.")

    attr_cfig = fsate.AttrConfigAndVars(path_attr_config)
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
    algo_cfig = fsate.AlgoConfigParser(path_algo_config)
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
    attrs_sel = fsate._id_attrs_sel_wrap(attr_cfig=attr_cfig,
                    path_cfig=path_attr_config,
                    name_attr_csv = name_attr_csv,
                    colname_attr_csv = colname_attr_csv)

    #%% ESTABLISH ALGORITHM FILE I/O
    dir_out = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out')
    dir_out_alg_base = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out_alg_base')
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
        path_pred_locs = fsate.build_pred_locs_path(path_meta_template=path_meta_pred, dir_std_base=dir_std_base, 
                                                    ds=ds,ds_type=ds_type, write_type=write_type)

        comids_pred = fsate._read_pred_comid(path_pred_locs, comid_pred_col )

        #%%  Read in predictor variable data (aka basin attributes) 
        # Read the predictor variable data (basin attributes) generated by proc.attr.hydfab
        df_attr = fsate.fs_read_attr_comid(dir_db_attrs, comids_pred, attrs_sel = attrs_sel,
                                           read_type = 'filename', # 'filename' tends to be the fastest (2025-06-01)
                                        _s3 = None,storage_options=None)
        df_attr = df_attr.drop(columns='dl_timestamp')
        # Constrain the values in the value column to two digits after the decimal point (to help ID duplicates)
        df_attr['value'] = df_attr['value'].apply(lambda x: round(x, 2))

        # Drop any duplicate rows
        df_attr.drop_duplicates(inplace=True)

        # Reset the index the dataframe
        df_attr.reset_index(inplace=True)

        # Remove the old index column
        df_attr.drop(columns=['index'], inplace=True)

        new_df_attr = df_attr[['featureID', 'attribute', 'value']]
        # Convert into wide format for model training
        df_attr_wide = new_df_attr.pivot(index='featureID', columns = 'attribute', values = 'value')

        map_feat_srce_feat_id = df_attr[['featureID','featureSource']].drop_duplicates()

        # Run predictions & save output
        dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))
        logging.info(f"PREDICTING algorithm for {ds}")
        for resp_var in resp_vars:
            for algo in algos:
                path_algo = fsate.std_algo_path(dir_out_alg_ds, algo=algo, metric=resp_var, dataset_id=ds)
                if not Path(path_algo).exists():
                    msg_nonexst = f"The following algorithm path does not exist: \n{path_algo}"
                    logging.error(msg_nonexst)
                    raise FileNotFoundError(msg_nonexst)

                # Read in the algorithm's pipelin
                pipeline_data = joblib.load(path_algo)
                
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
        
                    # Rename columns based on self.mapie_alpha values
                    for i, alpha in enumerate(mapie_alpha):
                        df_pred[f'mapie_lower_{alpha:.2f}'] = y_pis[:, 0, i]
                        df_pred[f'mapie_upper_{alpha:.2f}'] = y_pis[:, 1, i]
                elif mapie_alpha and 'mapie' not in pipeline_data:
                    logging.warning("MAPIE prediction interval estimation is not available in the " \
                    "trained algorithm pipeline, but mapie_alpha is specified in the prediction config file." \
                    "If prediction uncertainty desired, re-run the algorithm training fs_proc_algo_viz.py, " \
                    "with mapie specified in the Uncertainty section of the algo config file.")

                path_pred_out = fsate.std_pred_path(dir_out,algo=algo,metric=resp_var,dataset_id=ds)

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
                    if col.startswith('forest_ci') or col.startswith('mapie_')
                ])
                
                # Define the trailing metadata columns
                meta_cols = ['resp_var', 'dataset', 'algo', 'name_algo']
                
                # Combine all column lists for the final order
                final_col_order = col_order + uncertainty_cols + meta_cols
                
                # Reorder the dataframe
                df_pred_mrge = df_pred_mrge[final_col_order]                

                # Write prediction results
                df_pred_mrge.to_parquet(path_pred_out)
                logging.info(f"   Completed {algo} prediction of {resp_var}")
    logging.info(f"FINISHED algorithm prediction for {path_pred_config.name}")
    logging.shutdown()
