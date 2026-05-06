"""Workflow script to generate a map of predictions

Make sure to add in a unique string pertaining to the prediction of interest!
Example: 
    >>> python fs_map_pred.py "/path/to/pred_config.yaml" "huc08"

# Changelog/contributions
    2025-08-21 added logging, GL
    2025-10-10 refactor to renamed fs_algo modules, GL
    2026-05-01 adapted to support dynamic ID joins for hfATLAS and mapie uncertainties
"""
import argparse
import pandas as pd
from pathlib import Path
import fs_algo.utils as fsutil
import fs_algo.plots as fsplot
import geopandas as gpd
import logging
from logging.handlers import MemoryHandler
import fs_prep.proc_eval_metrics as pem
import sys

# Predict values and evaluate predictions
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the prediction config file')
    parser.add_argument('path_pred_config', type=str, help='Path to the YAML configuration file specific for prediction.')
    parser.add_argument('analysis_str', type=str, default='') # The string to add on to end of each plotting file name
    args = parser.parse_args()

    path_pred_config = Path(args.path_pred_config).expanduser() 
    
    # --- Commence logging before creating the log file
    memory_handler = MemoryHandler(capacity=30)
    root_logger = logging.getLogger()
    root_logger.addHandler(memory_handler)
    root_logger.setLevel(logging.INFO) 
    logging.info(f"Running fs_map_pred.py with {path_pred_config.name} config file")
    # ---
    
    analysis_str = args.analysis_str

    pred_cfg = fsutil.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()
    
    #%% PREDICTION FILE'S COMIDS
    #path_meta_pred = pred_cfg.pred_cfg_dict.get('path_meta')
    comid_pred_col = pred_cfg.pred_cfg_dict.get('pred_file_comid_colname')
    write_type = pred_cfg.pred_cfg_dict.get('write_type')
    ds_type = pred_cfg.pred_cfg_dict.get('ds_type')
    
    #%% prediction config
    resp_vars = pred_cfg.pred_cfg_dict.get('algo_response_vars')
    algos = pred_cfg.pred_cfg_dict.get('algo_type')

    #%%  READ CONTENTS FROM THE ATTRIBUTE CONFIG
    path_attr_config = fsutil.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_attr_config',None))
    path_algo_config = fsutil.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_algo_config'))

    algo_cfig = fsutil.AlgoConfigParser(path_algo_config)
    algo_cfig._read_algo_config()
    algo_config = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["algo_config"]
    metrics = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["metrics"]

    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets')

    dirs_std_dict = fsutil.fs_save_algo_dir_struct(dir_base)
    dir_out_viz_base = dirs_std_dict.get('dir_out_viz_base')
    dir_out = dirs_std_dict.get('dir_out')

    # ---------- Generate path to the log file & initialize logging -----------
    path_log = pem.std_path_log(dir_input=dir_base, path_config=path_pred_config, script='fs_map_pred')
    logging.basicConfig(level=logging.INFO, filename=path_log, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', force=True)

    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            memory_handler.setTarget(handler)
            memory_handler.flush()
            
    logging.info(f"Writing logs to {path_log}")
    root_logger.removeHandler(memory_handler) 
    # -------------------------------------------------------------------------
    
    for ds in datasets: 
        print(f"Mapping predictions for {ds} dataset")
        #path_pred_locs = fsutil.build_pred_locs_path(path_meta_template=path_meta_pred, dir_std_base=dir_std_base, ds=ds,ds_type=ds_type, write_type=write_type)
        #comids_pred = fsutil._read_pred_comid(path_pred_locs, comid_pred_col)

        path_fs_dat_resp =  fsutil._std_fs_prep_ds_paths(dir_std_base=dir_std_base,ds=ds,mtch_str='*.nc')
        path_gpkg_fs_prep = fsutil._std_fs_prep_ds_companion_gpkg_path(path_fs_dat_resp[0])
        gdf_all = gpd.read_file(path_gpkg_fs_prep)
        
        vals = {'dir_std_base':dir_std_base,'ds':ds}
        dir_db_attrs = Path(str(dir_db_attrs).format(**vals))

        # Keep original columns, but ensure we have standard names for plotting logic
        if 'featureID' not in gdf_all.columns and 'comid' in gdf_all.columns:
            gdf_all['featureID'] = gdf_all['comid']

        for metr in resp_vars:
            for algo_str in pred_cfg.pred_cfg_dict.get('algo_type'):
                logging.info(f"Generating prediction map for dataset: {ds}\nAlgorithm: {algo_str}\nResponse variable: {metr}")
                
                path_pred_in = fsutil.std_pred_path(dir_out=dir_out,algo=algo_str,metric=metr,dataset_id=ds)
                
                if not Path(path_pred_in).exists():
                    logging.warning(f"Prediction file not found: {path_pred_in}. Skipping.")
                    continue
                    
                df_pred = pd.read_parquet(path_pred_in)

                # --- DYNAMIC JOIN LOGIC FOR HFATLAS / COMID COMPATIBILITY ---
                possible_joins = [('featureID', 'featureID'), ('divide_id', 'featureID'), ('comid', 'featureID')]
                join_col_gpkg = None
                
                for gpkg_c, pred_c in possible_joins:
                    if gpkg_c in gdf_all.columns and pred_c in df_pred.columns:
                        join_col_gpkg = gpkg_c
                        #break
                        
                if not join_col_gpkg:
                    logging.error(f"Could not find matching ID columns to join GPKG {gdf_all.columns.tolist()} and Preds {df_pred.columns.tolist()}")
                    continue

                # Type-cast to string to prevent silent merge failures
                gdf_all[join_col_gpkg] = gdf_all[join_col_gpkg].astype(str)
                df_pred['featureID'] = df_pred['featureID'].astype(str)

                # Merge
                gdf_pred = gdf_all.merge(df_pred, how='inner', left_on=join_col_gpkg, right_on='featureID')
                
                if gdf_pred.empty:
                    logging.error("Merge resulted in an empty GeoDataFrame. IDs did not match.")
                    continue

                #%% PREDICT RAW VALUES
                logging.info(f"Plotting predictions inside {dir_out_viz_base}")
                fsplot.plot_map_pred_wrap(
                    test_gdf=gdf_pred,
                    dir_out_viz_base=dir_out_viz_base, 
                    ds=ds,
                    metr=metr,
                    algo_str=algo_str,
                    split_type=analysis_str,
                    colname_data='prediction',
                    epsg_reproj=4326
                )
                
                #%% PREDICT UNCERTAINTIES (If MAPIE Alpha columns exist)
                mapie_cols = [col for col in gdf_pred.columns if col.startswith('mapie_lower_')]
                if mapie_cols:
                    for l_col in mapie_cols:
                        alpha_val = float(l_col.split('_')[-1])
                        u_col = f"mapie_upper_{alpha_val:.2f}"
                        
                        if u_col in gdf_pred.columns:
                            logging.info(f"Generating MAPIE uncertainty map for alpha={alpha_val}")
                            # Calculate the interval range to visualize uncertainty spread
                            gdf_pred[f'uncn_range_{alpha_val}'] = gdf_pred[u_col] - gdf_pred[l_col]
                            
                            fsplot.plot_map_pred_wrap(
                                test_gdf=gdf_pred,
                                dir_out_viz_base=dir_out_viz_base, 
                                ds=ds,
                                metr=f"{metr}_uncertainty",
                                algo_str=algo_str,
                                split_type=f"{analysis_str}_alpha{alpha_val}",
                                colname_data=f'uncn_range_{alpha_val}'
                            )

        logging.info(f"Completed prediction map generation for {path_pred_config}")
    logging.shutdown()