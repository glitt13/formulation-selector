import argparse
import yaml
import pandas as pd
from pathlib import Path
import fs_algo.fs_algo_train_eval as fsate
import ast
import numpy as np
import geopandas as gpd
from shapely import wkt
import matplotlib.pyplot as plt

import warnings             

"""Workflow script to generate a map of predictions

:raises ValueError: When the algorithm config file path does not exist
:note python fs_map_pred.py "/path/to/pred_config.yaml" "huc08"

"""

# Predict values and evaluate predictions
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the prediction config file')
    parser.add_argument('path_pred_config', type=str, help='Path to the YAML configuration file specific for prediction.')
    parser.add_argument('analysis_str', type=str, default='') # The string to add on to end of each plotting file name
    # NOTE pred_config should contain the path for path_algo_config
    args = parser.parse_args()

    path_pred_config = Path(args.path_pred_config) #Path(f'~/git/formulation-selector/scripts/eval_ingest/xssangencerf/xssangencerf_pred_config.yaml') 
    analysis_str = args.analysis_str

    pred_cfg = fsate.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()
    
    #%% PREDICTION FILE'S COMIDS (IMPLICIT ASSUMPTION: Each dataset processes the same IDS)
    path_meta_pred = pred_cfg.pred_cfg_dict.get('path_meta')
    comid_pred_col = pred_cfg.pred_cfg_dict.get('pred_file_comid_colname')
    write_type = pred_cfg.pred_cfg_dict.get('write_type')
    ds_type = pred_cfg.pred_cfg_dict.get('ds_type')
    
    #%% prediction config
    resp_vars = pred_cfg.pred_cfg_dict.get('algo_response_vars')
    algos = pred_cfg.pred_cfg_dict.get('algo_type')

    path_meta_pred = pred_cfg.pred_cfg_dict.get('path_meta')
    #%%  READ CONTENTS FROM THE ATTRIBUTE CONFIG
    path_attr_config = fsate.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_attr_config',None))
    path_algo_config = fsate.build_cfig_path(pred_cfg.pred_cfg_dict.get('path_pred_config'),pred_cfg.pred_cfg_dict.get('name_algo_config'))

    with open(path_algo_config, 'r') as file:
        algo_cfg = yaml.safe_load(file)
    # Ensure the string literal is converted to a tuple for `hidden_layer_sizes`
    algo_config = algo_cfg.get('algorithms')
    if algo_config['mlp'][0].get('hidden_layer_sizes',None): # purpose: evaluate string literal to a tuple
        algo_config['mlp'][0]['hidden_layer_sizes'] = ast.literal_eval(algo_config['mlp'][0]['hidden_layer_sizes'])
    algo_config_og = algo_config.copy()

    metrics = algo_cfg.get('metrics',None)

    name_attr_config = algo_cfg.get('name_attr_config', Path(path_algo_config).name.replace('algo','attr')) 
    path_attr_config = fsate.build_cfig_path(path_algo_config, name_attr_config)

    attr_cfig = fsate.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()




    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest

    dirs_std_dict = fsate.fs_save_algo_dir_struct(dir_base)
    dir_out_viz_base = dirs_std_dict.get('dir_out_viz_base')
    dirs_std_dict.get('dir_out')
    dir_out = dirs_std_dict.get('dir_out')

    for ds in datasets: 
        path_pred_locs = fsate.build_pred_locs_path(path_meta_template=path_meta_pred, dir_std_base=dir_std_base, 
                                                    ds=ds,ds_type=ds_type, write_type=write_type)
        comids_pred = fsate._read_pred_comid(path_pred_locs, comid_pred_col )
       

        path_fs_dat_resp =  fsate._std_fs_prep_ds_paths(dir_std_base=dir_std_base,ds=ds,mtch_str='*.nc')
        path_gpkg_fs_prep = fsate._std_fs_prep_ds_companion_gpkg_path(path_fs_dat_resp[0])
        gdf_all = gpd.read_file(path_gpkg_fs_prep)
        gdf_all = gdf_all.rename(columns={'featIDgpkg':'featureID','featSrcegpkg':'featureSource'})

        for metr in resp_vars:
            for algo_str in pred_cfg.pred_cfg_dict.get('algo_type'):
          
                # Read in the prediction file for each response variable
                path_pred_in = fsate.std_pred_path(dir_out=dir_out,algo=algo_str,metric=metr,dataset_id=ds)
        
                df_pred = pd.read_parquet(path_pred_in)

                gdf_pred = gdf_all.merge(df_pred,how='right', right_on='featureID',left_on='comid')

                #gdf_pred = gdf_pred.dropna(subset='gage_id')
                #%% PREDICT                 
                fsate.plot_map_pred_wrap(gdf_pred,
                                dir_out_viz_base, ds,
                                    metr,algo_str,
                                    split_type=analysis_str,
                                    colname_data='prediction')
                            