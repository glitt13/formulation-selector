import argparse
import yaml
import pandas as pd
from pathlib import Path
import fs_algo.utils as fsutil
import fs_algo.plots as fsplot
import ast
import numpy as np
import geopandas as gpd
from shapely import wkt
"""Post-training/testing script that plots comparisons of test results

fs_proc_algo_viz.py must be run first for this to work

:raises ValueError: When the algorithm config file path does not exist
:note python fs_proc_algo.py "/path/to/algo_config.yaml"

Usage:
python fs_proc_viz_best_ealstm.py "~/git/formulation-selector/scripts/eval_ingest/ealstm/ealstm_algo_config.yaml"

Changelog/contributions
2024 Originally created, GL
2025-10-10 refactor to renamed fs_algo modules, GL
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the algorithm config file')
    parser.add_argument('path_algo_config', type=str, help='Path to the YAML configuration file specific for algorithm training')
    args = parser.parse_args()

    path_algo_config = Path(args.path_algo_config) #Path(f'~/git/formulation-selector/scripts/eval_ingest/xssa/xssa_algo_config.yaml') 

    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    algo_cfig = fsutil.AlgoConfigParser(path_algo_config)
    # Extract variables from dictionary created by AlgoConfigParser
    algo_config = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["algo_config"]
    
    # Generate variable algo_config_og
    algo_config_og = algo_config.copy()

    verbose = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["verbose"]
    test_size = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["test_size"]
    seed = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["seed"]
    read_type = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["read_type"]
    metrics = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["metrics"]
    make_plots = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["make_plots"]
    same_test_ids = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["same_test_ids"]

    metrics_compare = ['NNSE'] # TODO define the metrics of interest for comparison. This requires evaluating the results from fs_proc_algo_viz.py to determine which models are reasonable.

    #%% Attribute configuration
    path_attr_config = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["path_attr_config"]
    
    print("BEGINNING metric intercomparison among locations.")

    # Initialize attribute configuration class for extracting attributes
    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    # Grab the attributes of interest from the attribute config file,
    #  OR a .csv file if specified in the algo config file.
    name_attr_csv = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["name_attr_csv"]
    colname_attr_csv = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["colname_attr_csv"]
    attrs_sel = fsutil._id_attrs_sel_wrap(attr_cfig=attr_cfig,
                    path_cfig=path_attr_config,
                    name_attr_csv = name_attr_csv,
                    colname_attr_csv = colname_attr_csv)
    
    # Define directories/datasets from the attribute config file
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest

    #%%  Generate standardized output directories
    dirs_std_dict = fsutil.fs_save_algo_dir_struct(dir_base)
    dir_out = dirs_std_dict.get('dir_out')
    dir_out_alg_base = dirs_std_dict.get('dir_out_alg_base')
    dir_out_anlys_base = dirs_std_dict.get('dir_out_anlys_base')
    dir_out_viz_base = dirs_std_dict.get('dir_out_viz_base')

    if same_test_ids:
        # Must first establish which comids to use in the train-test split
        split_dict = fsutil.split_train_test_comid_wrap(dir_std_base=dir_std_base, 
                    datasets=datasets, path_attr_config=path_attr_config,
                    comid_col='comid', test_size=test_size,
                    random_state=seed)
        # If we use all the same comids for testing, we can make inter-comparisons
        test_ids = split_dict.get('sub_test_ids',None) #If this returns None, we use the test_size for all data
    else:
        test_ids = None


    #%% Cross-comparison across all datasets: determining where the best metric lives
    # The dataframe dtype structure generated in fs_proc_algo_viz.py as df_pred_obs_ds_metr
    dtype_dict = {'metric': 'str', 'comid': 'str', 'gage_id': 'str',
                'dataset':'str','algo':'str','performance':'float',
                'observed':'float'}        
    dict_pred_obs_ds = dict()
    for ds in datasets:
        for metr in metrics:
            path_pred_obs = fsutil.std_test_pred_obs_path(dir_out_anlys_base,ds, metr)
            ds_metr_str = f"{ds}_{metr}"
            try:
                df = pd.read_csv(path_pred_obs, dtype=dtype_dict)
                df['geometry'] = df['geometry'].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df,geometry = 'geometry', crs = '4326')
                dict_pred_obs_ds[ds_metr_str] = gdf
            except:
                print(f"Skipping {ds_metr_str}")
                continue
    
    df_pred_obs_all = pd.concat(dict_pred_obs_ds)

    #%% CUSTOM MUNGING
    df_pred_obs_all['name'] = df_pred_obs_all['dataset'].str.replace('kratzert19_','')

    # Simplify all lstms to just 'lstm'
    df_pred_obs_all['name_lstm'] = df_pred_obs_all['name']
    df_pred_obs_all['name_lstm']= df_pred_obs_all['name'].apply(lambda x: 'lstm' if 'lstm' in x else x)

    # Subset to  the NSE-optimized lstms
    df_pred_obs_sub = df_pred_obs_all[df_pred_obs_all['name'].isin(['SAC_SMA', 'lstm_NSE', 'ealstm_NSE',
       'lstm_no_static_NSE', 'mHm_basin', 'q_sim_fuse_904',
        'HBV_ub', 'VIC_basin'])]

    # TODO which metrics best when using idxmax()?
    # TODO which metrics are allowed to be predicted based on evaluation criteria?
    #%% Generate comparison plot
    for metr in metrics_compare:
        df_pred_obs_metr = df_pred_obs_all[df_pred_obs_all['metric']==metr]
        best_df = df_pred_obs_metr.loc[df_pred_obs_metr.groupby(['comid'])['performance'].idxmax()]
        for ds in datasets:
            # Save the same plot in every dataset subdirectory
            fsplot.plot_best_algo_wrap(best_df, dir_out_viz_base,
                        subdir_anlys=ds, metr=metr,comparison_col = 'dataset')



        #%% 2024 AGU-specific plot

        path_best_map_plot = fsplot.std_map_best_path(dir_out_viz_base,metr,'agu2024')
        states = fsplot.gen_conus_basemap(dir_out_basemap = dir_out_viz_base)
        title = f"Best predicted performance: {metr}"

        plot_best_perf = fsplot.plot_best_perf_map(best_df, states,title, comparison_col = 'dataset')
        plot_best_perf.savefig(path_best_map_plot, dpi=300, bbox_inches='tight')
        print(f"Wrote best performance map to \n{path_best_map_plot}")