import argparse
import yaml
import pandas as pd
from pathlib import Path
import fs_algo.fs_algo_train_eval as fsate
import ast
import numpy as np

"""Workflow script to train algorithms on catchment attribute data for predicting
    formulation metrics and/or hydrologic signatures.

:raises ValueError: When the algorithm config file path does not exist
:note python fs_proc_algo.py "/path/to/algo_config.yaml"

"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the algorithm config file')
    parser.add_argument('path_algo_config', type=str, help='Path to the YAML configuration file specific for algorithm training')
    args = parser.parse_args()
    home_dir = Path.home()
    path_algo_config = Path(args.path_algo_config) #Path(f'{home_dir}/git/formulation-selector/scripts/eval_ingest/xssa/xssa_algo_config.yaml') 

    with open(path_algo_config, 'r') as file:
        algo_cfg = yaml.safe_load(file)
    
    # Ensure the string literal is converted to a tuple for `hidden_layer_sizes`
    algo_config = {k: algo_cfg['algorithms'][k] for k in algo_cfg['algorithms']}
    if algo_config['mlp'][0].get('hidden_layer_sizes',None): # purpose: evaluate string literal to a tuple
        algo_config['mlp'][0]['hidden_layer_sizes'] = ast.literal_eval(algo_config['mlp'][0]['hidden_layer_sizes'])
    algo_config_og = algo_config.copy()

    verbose = algo_cfg['verbose']
    test_size = algo_cfg['test_size']
    seed = algo_cfg['seed']
    read_type = algo_cfg.get('read_type','all') # Arg for how to read attribute data using comids in fs_read_attr_comid(). May be 'all' or 'filename'.

    #%% Attribute configuration
    name_attr_config = algo_cfg.get('name_attr_config', Path(path_algo_config).name.replace('algo','attr')) 
    path_attr_config = fsate.build_cfig_path(path_algo_config, name_attr_config)
    
    if not Path(path_attr_config).exists():
        raise ValueError(f"Ensure that 'name_attr_config' as defined inside {path_algo_config.name} \
                          \n is also in the same directory as the algo config file {path_algo_config.parent}" )
    print("BEGINNING algorithm training, testing, & evaluation.")

    # Initialize attribute configuration class for extracting attributes
    attr_cfig = fsate.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()



    # Grab the attributes of interest from the attribute config file,
    #  OR a .csv file if specified in the algo config file.
    name_attr_csv = algo_cfg.get('name_attr_csv')
    colname_attr_csv = algo_cfg.get('colname_attr_csv')
    attrs_sel = fsate._id_attrs_sel_wrap(attr_cfig=attr_cfig,
                    path_cfig=path_attr_config,
                    name_attr_csv = name_attr_csv,
                    colname_attr_csv = colname_attr_csv)
    
    # Define directories/datasets from the attribute config file
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest

    #%%  Generate standardized output directories
    dir_out = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out')
    dir_out_alg_base = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out_alg_base')
    
    # %% Looping over datasets
    for ds in datasets: 
        print(f'PROCESSING {ds} dataset inside \n {dir_std_base}')

        dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))
        dir_out_alg_ds.mkdir(exist_ok=True)

        # TODO allow secondary option where dat_resp and metrics read in from elsewhere
        # Read in the standardized dataset generated by fs_proc
        dat_resp = fsate._open_response_data_fs(dir_std_base,ds)
        
        # The metrics approach. These are xarray data variables of the response(s)
        metrics = dat_resp.attrs['metric_mappings'].split('|')

        # %% COMID retrieval and assignment to response variable's coordinate
        [featureSource,featureID] = fsate._find_feat_srce_id(dat_resp,attr_cfig.attr_config) # e.g. ['nwissite','USGS-{gage_id}']
        gdf_comid = fsate.fs_retr_nhdp_comids_geom(featureSource=featureSource,
                                            featureID=featureID,
                                            gage_ids=dat_resp['gage_id'].values)
        comids_resp = gdf_comid['comid']
        dat_resp = dat_resp.assign_coords(comid = comids_resp)
        # Remove the unknown comids:
        dat_resp = dat_resp.dropna(dim='comid',how='any')
        comids_resp = [x for x in comids_resp if x is not np.nan]
        # TODO allow secondary option where featureSource and featureIDs already provided, not COMID 

        #%%  Read in predictor variable data (aka basin attributes) 
        # Read the predictor variable data (basin attributes) generated by proc.attr.hydfab
        df_attr = fsate.fs_read_attr_comid(dir_db_attrs, comids_resp, attrs_sel = attrs_sel,
                                        _s3 = None,storage_options=None,read_type=read_type)
        # Convert into wide format for model training
        df_attr_wide = df_attr.pivot(index='featureID', columns = 'attribute', values = 'value')

    # %% Train, test, and evaluate
        rslt_eval = dict()
        for metr in metrics:
            print(f' - Processing {metr}')
            if len(algo_config) == 0:
                algo_config = algo_config_og.copy()
            # Subset response data to metric of interest & the comid
            df_metr_resp = pd.DataFrame({'comid': dat_resp['comid'],
                                        metr : dat_resp[metr].data})
            # Join attribute data and response data
            df_pred_resp = df_metr_resp.merge(df_attr_wide, left_on = 'comid', right_on = 'featureID')

            # TODO may need to add additional distinguishing strings to dataset_id, e.g. in cases of probabilistic simulation

            # Instantiate the training, testing, and evaluation class
            train_eval = fsate.AlgoTrainEval(df=df_pred_resp,
                                        attrs=attrs_sel,
                                        algo_config=algo_config,
                                        dir_out_alg_ds=dir_out_alg_ds, dataset_id=ds,
                                        metr=metr,test_size=test_size, rs = seed,
                                        verbose=verbose)
            train_eval.train_eval() # Train, test, eval wrapper

            # Retrieve evaluation metrics dataframe
            rslt_eval[metr] = train_eval.eval_df
            path_eval_metr = fsate.std_eval_metrs_path(dir_out_alg_ds, ds,metr)
            train_eval.eval_df.to_csv(path_eval_metr)
            del train_eval
        # Compile results and write to file
        rslt_eval_df = pd.concat(rslt_eval).reset_index(drop=True)
        rslt_eval_df['dataset'] = ds
        rslt_eval_df.to_parquet(Path(dir_out_alg_ds)/Path('algo_eval_'+ds+'.parquet'))

        print(f'... Wrote training and testing evaluation to file for {ds}')

        dat_resp.close()
    print("FINISHED algorithm training, testing, & evaluation")