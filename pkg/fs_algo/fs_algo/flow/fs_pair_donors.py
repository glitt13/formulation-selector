"""fs_pair_donors.py

Following algorithm clustering predictions from fs_pred_algo.py, the donor-receiver pairing
assigns a donor basin (used in training) to every receiver basin based on sharing the same
cluster and then finding the donor basin within each cluster at the nearest euclidean distance.

Changelog / Contributions
 2026-07-20 Originally created, GL with heavy consultation from Gemini3.1Pro
"""

from sklearn.neighbors import NearestNeighbors
import gower

import argparse
import pandas as pd
from pathlib import Path
import logging
from logging.handlers import MemoryHandler
import sys
import numpy as np

import fs_algo.utils as fsutil
import fs_prep.proc_eval_metrics as pem

def assign_donors_to_receivers(
    df_donors: pd.DataFrame, 
    df_receivers: pd.DataFrame, 
    attrs: list, 
    metric: str = 'euclidean',
    cluster_col: str = 'prediction',
    id_col: str = 'featureID'
) -> pd.DataFrame:
    """
    Pairs each receiver basin with the most similar donor basin within its assigned cluster.
    """
    pairing_results = []
    unique_clusters = df_receivers[cluster_col].dropna().unique()
    
    for cluster_id in unique_clusters:
        donors_in_clust = df_donors[df_donors[cluster_col] == cluster_id].reset_index(drop=True)
        receivers_in_clust = df_receivers[df_receivers[cluster_col] == cluster_id].reset_index(drop=True)
        
        if donors_in_clust.empty:
            logging.warning(f"No donors found for Cluster {cluster_id}. {len(receivers_in_clust)} receivers unassigned.")
            continue
            
        X_donor = donors_in_clust[attrs]
        X_recv = receivers_in_clust[attrs]
        
        # Calculate Nearest Neighbor
        if metric == 'gower':
            dist_matrix = gower.gower_matrix(np.asarray(X_recv), np.asarray(X_donor))
            closest_donor_indices = np.argmin(dist_matrix, axis=1)
            distances = np.min(dist_matrix, axis=1)
        else:
            nn = NearestNeighbors(n_neighbors=1, metric=metric)
            nn.fit(X_donor)
            distances, closest_donor_indices = nn.kneighbors(X_recv)
            distances = distances.flatten()
            closest_donor_indices = closest_donor_indices.flatten()
            
        # Record the pairings
        clust_pairings = pd.DataFrame({
            'receiver_id': receivers_in_clust[id_col],
            'donor_id': donors_in_clust.loc[closest_donor_indices, id_col].values,
            'cluster_id': cluster_id,
            'distance_to_donor': distances
        })
        pairing_results.append(clust_pairings)
        
    if pairing_results:
        return pd.concat(pairing_results, ignore_index=True)
    else:
        return pd.DataFrame()




"""
Workflow script to pair ungauged receiver basins with gauged donor basins
based on 1:1 attribute nearest-neighbor matching within machine learning clusters.

Usage:
    >>> python fs_pair_donors.py "/path/to/datasetshortname_pred_config.yaml"
"""



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the prediction config file for donor pairing.')
    parser.add_argument('path_pred_config', type=str, help='Path to the prediction YAML config.')
    args = parser.parse_args()

    path_pred_config = Path(args.path_pred_config).expanduser()

    # --- Logging Setup ---
    memory_handler = MemoryHandler(capacity=30)
    root_logger = logging.getLogger()
    root_logger.addHandler(memory_handler)
    root_logger.setLevel(logging.INFO)
    logging.info(f"Running fs_pair_donors.py with {path_pred_config.name}")

    # --- Parse Configurations ---
    pred_cfg = fsutil.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()

    path_attr_config = fsutil.build_cfig_path(path_pred_config, pred_cfg.pred_cfg_dict.get('name_attr_config'))
    path_algo_config = fsutil.build_cfig_path(path_pred_config, pred_cfg.pred_cfg_dict.get('name_algo_config'))

    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()
    
    algo_cfig = fsutil.AlgoConfigParser(path_algo_config)
    algo_cfig._read_algo_config()
    
    task_type = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"].get("task_type", "regression")
    if task_type != 'clustering':
        logging.error("Donor pairing currently requires clustering predictions. Exiting.")
        sys.exit(1)

    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets')
    
    id_col_pred = pred_cfg.pred_cfg_dict.get('pred_file_comid_colname')
    context = {
        'dir_base': str(dir_base),
        'dir_std_base': str(dir_std_base),
        'dir_db_attrs': str(dir_db_attrs),
    }

    path_meta_raw = pred_cfg.pred_cfg_dict.get('path_meta')
    path_meta = Path(fsutil.resolve_fstrings(path_meta_raw, context))
    if not path_meta.exists():
        logging.error(f"Training metadata missing: {path_meta}. Cannot identify donors.")


    # Standard output directories
    dirs_std_dict = fsutil.fs_save_algo_dir_struct(dir_base)
    dir_out = dirs_std_dict.get('dir_out')
    dir_out_alg_base = dirs_std_dict.get('dir_out_alg_base')
    dir_regionalization = Path(dir_out) / "regionalization"
    dir_regionalization.mkdir(exist_ok=True)

    # Resolve Attributes
    name_attr_csv = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["name_attr_csv"]
    colname_attr_csv = algo_cfig.algo_cfg_unc_dict["algo_cfg_dict"]["colname_attr_csv"]
    attrs_sel = fsutil._id_attrs_sel_wrap(attr_cfig=attr_cfig, path_cfig=path_attr_config, 
                                          name_attr_csv=name_attr_csv, colname_attr_csv=colname_attr_csv)

    path_log = pem.std_path_log(dir_input=dir_base, path_config=path_pred_config, script='fs_pair_donors')
    logging.basicConfig(level=logging.INFO, filename=path_log, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', force=True)
    print(f"Logging to {path_log}")
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            memory_handler.setTarget(handler)
            memory_handler.flush()
            break
    root_logger.removeHandler(memory_handler)

    resp_vars = pred_cfg.pred_cfg_dict.get('algo_response_vars')
    algos = pred_cfg.pred_cfg_dict.get('algo_type')

    # --- Processing ---
    for ds in datasets:

        logging.info(f"PROCESSING Donor-Receiver Pairing for dataset: {ds}")
        dir_out_alg_ds = Path(dir_out_alg_base) / ds
        
        # 1. Load DONOR Attributes (Gauged Basins from Training)
        vals_train = {'ds_type': 'training', 'write_type': 'parquet', 'dir_std_base': dir_std_base, 'ds': ds}
        # Path to the training data (donors)
        dir_db_attrs = Path(str(dir_db_attrs).format(**vals_train))
     
            
        logging.info(f"Reading donor metadata from {path_meta}")    
        # Strip pint units
        # df_meta = fsutil.clean_hfatlas_columns(pd.read_parquet(path_meta))
        df_meta = fsutil.read_hfatlas_wrap_dask(
            paths_hfatl=[dir_db_attrs], 
            attrs_sel=[], # Empty list forces it to only pull the map_id_col
            map_id_col='featureID'
        )
        # Read in the donor ids, meaning those that were trained.
        gageids_donor = df_meta['featureID'].drop_duplicates().tolist()
        
        df_attr_donor = fsutil.fs_read_attr_comid(dir_db_attrs, 
                                                  gageids_donor, attrs_sel=attrs_sel, read_type='all')
        df_donor_wide = df_attr_donor.pivot(index='featureID', columns='attribute', values='value').dropna()
        logging.info(f"Ingested donor attribute data. Total locations = {df_donor_wide.shape[0]}")

        for resp_var in resp_vars:
            dynamic_algos = fsutil.discover_dynamic_algos(
                search_dir=dir_out_alg_ds, base_algos=algos, metric=resp_var,
                dataset_id=ds, file_prefix="algo_", file_extension=".joblib"
            )

            for algo in dynamic_algos:
                logging.info(f"Pairing locations for algorithm: {algo} | Response: {resp_var}")
                
                # 2. Predict Clusters for DONORS using the loaded pipeline
                path_algo = fsutil.std_algo_path(dir_out_alg_ds, algo=algo, metric=resp_var, dataset_id=ds)
                pipeline_data = fsutil.load_validated_pipeline(path_algo, arg_val=False)
                pipe = pipeline_data['pipeline']
                expected_features = pipe.feature_names_in_
                
                # Sort the df_donor_wide by expected features:
                df_donor_wide = df_donor_wide[expected_features]
                
                # Predict donor clusters
                donor_clusters = pipe.predict(df_donor_wide)
                df_donors_paired = df_donor_wide.copy().reset_index()
                df_donors_paired['prediction'] = donor_clusters

                # Determine distance metric
                dist_metric = 'gower' if 'gower' in algo.lower() else 'euclidean'

                # 3. Load RECEIVER Predictions (Ungauged Basins from fs_pred_algo.py)
                path_pred_in = fsutil.std_pred_path(dir_out=dir_out, algo=algo, metric=resp_var, dataset_id=ds)
                if not path_pred_in.exists():
                    logging.warning(f"Receiver predictions missing: {path_pred_in}")
                    print(f"PROBLEM: No Receiver predictions from the prediction step!! {path_pred_in}")
                    continue
                
                # TODO read in the predictions
                path_pred_out = fsutil.std_pred_path(dir_out,algo=algo,metric=resp_var,dataset_id=ds)
                df_receivers = pd.read_parquet(path_pred_out)
                df_recv_attrs = fsutil.read_hfatlas_wrap_dask(
                                    paths_hfatl=[path_meta], 
                                    attrs_sel=attrs_sel, # Empty list forces it to only pull the map_id_col
                                    map_id_col=id_col_pred#,query_clean=True
                                    )
                

                if 'featureID' not in df_receivers.columns:
                    df_receivers.rename(columns={id_col_pred:'featureID'},inplace=True)

                df_receivers_mrge = df_receivers.merge(df_recv_attrs, left_on = 'featureID', right_on = id_col_pred, how = 'inner')         
                df_receivers_mrge = df_receivers_mrge[['featureID','prediction']+attrs_sel]      

                # # TODO change this to read the receivers, not the donors!!
                # # Fetch receiver attributes to calculate distance
                # locids_recv = df_receivers[id_col_pred].tolist()
                # df_attr_recv = fsutil.fs_read_attr_comid(Path(str(dir_db_attrs).format(**vals_train)), 
                #                                          locids_recv, attrs_sel=attrs_sel, read_type='all')
                # df_recv_wide = df_attr_recv.pivot(index=id_col_pred, columns='attribute', values='value').dropna()
                logging.info(f"Ingested receiver attribute data. Total locations = {df_receivers.shape[0]}")

                # # Merge receiver predictions with their attributes
                # df_receivers_paired = df_receivers.merge(df_recv_wide, left_on=id_col_pred, right_index=True, how='inner')

                # 4. Execute 1:1 Pairing
                df_pairings = assign_donors_to_receivers(
                    df_donors=df_donors_paired,
                    df_receivers=df_receivers_mrge,
                    attrs=attrs_sel,
                    metric=dist_metric
                )

                if not df_pairings.empty:
                    df_pairings['dataset'] = ds
                    df_pairings['algo'] = algo
                    df_pairings['resp_var'] = resp_var
                    
                    # 5. Save Output
                    path_pair_out = dir_regionalization / ds / f"donor_pairs_{algo}_{resp_var}__{ds}.csv"
                    path_pair_out.parent.mkdir(parents=True, exist_ok=True)
                    df_pairings.to_csv(path_pair_out, index=False)
                    logging.info(f"Saved {len(df_pairings)} donor-receiver pairings to {path_pair_out}")

    logging.info("FINISHED Donor-Receiver Pairing.")
    logging.shutdown()