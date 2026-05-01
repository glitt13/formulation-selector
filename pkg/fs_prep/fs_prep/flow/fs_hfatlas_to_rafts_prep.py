"""Workflow script to format hfATLAS attributes into RaFTS-compatible attributes.

Intended to run after the initial prep script that generates the response variable .nc file.


Example: 
    >>> uv run python fs_hfatlas_to_rafts_prep.py --path_prep_config "~/git/formulation-selector/scripts/eval_ingest/hfatl_test2/hfatl_prep_config.yaml" --name_attr_config "hfatl_attr_config.yaml"

Changelog/Contributions
2026-04-28 Originally created to map hfATLAS divides to VPU and format for RaFTS.Developed by SS with the help of AI.

# TODO should gdf_hf['gage_id'] always be the same as divide_id??  We may want this to differ when performing basin-based assessments (e.g. HUC level, not divide level as currently used)
"""

import argparse
import ast
import logging
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime
from collections import ChainMap
from logging.handlers import MemoryHandler
import geopandas as gpd
from shapely.geometry import Point
import os

# Import RaFTS functions
from fs_prep.proc_eval_metrics import read_schm_ls_of_dict, std_path_log

import fs_algo.utils as fsutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process hfATLAS attributes & hydrofabric for RaFTS.")
    parser.add_argument("--path_prep_config", required=True, help="Path to the prep config YAML file (e.g., hfatl_prep_config.yaml)")
    parser.add_argument("--name_attr_config", help="Filename of the attribute config YAML file")
    parser.add_argument('--validate', action='store_true', default=False, 
                        help='If present, enables schema validation for all input and output data. Defaults to False.')
    args = parser.parse_args()

    if  args.validate:
        # Placeholder for future pandera integration if required for this script
        pass 

    memory_handler = MemoryHandler(capacity=30)
    root_logger = logging.getLogger()
    root_logger.addHandler(memory_handler)
    root_logger.setLevel(logging.INFO)
    
    path_prep_config = Path(args.path_prep_config).expanduser()
    if not path_prep_config.exists():
        raise ValueError(f"Prep config file not found at {path_prep_config}")
    logging.info(f"Running fs_hfatlas_to_rafts.py with {path_prep_config.name} config file")

    # # --- Execute workflow
    # process_hfatlas_to_rafts(path_prep_config, args.validate, memory_handler, root_logger)

    # PARSE THE ATTRIBUTE CONFIG
    path_attr_config = fsutil.build_cfig_path(path_known_config = path_prep_config, path_or_name_cfig = args.name_attr_config)
    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()
    
    # Define directories/datasets from the attribute config file
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest
    # Grab variables for building out the path to metadata (which contains comid-gage id mappings)
    ds_type = [x for x in attr_cfig.attr_config.get('file_io') if 'ds_type' in x][0]['ds_type']
    write_type = [x for x in attr_cfig.attr_config.get('file_io') if 'write_type' in x][0]['write_type']
    path_meta_fstr = [x for x in attr_cfig.attr_config.get('file_io') if 'path_meta' in x][0]['path_meta']
    # The hfatlas attribute column names of interest:
    attrs_sel = attr_cfig.attrs_cfg_dict.get("attrs_sel")

    # TODO add path_hfatl to attr_config parser
    home_dir = Path("~/").expanduser()
    path_hfatl = Path([x.get('paths_hfatl') for x in attr_cfig.attr_config.get('attr_select') if x.get('paths_hfatl') is not None][0][0].format(home_dir=home_dir))
    if not path_hfatl.exists():
        logging.error("No real path provided for hfATLAS data in the attribute config entry, paths_hfatl.")

    # Loop over each dataset. Realistically this will only be one, but keeping the definition of ds for consistency across RaFTS workflow scripts
    for ds in datasets: 
        logging.info(f'PROCESSING {ds} dataset inside \n {dir_std_base}')

        # 1. Parse the prep config YAML into a DataFrame
        logging.info(f"Loading configuration from {path_prep_config}")
        config_df = read_schm_ls_of_dict(path_prep_config)
        raw_config = config_df.iloc[0].dropna().to_dict()
        # Inject the dataset name into the dictionary for f-string resolution
        fio = {k: fsutil.resolve_fstrings(v, raw_config) for k, v in raw_config.items()}

        # 2. Extract specific variables of interest
        path_hf_gpkg = Path(fio.get("path_hf_gpkg"))
        dir_db_gpkg = Path(fio.get("dir_db_gpkg"))
        
        # featureID & featureSource:
        featureSource = fio.get('featureSource', 'hf_id')
        gage_id = fio.get('gage_id') # This pertains to the response variable dataset??

        # Hydrofabric cols, location id cols, & data source
        hf_layer = fio.get("hf_fp_layer", "flowpaths") # In hf v2.2, the flowpath layer that maps divide_ids & VPUs
        hf_id_col = fio.get("hf_fp_id_col", "id") # Flowpath layer's id column
        map_id_col = fio.get("map_id_col", "divide_id") # TODO should this differ from fio.get('gage_id')? A: probably. 
        map_id_type = fio.get("map_id_type", "hf_id")
        vpu_id_col = fio.get("vpu_id_col", "vpuid") # Column name for vpu id in the hydrofabric f"{hf_layer}"
        vpu_mapped = fio.get('vpu_mapped', False) # Should output data be organized by vpu? vpu_mapped = True

        # Define path of the response variable dataset
        path_fs_dat_resp =  fsutil._std_fs_prep_ds_paths(dir_std_base=dir_std_base,ds=ds,mtch_str='*.nc')
        if len(path_fs_dat_resp) > 1:
                error_str = f"The following directory contains too many .nc files: {path_fs_dat_resp}"
                logging.error(error_str)
                raise ValueError(error_str)
        
        # Define path of the standardized .gpkg file write of combined attributes & geometry (alternative to metadata parquet)
        # TODO should we also consider prediction locations here?
        path_gpkg_fs_prep = fsutil._std_fs_prep_ds_companion_gpkg_path(path_fs_dat_resp[0])

        # 2. Setup File Logging and transfer MemoryHandler logs
        path_log = std_path_log(dir_input=dir_base, path_config=path_prep_config, script='fs_hfatlas_to_rafts')
        
        file_handler = logging.FileHandler(path_log, mode='w')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler)
        
        # Stream handler for console output
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(stream_handler)
        
        # Flush memory handler to the newly created file/stream handlers, then remove it
        memory_handler.setTarget(file_handler)
        memory_handler.flush()
        root_logger.removeHandler(memory_handler)
        
        logging.info(f"Writing logs to {path_log}")
        logging.info("Starting hfATLAS to RaFTS conversion.")

        # 3. Ingest Data
        logging.info(f"Loading hfATLAS predictors from {path_hfatl}")
        
        # Run attribute read & clean wrapper function
        df_hfatlas = fsutil.read_hfatlas_wrap(path_hfatl, attrs_sel, 
                              map_id_col)

        # Read in the hydrofabric flowpath w/ vpuid and standardize cols
        gdf_hf = fsutil.generate_algo_points_gpkg_wrap(div_ids = df_hfatlas[map_id_col], 
                                                path_hf_gpkg=path_hf_gpkg,
                                                dir_db_gpkg=dir_db_gpkg,
                                                path_gpkg_fs_prep=path_gpkg_fs_prep, # Only for writing to file
                                                hf_layer=hf_layer,
                                                map_id_col=map_id_col,
                                                featureSource=featureSource,
                                                vpu_id_col=vpu_id_col,# epsg = 4326
                                                )
        # Combine location identifiers with geometry, write to file spread out among different vpu_ids if vpu_mapped==True
        df_save = fsutil.hfatl_hf_cmbo_wrap(df_hfatlas=df_hfatlas, gdf_hf=gdf_hf, 
                            ds=ds, dir_db_attrs=dir_db_attrs, featureSource=featureSource, map_id_col=map_id_col,
                            vpu_id_col=vpu_id_col, vpu_mapped=vpu_mapped,
                            data_source = fio.get('data_source','hfATLAS'))
      
        # 6. Generate Metadata Parquet
        # In R, this comes from proc.attr.hydfab::proc_attr_gageids()
        # plus the column dataset_name from Retr_Params$loc_id_read$loc_id_filepath
        # featureID featureSource      data_source        dl_timestamp      attribute   value  gage_id  dataset_name
        #  10302627   COMID   hydroatlas__v1       2024-12-24 20:31:06     ari_ix_sav  101.00 03300400 juliemai-xSSA

        # TODO can metadata be skipped??

        # # TODO make consistent with other metadata parquet. This needs attributes, but is it worth it??
        # logging.info("Generating placeholder metadata parquet file for spatial joining...")
        # unique_features = gdf_hf['featureID'].unique()
        # df_meta = pd.DataFrame({'featureID': unique_features})
        # df_meta = gdf_hf[['featureID','featureSource','gage_id','X','Y']].drop_duplicates()
        # df_meta['dataset_name'] = Path(path_hfatl).name
        # vals = {'ds_type':ds_type,'write_type':write_type, 'dir_std_base':dir_std_base,'ds':ds}
        # path_meta = path_meta_fstr.format(**vals)
        # df_meta.to_parquet(path_meta, index=False)

        # logging.info(f"Saved placeholder metadata for {len(unique_features)} locations to {path_meta}")
        logging.info("Successfully finished processing and writing all attribute files.")

    
