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
from fs_prep import prep_gpkg
import fs_algo.utils as fsutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Safe f-string Resolver for DataFrames ---
class SafeDict(dict):
    """Allows safe mapping where missing keys are left as unresolved f-strings (e.g. '{missing}')."""
    def __missing__(self, key):
        return '{' + key + '}'

def resolve_fstrings(val, context_dict, max_depth=3):
    """Recursively formats f-strings natively found inside parsed YAML items."""
    if not isinstance(val, str) or '{' not in val:
        return val
    for _ in range(max_depth):
        new_val = val.format_map(SafeDict(**context_dict))
        if new_val == val:
            return new_val
        val = new_val
    return val

def get_middle_vertex(geom) -> Point:
    """Returns the middle existing coordinate/vertex from a line."""
    if geom is None or geom.is_empty:
        return None
        
    # 1. Extract all coordinates into a single list
    if geom.geom_type == 'LineString':
        coords = list(geom.coords)
    elif geom.geom_type == 'MultiLineString':
        # Flatten the coordinates from all line segments into one list
        coords = [coord for line in geom.geoms for coord in line.coords]
    else:
        return None # Fallback for non-line geometries
        
    # 2. Find the middle index
    mid_index = len(coords) // 2
    
    # 3. Return as a shapely Point
    return Point(coords[mid_index])


def generate_algo_points_gpkg_wrap(
    div_ids: pd.Series,
    path_hf_gpkg: str | Path, 
    path_gpkg_fs_prep: str | Path,
    dir_db_gpkg: str | Path,
    hf_layer: str = 'flowpaths',
    map_id_col: str = 'divide_id',
    featureSource: str = 'hf_id',
    vpu_id_col: str = 'vpuid',
    epsg: int = 4326
) -> gpd.GeoDataFrame:
    """
    Standardized a dataset-specific .gpkg of points using a smart caching and extraction strategy
    """
    path_hf_gpkg = Path(path_hf_gpkg)
    path_gpkg_fs_prep = Path(path_gpkg_fs_prep) # The file write location
    dir_db_gpkg = Path(dir_db_gpkg)

    # Read just the divide-ids of interest from the flowpath layer of the hydrofabric GPKG
    formatted_ids = ", ".join([f"'{div_id}'" for div_id in div_ids])
    where_clause = f"{map_id_col} IN ({formatted_ids})"

    # Read ONLY the requested rows
    sub_fp_gdf = gpd.read_file(
        path_hf_gpkg, 
        layer=hf_layer, 
        where=where_clause,
        engine="pyogrio" 
    )

    sub_fp_gdf.to_crs(epsg=epsg, inplace=True)

    if vpu_id_col not in sub_fp_gdf.columns:
        logging.warning(f"Expecting {vpu_id_col} vpu col to be in hf dataset {path_hf_gpkg.name}")

    # TODO any additional colname modifications needed? 
    # Historic columns have been: sourceName featureID comid name X Y featureSource  gage_id measure reachcode tot_na
    try:
        sub_fp_gdf['X'] = sub_fp_gdf.geometry.x
        sub_fp_gdf['Y'] = sub_fp_gdf.geometry.y
    except:
        # Apply it to your GeoDataFrame
        gdf = sub_fp_gdf.copy()
        gdf['middle_vertex'] = gdf.geometry.apply(get_middle_vertex)

        # Extract X and Y if you need them in separate columns
        sub_fp_gdf['X'] = gdf['middle_vertex'].apply(lambda p: p.x if p else None)
        sub_fp_gdf['Y'] = gdf['middle_vertex'].apply(lambda p: p.y if p else None)
        
        # Reset the geometry to the middle vertex
        sub_fp_gdf['geometry'] = gdf['middle_vertex']
        sub_fp_gdf = sub_fp_gdf.set_geometry('geometry')

    # TODO figure out what to do about gage_id column.... Should there be an aggregation here based on subsetting???
    sub_fp_gdf['featureID'] = sub_fp_gdf[map_id_col]
    sub_fp_gdf['featureSource'] = featureSource
    sub_fp_gdf['gage_id'] = sub_fp_gdf[map_id_col] # TODO should this always be the same as divide_id?? 
    sub_fp_gdf['comid'] = sub_fp_gdf[map_id_col] # TODO should this always be the same as divide_id?? 
    sub_fp_gdf['tot_na'] = 0 # TODO figure out what to do about tot_na column
    # Write to the dataset-specific gpkg path:
    # TODO standardize this file write??
    sub_fp_gdf.to_file(path_gpkg_fs_prep, driver="GPKG",  layer = 'outlet')
    logging.info(f"Wrote {sub_fp_gdf.shape[0]} hydrofabric flowpaths to {path_gpkg_fs_prep}")

    return sub_fp_gdf

# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
def read_hfatlas_wrap(path_hfatl: Path | os.PathLike, attrs_sel:list, 
                        map_id_col: str = "divide_id") -> pd.DataFrame: 
    """Read attribute dataset, removing pint awareness when hfATLAS formatted

    :param path_hfatl: The filepath to the hydrofabricATLAS aggregated final output
    :type path_hfatl: Path | os.PathLike
    :param attrs_sel: The attributes of interest for training/predicting
    :type attrs_sel: list
    :param map_id_col: The location id col in attribute data, defaults to "divide_id"
    :type map_id_col: str, optional
    :return: Dataset of location identifier column and subset attributes
    :rtype: pd.DataFrame
    """
    df_hfatlas = pd.read_parquet(path_hfatl)
    df_hfatlas = clean_hfatlas_columns(df_hfatlas) 
    # Subset to attribute columns. This must happen after clean_hfatlas_columns, which drops pint unit awareness
    cols_keep_hfatlas = [map_id_col] + attrs_sel
    df_hfatlas = df_hfatlas.filter(items=cols_keep_hfatlas)
    miss_cols = [col for col in cols_keep_hfatlas if col not in df_hfatlas.columns]
    if len(miss_cols) > 0:
        logging.warning(f"Problem with attribute selection, the following are missing: \
                        {miss_cols}")
    return df_hfatlas


def clean_hfatlas_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Parses pint-aware string columns and renames them to standard strings.

    :param df: The dataframe containing raw hfATLAS column names (e.g., "('TOT_AET_hfa', 'millimeter')").
    :type df: pd.DataFrame
    :return: The dataframe with cleaned, standard column names (e.g., 'TOT_AET_hfa').
    :rtype: pd.DataFrame
    """
    new_cols = {}
    for col in df.columns:
        if col.startswith("('") and col.endswith("')"):
            try:
                parsed_tuple = ast.literal_eval(col)
                new_cols[col] = parsed_tuple[0]
            except (ValueError, SyntaxError):
                new_cols[col] = col
        else:
            new_cols[col] = col
            
    return df.rename(columns=new_cols)

def generate_vpu_attr_filepath(dir_db_attrs: Path, dataset_name: str, vpuid: str) -> Path:
    """Creates a standardized filepath grouped by dataset and VPU identifier.

    :param dir_db_attrs: The base directory where attribute parquet files are stored.
    :type dir_db_attrs: Path
    :param dataset_name: The name of the dataset being processed (e.g., 'hfatlas').
    :type dataset_name: str
    :param vpuid: The Vector Processing Unit identifier (e.g., '01', '18', or 'all').
    :type vpuid: str
    :return: The fully resolved Path object for saving the attribute parquet file.
    :rtype: Path
    """
    # TODO ensure that dir_db_attrs is ALWAYS used here for consistency across codebase attribute storage
    save_dir = dir_db_attrs / dataset_name / str(vpuid)
    save_dir.mkdir(parents=True, exist_ok=True)
    return save_dir / f"attr_{vpuid}.parquet"


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
    path_hfatl = [x.get('paths_hfatl') for x in attr_cfig.attr_config.get('attr_select') if x.get('paths_hfatl') is not None][0][0].format(home_dir=home_dir)
    if not path_hfatl:
        logging.error("No real path provided for hfATLAS data in the attribute config entry, paths_hfatl.")

    # Loop over each dataset. Realistically this will only be one, but keeping the definition of ds for consistency across RaFTS workflow scripts
    for ds in datasets: 
        logging.info(f'PROCESSING {ds} dataset inside \n {dir_std_base}')

        vals = {'ds_type':ds_type,'write_type':write_type, 'dir_std_base':dir_std_base,'ds':ds}
        path_meta = path_meta_fstr.format(**vals)

        # 1. Parse the prep config YAML into a DataFrame
        logging.info(f"Loading configuration from {path_prep_config}")
        config_df = read_schm_ls_of_dict(path_prep_config)
        raw_config = config_df.iloc[0].dropna().to_dict()

        # Inject the dataset name into the dictionary for f-string resolution
        fio = {k: resolve_fstrings(v, raw_config) for k, v in raw_config.items()}


        # 2. Extract specific variables
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
        
        # Define path of the standardized .gpkg corresponding to the response variables
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
        df_hfatlas = read_hfatlas_wrap(path_hfatl, attrs_sel, 
                              map_id_col)

        # Read in the hydrofabric flowpath w/ vpuid and standardize cols
        gdf_hf = generate_algo_points_gpkg_wrap(div_ids = df_hfatlas[map_id_col], 
                                                path_hf_gpkg=path_hf_gpkg,
                                                path_gpkg_fs_prep=path_gpkg_fs_prep,
                                                dir_db_gpkg=dir_db_gpkg,
                                                hf_layer=hf_layer,
                                                map_id_col=map_id_col,
                                                featureSource=featureSource,
                                                vpu_id_col=vpu_id_col,# epsg = 4326
                                                )
        # combine attribute data with the gdf containing vpu
        df_hfatl = df_hfatlas.merge(gdf_hf, how = 'outer', on = map_id_col)

        if not vpu_mapped:
            logging.info("No valid VPU mapping found or specified. Defaulting to placeholder VPU ID: 'all'")
            df_hfatlas['vpuid'] = 'all'

        # 4. Reshape to RaFTS Long Format
        logging.info("Reshaping attributes to RaFTS standard schema...")
        df_long = df_hfatl.melt(id_vars=[map_id_col, vpu_id_col], var_name='attribute', value_name='value')
        
        df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
        df_long = df_long.dropna(subset=['value'])

        df_long = df_long.rename(columns={map_id_col: 'featureID'})
        df_long['featureSource'] = featureSource
        df_long['data_source'] = fio.get('data_source','hfATLAS')
        df_long['dl_timestamp'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = ['vpuid', 'featureID', 'featureSource', 'data_source', 'dl_timestamp', 'attribute', 'value']
        df_long = df_long[final_columns]
        
        # 5. Distributed I/O: Write parquet files by VPU
        logging.info(f"Writing parquet files grouped by VPU to {dir_db_attrs}")
        grouped_vpus = df_long.groupby('vpuid')
        total_vpus = len(grouped_vpus)
        
        for i, (vpuid, df_vpu) in enumerate(grouped_vpus):
            df_save = df_vpu.drop(columns=['vpuid'])
            
            save_path = generate_vpu_attr_filepath(dir_db_attrs, ds, vpuid)
            logging.info(f"Writing {save_path.name} ({i+1}/{total_vpus})...")
            df_save.to_parquet(save_path, index=False)
        
        # 6. Generate Metadata Parquet
        # TODO make consistent with other metadata parquet
        logging.info("Generating placeholder metadata parquet file for spatial joining...")
        unique_features = gdf_hf['featureID'].unique()
        df_meta = pd.DataFrame({'featureID': unique_features})
        df_meta = gdf_hf[['featureID','featureSource','gage_id','X','Y']].drop_duplicates()
        df_meta.to_parquet(path_meta, index=False)
        
        logging.info(f"Saved placeholder metadata for {len(unique_features)} locations to {path_meta}")
        logging.info("Successfully finished processing and writing all attribute files.")

    
