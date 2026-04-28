"""Workflow script to format hfATLAS outputs into RaFTS-compatible inputs.

Example: 
    >>> python fs_hfatlas_to_rafts.py "/path/to/hfatl_rafts_config.yaml"

Changelog/Contributions
2026-04-28 Originally created to map hfATLAS divides to VPU and format for RaFTS.Developed by SS with the help of AI.
"""
import argparse
import ast
import yaml
import logging
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime
from collections import ChainMap
from logging.handlers import MemoryHandler

# Import RaFTS standard logging utility
from fs_prep.proc_eval_metrics import std_path_log

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
    save_dir = dir_db_attrs / dataset_name / str(vpuid)
    save_dir.mkdir(parents=True, exist_ok=True)
    return save_dir / f"attr_{vpuid}.parquet"

def process_hfatlas_to_rafts(config_path: Path, arg_val: bool, memory_handler: MemoryHandler, root_logger: logging.Logger):
    """Main workflow to convert hfATLAS output to RaFTS inputs at the VPU scale.

    :param config_path: The filepath to the YAML configuration file.
    :type config_path: Path
    :param arg_val: Boolean flag indicating if schema validation is enabled.
    :type arg_val: bool
    :param memory_handler: The logging handler caching logs before the file handler is configured.
    :type memory_handler: MemoryHandler
    :param root_logger: The root logger object.
    :type root_logger: logging.Logger
    """
    # 1. Load Configuration 
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    # Robust parsing: seamlessly handle both list-of-dicts (with hyphens) and standard dicts
    fio_raw = config.get('file_io', {})
    fio = dict(ChainMap(*fio_raw)) if isinstance(fio_raw, list) else fio_raw
    
    meta_raw = config.get('rafts_metadata', {})
    rafts_meta = dict(ChainMap(*meta_raw)) if isinstance(meta_raw, list) else meta_raw
    
    dir_base = Path(fio['dir_base']).expanduser()
    dir_db_attrs = Path(fio['dir_db_attrs'].format(dir_base=dir_base))
    dataset_name = rafts_meta['dataset_name']
    
    # 2. Setup File Logging and transfer MemoryHandler logs
    path_log = std_path_log(dir_input=dir_base, path_config=config_path, script='fs_hfatlas_to_rafts')
    
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
    path_hfatlas = Path(fio['path_hfatlas_parquet'].format(dir_base=dir_base))
    logging.info(f"Loading hfATLAS predictors from {path_hfatlas}")
    df_hfatlas = pd.read_parquet(path_hfatlas)
    df_hfatlas = clean_hfatlas_columns(df_hfatlas)
    
    # --- VPU ID Handling ---
    path_vpu_raw = fio.get('path_vpu_crosswalk', '')
    vpu_mapped = False
    
    if path_vpu_raw:
        path_vpu = Path(path_vpu_raw.format(dir_base=dir_base))
        if path_vpu.exists():
            logging.info(f"Attempting to extract VPU mapping from {path_vpu}...")
            try:
                df_vpu_map = pd.read_parquet(path_vpu, columns=['divide_id', 'vpuid'])
                df_hfatlas = df_hfatlas.merge(df_vpu_map, on='divide_id', how='left')
                df_hfatlas['vpuid'] = df_hfatlas['vpuid'].fillna('all')
                vpu_mapped = True
                logging.info("Successfully merged VPU mapping.")
            except Exception as e:
                logging.warning(f"Could not read 'vpuid' from {path_vpu}. Error: {e}")
                
    if not vpu_mapped:
        logging.info("No valid VPU mapping found or specified. Defaulting to placeholder VPU ID: 'all'")
        df_hfatlas['vpuid'] = 'all'

    # --- Filter out complex data structures ---
    logging.info("Scanning for complex data structures incompatible with fs_algo...")
    complex_cols = [
        col for col in df_hfatlas.columns 
        if col not in ['divide_id', 'vpuid'] 
        and df_hfatlas[col].first_valid_index() is not None 
        and isinstance(df_hfatlas[col].loc[df_hfatlas[col].first_valid_index()], (dict, list, np.ndarray))
    ]
                
    if complex_cols:
        logging.warning(f"Dropping {len(complex_cols)} columns with complex types: {complex_cols}")
        df_hfatlas = df_hfatlas.drop(columns=complex_cols)

    # 4. Reshape to RaFTS Long Format
    logging.info("Reshaping attributes to RaFTS standard schema...")
    df_long = df_hfatlas.melt(id_vars=['divide_id', 'vpuid'], var_name='attribute', value_name='value')
    
    df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
    df_long = df_long.dropna(subset=['value'])

    df_long = df_long.rename(columns={'divide_id': 'featureID'})
    df_long['featureSource'] = rafts_meta['featureSource']
    df_long['data_source'] = rafts_meta['data_source_flag']
    df_long['dl_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    final_columns = ['vpuid', 'featureID', 'featureSource', 'data_source', 'dl_timestamp', 'attribute', 'value']
    df_long = df_long[final_columns]
    
    # 5. Distributed I/O: Write parquet files by VPU
    logging.info(f"Writing parquet files grouped by VPU to {dir_db_attrs}")
    grouped_vpus = df_long.groupby('vpuid')
    total_vpus = len(grouped_vpus)
    
    for i, (vpuid, df_vpu) in enumerate(grouped_vpus):
        df_save = df_vpu.drop(columns=['vpuid'])
        save_path = generate_vpu_attr_filepath(dir_db_attrs, dataset_name, vpuid)
        logging.info(f"Writing {save_path.name} ({i+1}/{total_vpus})...")
        df_save.to_parquet(save_path, index=False)
            
    # 6. Generate Placeholder Metadata Parquet
    logging.info("Generating placeholder metadata parquet file for spatial joining...")
    unique_features = df_long['featureID'].unique()
    df_meta = pd.DataFrame({'featureID': unique_features})
    df_meta['featureSource'] = rafts_meta['featureSource']
    df_meta['lat'] = np.nan
    df_meta['lon'] = np.nan
    df_meta['geometry_populated'] = False
    
    path_meta = Path(fio['path_meta_placeholder'].format(dir_base=dir_base, dataset_name=dataset_name))
    path_meta.parent.mkdir(parents=True, exist_ok=True)
    df_meta.to_parquet(path_meta, index=False)
    
    logging.info(f"Saved placeholder metadata for {len(unique_features)} locations to {path_meta}")
    logging.info("Successfully finished processing and writing all files.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the hfATLAS configuration file for RaFTS.')
    parser.add_argument('path_config', type=str, help='Path to the YAML configuration file specific to hfATLAS processing.')
    parser.add_argument('--validate', action='store_true', default=False, 
                        help='If present, enables schema validation for all input and output data. Defaults to False.')
    args = parser.parse_args()

    path_config = Path(args.path_config).expanduser()

    # --- Conditionally log schemas
    arg_val = args.validate 
    if arg_val:
        # Placeholder for future pandera integration if required for this script
        pass 
            
    # --- Commence logging before creating the log file
    memory_handler = MemoryHandler(capacity=30)
    root_logger = logging.getLogger()
    root_logger.addHandler(memory_handler)
    root_logger.setLevel(logging.INFO)
    
    logging.info(f"Running fs_hfatlas_to_rafts.py with {path_config.name} config file")
    
    # --- Execute workflow
    process_hfatlas_to_rafts(path_config, arg_val, memory_handler, root_logger)