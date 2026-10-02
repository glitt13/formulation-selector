'''
@title: Generate a test dataset from hfATLAS outputs based on basin-aggregated values
@author: Guy Litt <guy.litt@noaa.gov>
@description: This is the first rafts_select output from the July 2026 calibration
 experiments. Selection criteria: multiobj function, equally weighting NNSE & MAPPE
@usage: 
python agg_calib_basins.py # Must run first
python prep_regn_test.py "/full/path/to/regn_prep_config.yaml"

Changelog/contributions
    2026-04-20 Originally created, GL
    2026-05-13 Adapted from hfatl_test, GL
'''
import argparse
import pandas as pd
from pathlib import Path
import yaml
import rafts_prep.proc_eval_metrics as pem
import logging
import geopandas as gpd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the YAML config file.')
    parser.add_argument('path_config', type=str, help='Path to the YAML configuration file')
    args = parser.parse_args()
    # The path to the configuration
    path_config = Path(args.path_config).expanduser() # path_config = Path('~/git/rafts/scripts/workflow_configs/regn_apr26_test1/regn_prep_config.yaml').expanduser() 

    if not Path(path_config).exists():
        raise ValueError("The provided path to the configuration file does not exist: {path_config}")

    # Load the YAML configuration file
    with open(path_config, 'r') as file:
        config = yaml.safe_load(file)

    # Logger setup - first define required paths/name in the config
    home_dir = Path("~/").expanduser()
    file_io = config.get('file_io')
    if not file_io:
        raise ValueError(f"'file_io' section missing or empty in config: {path_config}")
    dir_save_entries = [x for x in file_io if 'dir_save' in x.keys()]
    if not dir_save_entries:
        raise ValueError(f"No 'dir_save' entry found under 'file_io' in config: {path_config}")
    dir_save = dir_save_entries[0]['dir_save'].format(home_dir = home_dir)

    # Generate path to the log file & initialize logging
    path_log = pem.std_path_log(dir_input=dir_save, path_config=path_config,
                            script='prep_xssa_metrics')
    logging.basicConfig(level=logging.INFO, filename=path_log, format='%(asctime)s - %(levelname)s - %(message)s')

    # ----- File IO
    logging.info("Converting schema to DataFrame")
    # Read in the config file & convert to pd.DataFrame
    col_schema_df = pem.read_schm_ls_of_dict(schema_path = path_config)

    # Extract path and format the home_dir in case it was defined in file path
    path_data = col_schema_df['path_data'].loc[0].format(home_dir = str(Path.home()))
    dir_gpkg_out = Path(path_data).parent
    print(path_data)

    
    # BEGIN CUSTOMIZED DATASET MUNGING
    # ---- Read in hfatlas test dataset
    logging.info("Custom code: Reading/formatting non-standardized input datasets")
    df_all_data = pd.read_parquet(path_data)#,engine="pyarrow")
    # # df_all_data[col_schema_df['gage_id']] = df_all_data.index.astype(str)
    # # TODO standardize the dataset here

    # END CUSTOMIZED DATASET MUNGING

    # ------ Extract metric data and write to file
    ds = pem.proc_col_schema(df_all_data, col_schema_df, dir_save)
    print("COMPLETED PROCESSING prep_regn_test_agg.py")
    logging.shutdown() # Remember to add this at the end of each script so that log files are separated by the script run