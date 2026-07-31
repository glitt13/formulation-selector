"""fs_write_params_gpkg.py

Extracts regionalized parameters at the divide_id scale and writes them as 
new tables into a new standalone GeoPackage. 

The tables are written with 'divide_id' set as the formal index and include 
the algorithm string in the table name (e.g., formulation_algo). The script 
filters algorithms using wildcard matching from 'algo_select'.

Usage:
    >>> python fs_write_params_gpkg.py "/path/to/pred_config.yaml"
"""

import argparse
import pandas as pd
from pathlib import Path
import logging
import sqlite3
import sys
import shutil
import fnmatch

import fs_algo.utils as fsutil
import fs_prep.proc_eval_metrics as pem

def register_gpkg_attributes_table(conn: sqlite3.Connection, table_name: str):
    """Registers a raw SQLite table as a non-spatial GeoPackage attributes layer."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='gpkg_contents'")
        if cursor.fetchone()[0] == 1:
            query = f"""
            INSERT OR IGNORE INTO gpkg_contents (table_name, data_type, identifier, description)
            VALUES ('{table_name}', 'attributes', '{table_name}', 'Regionalized parameters');
            """
            cursor.execute(query)
            conn.commit()
    except sqlite3.Error as e:
        logging.warning(f"Could not register table '{table_name}' in gpkg_contents: {e}")

def create_sqlite_index(conn: sqlite3.Connection, table_name: str, index_col: str):
    """Creates a formal SQLite database index on the specified column to speed up joins."""
    try:
        cursor = conn.cursor()
        index_name = f"idx_{table_name}_{index_col}"
        query = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{index_col}");'
        cursor.execute(query)
        conn.commit()
    except sqlite3.Error as e:
        logging.warning(f"Could not create SQLite index on '{table_name}' for column '{index_col}': {e}")

def update_database(db_path: Path, df_data: pd.DataFrame, table_name: str, id_col: str, overwrite: bool):
    """Helper function to execute standard SQLite table writing, appending, registration, and indexing."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            table_exists = cursor.fetchone()[0] == 1
            
            if not table_exists or overwrite:
                if overwrite and table_exists:
                    logging.info(f"Replacing existing table '{table_name}' in {db_path.name}.")
                else:
                    logging.info(f"Creating new table '{table_name}' in {db_path.name}.")
                    
                df_data.set_index(id_col).to_sql(table_name, conn, if_exists='replace', index=True)
                register_gpkg_attributes_table(conn, table_name)
                create_sqlite_index(conn, table_name, id_col)
                
            else:
                logging.info(f"Table '{table_name}' exists in {db_path.name}. Checking for missing {id_col}s...")
                existing_ids_query = f"SELECT {id_col} FROM '{table_name}'"
                try:
                    df_existing = pd.read_sql_query(existing_ids_query, conn)
                    existing_id_set = set(df_existing[id_col].astype(str))
                    
                    df_data[id_col] = df_data[id_col].astype(str)
                    df_new = df_data[~df_data[id_col].isin(existing_id_set)]
                    
                    if not df_new.empty:
                        logging.info(f"Appending {len(df_new)} new records to '{table_name}'.")
                        df_new.set_index(id_col).to_sql(table_name, conn, if_exists='append', index=True)
                        register_gpkg_attributes_table(conn, table_name)
                        create_sqlite_index(conn, table_name, id_col)
                    else:
                        logging.info(f"No new records to append. Table '{table_name}' is up to date.")
                except sqlite3.OperationalError:
                    logging.error(f"Identifier column '{id_col}' missing in the existing table '{table_name}'. Cannot append.")
    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred while writing to {db_path.name}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Write regionalized parameters to a standalone GeoPackage.')
    parser.add_argument('path_pred_config', type=str, help='Path to the prediction YAML config.')
    args = parser.parse_args()

    path_pred_config = Path(args.path_pred_config).expanduser()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Running fs_write_params_gpkg.py using {path_pred_config.name}")

    # --- Parse Configurations ---
    pred_cfg = fsutil.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()

    path_attr_config = fsutil.build_cfig_path(path_pred_config, pred_cfg.pred_cfg_dict.get('name_attr_config'))
    attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()
    
    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets')
    home_dir = attr_cfig.attrs_cfg_dict.get('home_dir')
    
    context = {
        'dir_base': str(dir_base),
        'dir_std_base': str(dir_std_base),
        'dir_db_attrs': str(dir_db_attrs),
        'home_dir': str(home_dir),
    }

    # Fetch configuration variables
    overwrite_sql = pred_cfg.pred_cfg_dict.get('overwrite_sql', False)
    base_algos = pred_cfg.pred_cfg_dict.get('algo_type', [])
    resp_vars = pred_cfg.pred_cfg_dict.get('algo_response_vars', [])
    
    algo_select = pred_cfg.pred_cfg_dict.get('algo_select', [])
    if isinstance(algo_select, str):
        algo_select = [algo_select]

    # Locate the Prep Config to extract formulation ID
    try:
        name_prep_config = [x for x in attr_cfig.attr_config.get('file_io') if 'name_prep_config' in x][0]['name_prep_config']
        path_prep_config = fsutil.build_cfig_path(path_pred_config, name_prep_config)
        col_schema_df = pem.read_schm_ls_of_dict(path_prep_config)
        formulation_id = pem.std_form_id(col_schema_df)
        logging.info(f"Resolved base formulation name: {formulation_id}")
    except Exception as e:
        logging.error(f"Failed to resolve prep config or formulation ID: {e}")
        sys.exit(1)

    dirs_std_dict = fsutil.fs_save_algo_dir_struct(dir_base)
    subdir = path_pred_config.parent.name
    dir_regionalization_sub = Path(dirs_std_dict.get('dir_out')) / "regionalization" / subdir
    dir_regionalization_sub.mkdir(parents=True, exist_ok=True)
    
    # Initialize Master GPKG Path
    path_hf_finl_gpkg_raw = pred_cfg.pred_cfg_dict.get('path_hf_finl_gpkg')
    if not path_hf_finl_gpkg_raw:
        logging.error("No 'path_hf_finl_gpkg' found in prediction config.")
        sys.exit(1)
        
    path_hf_finl_gpkg = Path(fsutil.resolve_fstrings(path_hf_finl_gpkg_raw, context))
    if not path_hf_finl_gpkg.exists():
        logging.error(f"Source Master GPKG does not exist: {path_hf_finl_gpkg}")
        sys.exit(1)

    # Ensure local Master GPKG copy exists
    dest_master_path = dir_regionalization_sub / path_hf_finl_gpkg.name.replace('.gpkg','_regn.gpkg')
    if not dest_master_path.exists(): 
        logging.info(f"Initializing local Master GPKG by copying {path_hf_finl_gpkg.name} to {dest_master_path.parent}")
        shutil.copy2(path_hf_finl_gpkg, dest_master_path)

    dir_regionalization_ds = Path(dirs_std_dict.get('dir_out')) / "regionalization"
    dir_out_alg_base = Path(dirs_std_dict.get('dir_out_alg_base'))

    # =========================================================================
    # Process Datasets and Write Tables
    # =========================================================================
    for ds in datasets:
        dir_ds_regn = dir_regionalization_ds / ds
        dir_out_alg_ds = dir_out_alg_base / ds

        for resp_var in resp_vars:
            dynamic_algos = fsutil.discover_dynamic_algos(
                search_dir=dir_out_alg_ds,
                base_algos=base_algos,
                metric=resp_var,
                dataset_id=ds,
                file_prefix="algo_",
                file_extension=".joblib"
            )

            for algo in dynamic_algos:
                
                # --- APPLY ALGO_SELECT FILTER ---
                if algo_select:
                    matched = any(fnmatch.fnmatch(algo, pattern) for pattern in algo_select)
                    if not matched:
                        logging.info(f"Algorithm '{algo}' skipped (does not match 'algo_select' patterns).")
                        continue

                param_file_mapped = dir_ds_regn / f"receiver_params_mapped_{algo}_{resp_var}__{ds}.parquet"
                param_file_std = dir_ds_regn / f"receiver_params_{algo}_{resp_var}__{ds}.csv"

                if param_file_mapped.exists():
                    param_file = param_file_mapped
                elif param_file_std.exists():
                    param_file = param_file_std
                else:
                    logging.error(f"No parameter files found for {algo} and {resp_var} in {dir_ds_regn}")
                    continue
                    
                logging.info(f"Loading regionalized parameters from {param_file.name}")
                if param_file.suffix == '.parquet':
                    df_params = pd.read_parquet(param_file)
                else:
                    df_params = pd.read_csv(param_file)

                id_col = 'divide_id' if 'divide_id' in df_params.columns else 'featureID'
                if id_col not in df_params.columns:
                    id_col = df_params.columns[0]
                    logging.warning(f"Expected identifier column not found. Defaulting to first column: '{id_col}'.")
                    
                # Dynamically generate table and file names
                table_name = f"{formulation_id}_{algo}"
                path_output_sql = dir_regionalization_sub / f"compiled_regionalized_params_{algo}.sqlite"
                
                # Write/Append to Local SQLite DB
                update_database(path_output_sql, df_params, table_name, id_col, overwrite_sql)
                
                # Write/Append to Master GPKG
                update_database(dest_master_path, df_params, table_name, id_col, overwrite_sql)

    logging.info("FINISHED writing and transferring parameters to GeoPackages.")