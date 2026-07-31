"""fs_write_params_gpkg.py

Extracts regionalized parameters at the divide_id scale and writes them as 
new tables into a new standalone GeoPackage. The table is named after the 
formulation-id. This new GeoPackage is stored in a standardized directory.

After the comprehensive GPKG is created, the script copies the tables into 
the existing master hydrofabric GPKG defined in the prediction configuration.
The tables are written with 'divide_id' set as the index.

Usage:
    >>> python fs_write_params_gpkg.py "/path/to/pred_config.yaml"

Changelog / Contributions
 2026-07-30 created via Gemini3.1Pro prompts
 2026-07-31 expanded to set index and transfer tables to master GPKG
"""

import argparse
import pandas as pd
from pathlib import Path
import logging
import sqlite3
import sys
import shutil

import fs_algo.utils as fsutil
import fs_prep.proc_eval_metrics as pem

def register_gpkg_attributes_table(conn: sqlite3.Connection, table_name: str):
    """Registers a raw SQLite table as a non-spatial GeoPackage attributes layer."""
    try:
        cursor = conn.cursor()
        # Check if the GeoPackage metadata table exists
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='gpkg_contents'")
        if cursor.fetchone()[0] == 1:
            # Register the table as an 'attributes' data_type
            query = f"""
            INSERT OR IGNORE INTO gpkg_contents (table_name, data_type, identifier, description)
            VALUES ('{table_name}', 'attributes', '{table_name}', 'Regionalized parameters');
            """
            cursor.execute(query)
            conn.commit()
    except sqlite3.Error as e:
        logging.warning(f"Could not register table '{table_name}' in gpkg_contents: {e}")

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
    
    # Context for f-string resolution
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

    # Locate the Prep Config to extract formulation ID using proc_eval_metrics
    try:
        name_prep_config = [x for x in attr_cfig.attr_config.get('file_io') if 'name_prep_config' in x][0]['name_prep_config']
        path_prep_config = fsutil.build_cfig_path(path_pred_config, name_prep_config)
        
        col_schema_df = pem.read_schm_ls_of_dict(path_prep_config)
        formulation_id = pem.std_form_id(col_schema_df)
        logging.info(f"Resolved formulation table name: {formulation_id}")
    except Exception as e:
        logging.error(f"Failed to resolve prep config or formulation ID: {e}")
        sys.exit(1)

    # Standard output directories
    dirs_std_dict = fsutil.fs_save_algo_dir_struct(dir_base)
    
    # Define destination subdirectory based on the config file's parent folder
    subdir = path_pred_config.parent.name
    dir_regionalization_sub = Path(dirs_std_dict.get('dir_out')) / "regionalization" / subdir
    dir_regionalization_sub.mkdir(parents=True, exist_ok=True)
    
    # --- Read the source GPKG path from prediction config ---
    path_hf_finl_gpkg_raw = pred_cfg.pred_cfg_dict.get('path_hf_finl_gpkg')
    if path_hf_finl_gpkg_raw:
        path_hf_finl_gpkg = Path(fsutil.resolve_fstrings(path_hf_finl_gpkg_raw, context))
        logging.info(f"Master GPKG identified as: {path_hf_finl_gpkg}")

    # Define output GPKG path as a master compiled database
    path_output_sql = dir_regionalization_sub / "compiled_regionalized_params.sqlite"
    logging.info(f"Local compiled GPKG will be generated at: {path_output_sql}")

    # The previously saved mapped parameters are inside output/regionalization/{ds}
    dir_regionalization_ds = Path(dirs_std_dict.get('dir_out')) / "regionalization"
    dir_out_alg_base = Path(dirs_std_dict.get('dir_out_alg_base'))

    # =========================================================================
    # PART 1: Build the Comprehensive Local sqlite db
    # =========================================================================
    for ds in datasets:
        dir_ds_regn = dir_regionalization_ds / ds
        dir_out_alg_ds = dir_out_alg_base / ds

        for resp_var in resp_vars:
            # Dynamically discover algorithm permutations (e.g. kmeans_k8, gower_agglomerative_k12)
            dynamic_algos = fsutil.discover_dynamic_algos(
                search_dir=dir_out_alg_ds,
                base_algos=base_algos,
                metric=resp_var,
                dataset_id=ds,
                file_prefix="algo_",
                file_extension=".joblib"
            )

            if not dynamic_algos:
                logging.warning(f"No trained algorithms found for {resp_var} in {ds}. Skipping.")
                continue

            for algo in dynamic_algos:
                # Search for the mapped crosswalk file first, fallback to standard params
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

                # Determine identifier column dynamically (defaults to divide_id if present, else featureID)
                id_col = 'divide_id' if 'divide_id' in df_params.columns else 'featureID'
                
                if id_col not in df_params.columns:
                    id_col = df_params.columns[0]
                    logging.warning(f"Expected identifier column not found. Defaulting to first column: '{id_col}'.")
                    
                # --- SQLite Database Operations for Local Compiled GPKG ---
                try:
                    with sqlite3.connect(path_output_sql) as conn:
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{formulation_id}'")
                        table_exists = cursor.fetchone()[0] == 1
                        
                        if not table_exists or overwrite_sql:
                            if overwrite_sql and table_exists:
                                logging.info(f"Overwrite flag is True. Replacing existing table '{formulation_id}' in compiled GPKG.")
                            else:
                                logging.info(f"Table '{formulation_id}' does not exist. Creating new table in compiled GPKG.")
                                
                            # Set the index to the identifier column and write
                            df_params_to_write = df_params.set_index(id_col)
                            df_params_to_write.to_sql(formulation_id, conn, if_exists='replace', index=True)
                            register_gpkg_attributes_table(conn, formulation_id)
                            logging.info(f"Successfully wrote {len(df_params_to_write)} rows to table '{formulation_id}'.")
                            
                        else:
                            logging.info(f"Table '{formulation_id}' already exists. Checking for missing {id_col}s...")
                            existing_ids_query = f"SELECT {id_col} FROM '{formulation_id}'"
                            try:
                                df_existing = pd.read_sql_query(existing_ids_query, conn)
                                existing_id_set = set(df_existing[id_col].astype(str))
                                
                                df_params[id_col] = df_params[id_col].astype(str)
                                df_new_records = df_params[~df_params[id_col].isin(existing_id_set)]
                                
                                if not df_new_records.empty:
                                    logging.info(f"Found {len(df_new_records)} new records. Appending to table '{formulation_id}'.")
                                    # Set the index before appending
                                    df_new_records_to_write = df_new_records.set_index(id_col)
                                    df_new_records_to_write.to_sql(formulation_id, conn, if_exists='append', index=True)
                                    register_gpkg_attributes_table(conn, formulation_id)
                                    logging.info("Append complete.")
                                else:
                                    logging.info("No new records found. Local compiled table is already up to date.")
                            except sqlite3.OperationalError:
                                logging.error(f"Identifier column '{id_col}' missing in the existing table. Cannot append.")
                                
                except sqlite3.Error as e:
                    logging.error(f"SQLite error occurred while writing to local compiled GPKG: {e}")
                    sys.exit(1)


    # =========================================================================
    # PART 2: Transfer Tables to Master GeoPackage
    # =========================================================================
    if not path_hf_finl_gpkg.exists():
        logging.error(f"Master GPKG defined in config does not exist: {path_hf_finl_gpkg}")
        logging.warning("Skipping transfer to Master GPKG.")
        sys.exit(0)

    # Copy the hydrofabric gpkg to the same dir as path_output_sql
    dest_path = path_output_sql.parent / path_hf_finl_gpkg.name.replace('.gpkg','_regn.gpkg')
    if not Path(dest_path).exists(): # Copy the hydrofabric .gpkg
        shutil.copy2(path_hf_finl_gpkg, dest_path) # copy2 preserves file metadata

    logging.info(f"Transferring compiled tables to the Master GeoPackage: {dest_path.name}")
    try:
        # Open connections to both databases
        with sqlite3.connect(path_output_sql) as conn_compiled:
            # Query all table names from the local compiled database
            cursor_compiled = conn_compiled.cursor()
            cursor_compiled.execute("SELECT name FROM sqlite_master WHERE type='table'")
            compiled_tables = [row[0] for row in cursor_compiled.fetchall()]
            
            with sqlite3.connect(dest_path) as conn_master:
                for table_name in compiled_tables:
                    logging.info(f"Processing table '{table_name}' for transfer...")
                    
                    # Read the table from the compiled DB
                    df_transfer = pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn_compiled)
                    
                    # Ensure the ID column is set as the index before transferring
                    transfer_id_col = 'divide_id' if 'divide_id' in df_transfer.columns else df_transfer.columns[0]
                    if transfer_id_col in df_transfer.columns:
                        df_transfer.set_index(transfer_id_col, inplace=True)

                    cursor_master = conn_master.cursor()
                    cursor_master.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                    master_table_exists = cursor_master.fetchone()[0] == 1
                    
                    if not master_table_exists or overwrite_sql:
                        # Write or completely replace the table in the Master GPKG
                        df_transfer.to_sql(table_name, conn_master, if_exists='replace', index=True)
                        register_gpkg_attributes_table(conn_master, table_name)
                        logging.info(f"Successfully wrote/replaced table '{table_name}' in Master GPKG.")
                    else:
                        # Append logic for the Master GPKG
                        logging.info(f"Table '{table_name}' exists in Master GPKG. Checking for new records...")
                        existing_ids_query = f"SELECT {transfer_id_col} FROM '{table_name}'"
                        try:
                            df_existing_master = pd.read_sql_query(existing_ids_query, conn_master)
                            existing_id_set_master = set(df_existing_master[transfer_id_col].astype(str))
                            
                            # Reset index temporarily to filter
                            df_transfer_reset = df_transfer.reset_index()
                            df_transfer_reset[transfer_id_col] = df_transfer_reset[transfer_id_col].astype(str)
                            df_new_master = df_transfer_reset[~df_transfer_reset[transfer_id_col].isin(existing_id_set_master)]
                            
                            if not df_new_master.empty:
                                # Set index back before appending
                                df_new_master.set_index(transfer_id_col, inplace=True)
                                df_new_master.to_sql(table_name, conn_master, if_exists='append', index=True)
                                register_gpkg_attributes_table(conn_master, table_name)
                                logging.info(f"Appended {len(df_new_master)} new records to Master GPKG.")
                            else:
                                logging.info("No new records to append to Master GPKG.")
                        except sqlite3.OperationalError as e:
                            logging.error(f"Could not append to Master GPKG table '{table_name}': {e}")
                            
    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred while transferring to Master GPKG: {e}")
        sys.exit(1)

    logging.info("FINISHED successfully transferring parameters to Master GeoPackage.")