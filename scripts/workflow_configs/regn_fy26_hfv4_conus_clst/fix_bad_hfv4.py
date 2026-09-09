'''
Since hfv4 test .gpkg files do not have unique ids for divide-id, this
creates a new column, gageID_divide_id in the .gpkg files' divides and flowpaths layers. 
'''
import re
import shutil
import sqlite3
import logging
import argparse
from pathlib import Path

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_gpkg_layers(dir_in: str | Path, dir_out: str | Path):
    """
    Reads GPKGs, extracts gage_id from the filename, copies the file, 
    temporarily drops spatial triggers to avoid SQLite errors, updates the 
    divides and flowpaths layers dynamically based on existing schema, 
    and restores the triggers.
    """
    dir_in = Path(dir_in).expanduser()
    dir_out = Path(dir_out).expanduser()
    
    # Create output directory if it doesn't exist
    dir_out.mkdir(parents=True, exist_ok=True)

    # 1. Compile the regex to extract the gage_id
    pattern = re.compile(r"USGS-(.*)-ngen", re.IGNORECASE)

    # 2. Find all GPKG files in the input directory
    gpkg_files = list(dir_in.glob("*.gpkg"))
    if not gpkg_files:
        logging.warning(f"No .gpkg files found in {dir_in}")
        return

    # Define the layers to modify
    layers_to_process = ["divides", "flowpaths"]

    for gpkg_path in gpkg_files:
        # Match the filename to extract the gage_id
        match = pattern.search(gpkg_path.stem)
        if not match:
            continue

        gage_id = match.group(1)
        out_path = dir_out / gpkg_path.name
        logging.info(f"Processing {gpkg_path.name} -> Extracted gage_id: {gage_id}")

        # 3. Copy the entire GPKG 
        shutil.copy2(gpkg_path, out_path)

        # 4. Connect to the copied GeoPackage via SQLite
        try:
            with sqlite3.connect(out_path) as conn:
                cursor = conn.cursor()
                
                # 5. Iterate through the target layers
                for table_name in layers_to_process:
                    try:
                        # Check if the layer actually exists
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                        if not cursor.fetchone():
                            logging.warning(f"  -> '{table_name}' layer not found. Skipping.")
                            continue
                            
                        # Retrieve and temporarily drop the spatial triggers on the table
                        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table_name,))
                        triggers = cursor.fetchall()
                        
                        for trigger_name, _ in triggers:
                            cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

                        # Add the new column (ignore if it already exists)
                        try:
                            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN gageID_divide_id TEXT")
                            logging.info(f"  -> Created column 'gageID_divide_id' in '{table_name}'")
                        except sqlite3.OperationalError:
                            logging.info(f"  -> Column 'gageID_divide_id' already exists in '{table_name}'")
                        
                        # Dynamically check columns to find the right ID to concatenate
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        existing_columns = [row[1] for row in cursor.fetchall()]
                        
                        # Determine which ID column to use
                        if "divide_id" in existing_columns:
                            id_col = "divide_id"
                        elif "id" in existing_columns:
                            id_col = "id"
                        else:
                            logging.warning(f"  -> Neither 'divide_id' nor 'id' found in '{table_name}'. Cannot populate 'gageID_divide_id'.")
                            continue

                        # Execute the update using the dynamically found ID column
                        cursor.execute(f"UPDATE {table_name} SET gageID_divide_id = 'USGS-{gage_id}_' || {id_col}")
                        
                        # Recreate the triggers exactly as they were
                        for _, trigger_sql in triggers:
                            if trigger_sql: 
                                cursor.execute(trigger_sql)
                        
                        # Commit per layer to prevent a failure in one layer from rolling back the other
                        conn.commit()
                        logging.info(f"  -> Successfully populated 'gageID_divide_id' in '{table_name}' using '{id_col}'.")
                        
                    except Exception as layer_error:
                        logging.error(f"  -> Failed processing layer '{table_name}': {layer_error}")
                        conn.rollback() # Rollback only this specific layer's failed transaction
                        
                logging.info(f"  -> Finished processing {gpkg_path.name}.")
                
        except Exception as e:
            logging.error(f"  -> Failed to open or modify {gpkg_path.name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add gageID_divide_id column to divides and flowpaths layers in GPKGs.")
    parser.add_argument("--dir_in", type=str, required=True, help="Input directory containing raw .gpkg files")
    parser.add_argument("--dir_out", type=str, required=True, help="Output directory for modified .gpkg files")
    
    args = parser.parse_args()
    
    process_gpkg_layers(args.dir_in, args.dir_out)
    logging.info("Complete!")