# Aggregate attribute data values across individual calibration basins and write to file as a singular dataset


import pandas as pd
import geopandas as gpd
from pathlib import Path
import rafts_algo.utils as raftsutil
import ast
import logging
from typing import Union, List
import pyarrow.parquet as pq
import dask.dataframe as dd

def read_hfatlas_subset_dask(
    paths_hfatl: Union[Path, str, List[Union[Path, str]]], 
    subset_ids: Union[list, tuple, pd.Series],
    attrs_sel: Union[list, str] = 'all', 
    map_id_col: str = "divide_id"
) -> pd.DataFrame: 
    """
    Highly efficient implementation to read specific rows and columns from Parquet files.
    Uses Predicate Pushdown to filter IDs at the engine level before loading into RAM.
    """
    if not isinstance(paths_hfatl, list):
        paths_hfatl = [paths_hfatl]
        
    # Ensure subset_ids is a standard Python list for PyArrow compatibility
    if not isinstance(subset_ids, list):
        subset_ids = list(subset_ids)
        
    # 1. Expand directories into a list of specific parquet files
    all_files = []
    for p in paths_hfatl:
        p_obj = Path(p)
        if p_obj.is_dir():
            all_files.extend(list(p_obj.rglob("*.parquet")))
        elif p_obj.is_file() and p_obj.suffix == '.parquet':
            all_files.append(p_obj)
            
    if not all_files:
        logging.error("No valid parquet files found in the provided paths.")
        return pd.DataFrame()

    ddrafts_to_merge = []
    found_attrs = set()

    # 2. PEEK phase: Read only the schema metadata
    for file in all_files:
        col_mapping = get_cleaned_parquet_schema(file)
        
        if not col_mapping:
            continue

        if map_id_col not in col_mapping:
            logging.warning(f"File {file.name} is missing the index col '{map_id_col}'. Skipping.")
            continue

        # Determine which requested attributes actually exist in this file
        if attrs_sel == 'all':
            available_clean_cols = [col for col in col_mapping.keys() if col != map_id_col]
        else:
            available_clean_cols = [col for col in attrs_sel if col in col_mapping]
            
        logging.info(f"Found {len(available_clean_cols)} requested attributes in {file.name}")
        found_attrs.update(available_clean_cols)

        # 3. LAZY LOAD phase with PREDICATE PUSHDOWN
        raw_id_col = col_mapping[map_id_col]
        raw_cols_to_load = [raw_id_col] + [col_mapping[col] for col in available_clean_cols]
        rename_dict = {col_mapping[col]: col for col in [map_id_col] + available_clean_cols}

        # The PyArrow filter syntax requires Disjunctive Normal Form (DNF): [[(col, op, val)]]
        row_filter = [[(raw_id_col, 'in', subset_ids)]]

        # Tell Dask to apply the row filter at the exact moment of reading the Parquet blocks
        ddf = dd.read_parquet(
            file, 
            columns=raw_cols_to_load, 
            filters=row_filter, 
            engine='pyarrow'
        )
        
        ddf = ddf.rename(columns=rename_dict)
        ddf = ddf.set_index(map_id_col)
        ddrafts_to_merge.append(ddf)

    if not ddrafts_to_merge:
        logging.warning("None of the requested attributes were found.")
        expected_cols = [map_id_col] if attrs_sel == 'all' else [map_id_col] + (attrs_sel if isinstance(attrs_sel, list) else [])
        return pd.DataFrame(columns=expected_cols)

    # 4. MERGE phase
    logging.info("Building Dask merge graph...")
    combined_ddf = ddrafts_to_merge[0]
    for i in range(1, len(ddrafts_to_merge)):
        combined_ddf = combined_ddf.join(ddrafts_to_merge[i], how='outer')

    # 5. COMPUTE phase
    logging.info("Executing computations and pulling subset to memory...")
    combined_df = combined_ddf.compute().reset_index()

    if isinstance(attrs_sel, list):
        miss_cols = [col for col in attrs_sel if col not in found_attrs]
        if len(miss_cols) > 0:
            logging.warning(f"Missing requested attributes across all files: {miss_cols}")
        
    return combined_df

def get_cleaned_parquet_schema(file_path: Union[str, Path]) -> dict:
    """
    Reads a Parquet file's schema metadata by extracting exactly 1 row.
    This guarantees we get the exact Pandas column names and avoids PyArrow 
    internal structural names (like 'element') that crash Dask.
    """
    try:
        # Extract exactly 1 row to see the true Pandas column representation
        pf = pq.ParquetFile(file_path)
        first_batch = next(pf.iter_batches(batch_size=1))
        raw_cols = first_batch.to_pandas().columns.tolist()
    except Exception as e:
        logging.warning(f"Could not read schema for {Path(file_path).name}: {e}")
        return {}

    col_mapping = {}
    for raw_col in raw_cols:
        if raw_col.startswith("('") and raw_col.endswith("')"):
            try:
                # Extract the first element of the tuple string
                clean_col = ast.literal_eval(raw_col)[0]
                col_mapping[clean_col] = raw_col
            except (ValueError, SyntaxError):
                col_mapping[raw_col] = raw_col
        else:
            col_mapping[raw_col] = raw_col
            
    return col_mapping


# 1. Define paths
home_dir = Path.home()
dir_hydfab_calib = home_dir / Path("noaa/hydrofabric/hf22_apr26cal/selected_subsets_edited_geom/sites_new_ngsh_edited_geom").expanduser()    
path_preds = home_dir / "noaa/regionalization/data/raw/hfatlas_hf22/hf22_predictors_CONUS_final.parquet"
path_params = home_dir / "noaa/regionalization/data/raw/hfatlas_hf22/hf22_parameters_CONUS_final.parquet"
path_output = home_dir / "noaa/regionalization/data/raw/x300calib/x300apr26calib_agg_hfatlas.parquet"
id_col = 'divide_id'
# 2. Load the massive CONUS datasets ONCE and merge them
print("Loading and merging CONUS Parquet files...")

data_cols = ['lka_stats', 'ele_stats', 'ari_ix_stats', 'slp_dg_mean',
       'imperv_surf', 'Forest_Cover_Extent_L07', 'b', 'satdk', 'satpsi',
       'slope', 'maxsmc', 'wltsmc', 'structure.vegtyp', 'structure.sfctyp',
       'area_sqkm', 'centroid_lat', 'centroid_lon', 'snw_pc_max', 'snd_pct',
       'cly_pct',
       'gw_Expon', 'gw_Zmax', 'aet_rootzone', 'alpha_fc',
       'location.azimuth', 'TOT_PPT9120_ANN_hfa', 'TOT_Wet_hfa',
       'TOT_WB5100_yr_min_hfa', 'TOT_AET_hfa', 'TOT_EVI_JFM_2012_hfa',
       'TOT_CLAYAVE_hfa', 'TOT_PRSNOW_hfa', 'TOT_RH_hfa', 'TOT_Dry_hfa',
       'TOT_BFI_hfa', 'TOT_SRL55AG_hfa', 'TOT_SRL25AG_hfa', 'TOT_TWI_hfa',
       'TOT_SANDAVE_hfa', 'TOT_SILTAVE_hfa', 'TOT_ELEV_MEAN_hfa',
       'TOT_IMPV01_hfa', 'TOT_RECHG_hfa', 'TOT_Intensity_hfa',
       'TOT_TMIN7100_JAN_hfa', 'TOT_TMIN7100_FEB_hfa', 'TOT_TMIN7100_MAR_hfa',
       'TOT_TMIN7100_APR_hfa', 'TOT_TMIN7100_MAY_hfa', 'TOT_TMIN7100_JUN_hfa',
       'TOT_TMIN7100_JUL_hfa', 'TOT_TMIN7100_AUG_hfa', 'TOT_TMIN7100_SEP_hfa',
       'TOT_TMIN7100_OCT_hfa', 'TOT_TMIN7100_NOV_hfa', 'TOT_TMIN7100_DEC_hfa',
       'TOT_AWCAVE_hfa', 'CAT_RFACT_hfa', 'TOT_TAV7100_MAR_hfa',
       'TOT_TAV7100_APR_hfa', 'swc_pc_s05_hfa', 'glc_pc_c18_hfa',
       'aet_mm_s03_hfa']


# ---------------------------------------------------------
# PHASE 1: Build a master mapping of gage_id -> divide_id
# ---------------------------------------------------------
print("Scanning GPKG files to build ID mapping...")
mapping_list = []

for gpkg_path in dir_hydfab_calib.glob("*.gpkg"):
    gage_id = gpkg_path.name.replace(".gpkg", "").replace("gage_", "")
    
    # Read just the divide_id column from the GPKG to save RAM/time
    gdf = gpd.read_file(gpkg_path, engine="pyogrio", layer='divides', columns=['divide_id'])
    
    for div_id in gdf['divide_id']:
        mapping_list.append({'gage_id': gage_id, 'divide_id': div_id})

# Create a mapping dataframe and a unique list of all divides we'll ever need
df_mapping = pd.DataFrame(mapping_list)
all_needed_divides = df_mapping['divide_id'].unique().tolist()

# ---------------------------------------------------------
# PHASE 2: Read Parquet data ONCE using Predicate Pushdown
# ---------------------------------------------------------
print(f"Reading Parquet data for {len(all_needed_divides)} unique divides...")
df_all_pred_params = read_hfatlas_subset_dask(
    paths_hfatl=[path_params, path_preds], 
    subset_ids=all_needed_divides,
    attrs_sel='all', 
    map_id_col="divide_id"
)

# Ensure divide_id is a column for the upcoming merge (if it was set as index)
if df_all_pred_params.index.name == 'divide_id':
    df_all_pred_params = df_all_pred_params.reset_index()

# ---------------------------------------------------------
# PHASE 3: Clean and unpack columns ONCE
# ---------------------------------------------------------
print("Unpacking and formatting columns...")
for col in data_cols:
    if col in df_all_pred_params.columns:
        # Unpack if PyArrow loaded it as a dictionary/list
        if df_all_pred_params[col].dtype == 'object':
            df_all_pred_params[col] = df_all_pred_params[col].apply(
                lambda x: list(x.values())[0] if isinstance(x, dict) else (
                          x[0] if isinstance(x, (list, np.ndarray)) else x)
            )
        # Force to numeric
        df_all_pred_params[col] = pd.to_numeric(df_all_pred_params[col], errors='coerce')

# ---------------------------------------------------------
# PHASE 4: Merge, Group, and Aggregate!
# ---------------------------------------------------------
print("Aggregating data by gage_id...")

# Merge the cleaned data with our gage mapping
df_merged = pd.merge(df_mapping, df_all_pred_params, on='divide_id', how='inner')

# Build the agg_dict securely
agg_dict = {
    col: 'mean' 
    for col in data_cols 
    if col != 'area_sqkm' and col in df_merged.columns
}
if 'area_sqkm' in df_merged.columns:
    agg_dict['area_sqkm'] = 'sum'

# Perform the aggregation across all gages simultaneously
df_final = df_merged.groupby('gage_id').agg(agg_dict).reset_index()

# Create parent directories if they don't exist
path_output.parent.mkdir(parents=True, exist_ok=True)

# Save as Parquet (or use .to_csv(path_output, index=False) if you prefer CSV)
df_final.to_parquet(path_output, index=False)

print(f"Successfully wrote aggregated data for {len(df_final)} gages to {path_output}")