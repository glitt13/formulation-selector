# RaFTS Workflow: hydrofabric and custom-generated predictors

This document describes the workflow for preparing, aggregating, and training (pooling) models within the RaFTS (Regionalization and Formulation Testing System) framework using the hydrofabric and corresponding predictors (e.g. hfATLAS).

---

## I. Workflow Overview

The RaFTS hfATLAS workflow is executed in a specific sequence to process raw input datasets, aggregate spatial attributes & format them, train machine learning algorithms, and ultimately perform out-of-sample predictions using algorithms deemed acceptable for prediction by the user. A shell script contained in the same workflow directory as the corresponding config files is recommended for running all of these steps sequentially (e.g., `hfatl_test_proc_rafts_all.sh`).

Note additional configurations/workflows may not be tracked in this repo, but stored in [NOAA gdrive](https://drive.google.com/drive/folders/1ghSVlE890S3LXir1-JhexDlNf1zT6T6h?usp=drive_link)

1. **Preparation**: Custom dataset munging is performed first to prepare the initial response variable dataset and write to file in a standardized NetCDF (`.nc`). Typically named `prep_*.py` inside the same directory as the corresponding config files. 


2. **Aggregation**: Hydrofabric-based watershed attributes are aggregated based on the gage_id representing the basins of interest. Both methods create the RaFTS-standard form of watershed attribute data, whose column names include `['featureID', 'featureSource',  'data_source', 'dl_timestamp', 'attribute', 'value']`.
 - `fs_agg_hfatl_basin.py`: aggregates hydrofabric divides to a larger basin scale.
 - `fs_hfatlas_to_rafts_prep.py`: uses existing location identifiers (e.g. `divide_id`) and performs no aggregation.

### Understanding `fs_agg_hfatl_basin.py` vs. `fs_hfatlas_to_rafts_prep.py`

Both scripts prepare the watershed attribute data and the choice depends on whether the input data have been aggregated to the desired scale (i.e. basin size). Preferably, the input watershed attribute data have already been aggregated to same scale as the response variables. In that case, `fs_hfatlas_to_rafts_prep.py` is the method.
These run after the initial custom prep script that generates the response variable `.nc` file. 


* **`fs_hfatlas_to_rafts_prep.py`**:  This script formats hfATLAS attributes into RaFTS-compatible attributes. It is intended to run when the raw response variable and corresponding attribute data are already described at the native `featureID` level such as a USGS gaged basin and therefore need no additional aggregation. 


* **`fs_agg_hfatl_basin.py`**: Not recommended unless attribute data have not been pre-processed to the desired basin scale. This script is used specifically in cases where hydrofabric divides need to be aggregated to a larger scale. It reads a hydrofabric GPKG, extracts the divide-to-gage mapping, loads the raw hfATLAS attributes, and aggregates them to the gage level. It outputs analysis-ready aggregated attributes. Note that aggregation is assumed to take the area-weighted average of attributes across divides, unless it is an area column in which case the sum is taken. Code modification will be required for exceptions. A simpler approach would be to use aggregation methods via the custom attribute pre-processing tool `hfATLAS`.


3. **Training & Testing**: Algorithms are trained and tested in parallel on the catchment attribute data to predict formulation metrics or hydrologic signatures. There are a few different algorithm training scripts contained inside `pkg/fs_algo/fs_algo/flow/`:
 - `fs_proc_algo_pool.py`: The computationally-efficient parallelized algorithm training script. Recommended, especially when using the current workflow, with pre-processed attribute data.
 - `fs_proc_algo_viz.py`: The legacy script which is kept in case historic workflow runs are desired (e.g. `/scripts/workflow_configs/legacy/xssa_us/`)

4. **Prediction **  Using trained algorithms to make predictions of response variables in out-of-sample locations. The resulting predictions and their associated uncertainties are plotted onto static spatial maps across the hydrofabric.

 - `fs_pred_algo.py`: Generates the predictions and optionally calculates uncertainty bounds (e.g., MAPIE prediction intervals or ForestCI).


5. **Mapping**: The resulting predictions and their associated uncertainties are plotted onto static spatial maps across the hydrofabric.


 - `fs_map_pred_hfatl.py`: Generates static `.png` maps of the predicted values and uncertainty bounds joined dynamically to the hydrofabric geometries.

6. **Donor-Receiver Pairing**: Used for unsupervised algorithm-only algorithms. 

- `fs_pair_donors.py`: Pairs donor and receiver basins within the same cluster based on euclidean distance.

Note: The attribute transformation workflow has been deprecated for the current processing workflows. It's recommended to perform desired transformations on the input attribute data beforehand. If attribute transformation is desired, updates will need to be made to the prediction step.

---

## II. Configuration Files

The workflow relies on a suite of YAML configuration files to dictate file I/O, dataset schemas, and algorithm hyperparameters. Different datasets/workflows have different sets of config files, as stored inside `scripts/workflow_configs/{workflow_subdir}/`

### 1. Preparation Config (e.g. `*_prep_config.yaml`)

Defines the required column mappings, file paths, and metadata for the response variable dataset.

* **`col_schema`**:
* `gage_id`: The basin identifier/gage id used for each modeled location. (Required).


* `featureID`: The Python f-string format converting the `gage_id` to a standardized featureID. (Required).


* `featureSource`: The standardized nhdplusTools featureSource (e.g., `hfv22_id`). (Required). This is used to distinguish different types of watershed attribute datasets and is assigned to the `featureSource` column in the attribute data.  In legacy forms of RaFTS that use the `proc.attr.hydfab` R-package, it may be used to designate the data source from NHDPlus attribute retrieval (e.g. 'nwissite'). In normal hydrofabric-based workflows using pre-existing attribute data, it is used to represent the hydrofabric version corresponding to the attribute data (e.g. `hfv22_id`, or `hfv40_id`).


* `metric_cols` & `metric_mappings`: Column(s) in the raw response variables dataset (`'metric_cols'`) and the mapped column names to be used for the dataset and all further processing. In other words, `'metric_cols'` gives the user the option to rename the response variables' data columns. (Required).


* **`file_io`**:
* `home_dir`: The base home directory. If not specified, the default `~/` will be used. (Optional).

* `dir_data` or `path_data`: Where the raw response variable data are stored. This is flexible according to the custom prep script created by the user to ingest & munge the data. (Required).

* `data_source`: This is a column in the attribute dataset to specify the data source. Required when providing attribute data using the standard RaFTS workflow with pre-processed attribute data. This specifies attribute dataset version (may need to update with major version changes). If using the legacy attribute retrieval workflow (aka `proc.attr.hydfab` R-package), then this is not required.

* `dir_save`: The save location of standardized output. (Required).


* `save_type`: Save as hierarchical files. Should be `netcdf`. (Required).


* `save_loc`: Use `local` for saving to a local path. (Required).


* `path_hf_gpkg`: The path to the gpkg containing the hfATLAS divides, required for mapping the divide_id. (Required).


* `path_hf_basins_gpkg`: The path to the gpkg containing the divide_ids as they correspond to basins of interest. (Optional; expected by `fs_agg_hfatl_basin.py` for aggregation).


* `gage_id_col_gpkg`: The gage_id column in the `path_hf_basins_gpkg`, if present. Otherwise do not provide this entry in the config file and specify `gpkg_filename_pattern` instead, where the gage_id is contained inside the filename.

* `gpkg_filename_pattern`: The pattern used to extract the gage_id from the .gpkg filenames contained inside path_hf_basins_gpkg when it's a dir w/ many gpkg files. Default `'gage_(.*).gpkg'`. Use `gage_id_col_gpkg` instead if gage_id is specified in a column inside the .gpkg.

### Attribute prep config requirements
Some options are specific to the next step in the standard workflow using pre-defined attributes. These requirements may vary by the choice of script (`fs_hfatlas_to_rafts_prep.py` vs. `fs_agg_hfatl_basin.py`).

#### `fs_hfatlas_to_rafts_prep.py`
When working on the individual divide scale, the entire hydrofabric may be considered as an input dataset. Organizing the attributes and hydrofabric by vpu subdirectories offers data reading efficiencies. Otherwise, smaller datasets do not need to be organized by vpu.
* **`file_io`**:
* `vpu_mapped`:  Boolean str. Should output attribute data be organized by vpu? Recommended to be True for large datasets, e.g. entire CONUS scale. If set to False, the vpuid will be assigned as 'all' (appropriate for smaller datasets). More details in `fs_algo.utils.hfatl_hf_cmbo_wrap()`. The following flowpath and vpu config spec options pertain to `vpu_mapped=True`.

* `hf_fp_layer` Hydrofabric name for the flowpaths layer, used for organizing file write of data by vpu

* `hf_fp_id_col` The column name in the flowpaths layer corresponding to the flowpath outlet of the divide, used for organizing file write of data by vpu

* `vpu_id_col` The column name in the hydrofabric flowpaths layer, , used for organizing file write of data by vpu

#### Both `fs_agg_hfatl_basin.py` and `fs_hfatlas_to_rafts_prep.py`
When wanting to aggregate hydrofabric divides to a larger scale, the following config options in the prep config specify the aggregation scale for algorithm training. Ideally, this capability is ignored if the attribute data have already been aggregated in a previous step.
* **`file_io`**
* `path_hf_basins_gpkg`  The filepath or dir to the gpkg(s) containing the divide_ids as they correspond to basins of interest (e.g. individual .gpkg files representing each calibration basin).
* `gpkg_filename_pattern` Optional string pattern used to isolate the gage_id from the gpkg filenames inside the `path_hf_basins_gpkg` directory. (Default 'gage_{gage_id}.gpkg')

**`formulation_metadata**:
* `dataset_name`: The name to be assigned to this dataset. This name will be used to create a subdirectory inside `user_data_std`. (Required).
**NOTE** the dataset name of interest must also be specified in the attribute config.
This was initially created to handle processing multiple datasets all at once, but present workflows have made it only possible to processing one dataset per config file (e.g. a required assumption in `fs_agg_hfatl_basin.py`). 
Refer to `scripts/workflow_configs/legacy/ealstm/` for an example of processing multiple datasets.



### 2. Attribute Config (e.g.`*_attr_config.yaml`)

Configures the acquisition of catchment attributes corresponding to standard-named locations.

* **`file_io`**:
* `dir_base`: The base save location. (Required).


* `dir_std_base`: The location of standardized data generated by the `fs_prep` package. (Required).


* `dir_db_attrs`: The parent dir where each dataset's parquet attributes are stored. (Required).


* `ds_type`: A string used in the filename of the output metadata (e.g., `training`). (Required).


* `write_type`: Filetype for writing NLDI feature metadata. Strongly recommend default `'parquet'`. (Required). 


* `name_prep_config`: The name of the prep config file. (Required when running the hfatlas-based workflow).

* `path_meta`: Ignored when processing pre-existing attribute data (although not ignored in the prediction config). For the attribute config, this is used in the legacy RaFTS workflow's `proc.attr.hydfab` processing. Training attribute metadata filepath formatted for R's glue or py f-string, as generated using `proc.attr.hydfab::write_meta_nldi_feat()`. Strongly suggested default:  "{dir_std_base}/{ds}/nldi_feat_{ds}_{ds_type}.{write_type}"


* **`formulation_metadata`**:
* `datasets`: The dataset names to select for further processing. (Required).
**NOTE** the dataset names of interest must also be specified in the prep config.
This was initially created to handle processing multiple datasets all at once, but present workflows have drifted towards just processing one dataset at a time. 
Refer to `scripts/workflow_configs/legacy/ealstm/` for an example of processing multiple datasets.


* **`attr_select`**:
* `paths_hfatl`: Path to tabular HydrofabricATLAS parquet files. This is required in the current workflow where the user provides the attribute data. This can be the CONUS-scale attribute data when performing basin aggregation via `fs_agg_hfatl_basin.py`. (Required).


* `hfatl_vars`: A list of the specific attribute data column names to use as predictors when training algorithms. (Required).





### 3. Algorithm Config (e.g. `*_algo_config.yaml`)

Configures the training and testing of algorithms that predict formulation metrics or hydrologic signatures.

* `task_type`: The type of machine learning task (e.g., `clustering`). (Required for unsupervised runs).


* `algorithms`: Selected algorithm(s) to run, such as `kmeans`, `gower_agglomerative`, `rf`, or `mlp`. (Required).


* `test_size`: The proportion of the dataset reserved for testing. (Required).


* `seed`: The starting point for the random number generator. (Required).


* `name_attr_config`: Name of the corresponding dataset's attribute configuration file. (Required).


* `read_type`: Must use `all` for anything using custom-generated predictor (attributes) data. (Required).


* `verbose`: Should the train/test/eval provide printouts on progress? (Optional).


* `make_plots`: Should plots be created & saved to file? (Optional).


* `uncertainty`: Defines methods to quantify uncertainty in model training and predictions, supporting methods like `bagging`, `forestci`, and `mapie`. (Optional).


### 4. Prediction Config (e.g. *_pred_config.yaml)

Configures the out-of-sample prediction step and downstream mapping.

* `name_attr_config`: Name of the corresponding attribute configuration file. (Required).


* `name_algo_config`: The name of the trained algorithm configuration file. (Required).


* `name_tfrm_config`: The name of the transformation configuration file. (Required if transforming predictor data).


* `ds_type`: A string identifying the output dataset, highly recommended to be set to `prediction`. (Required).


* `write_type`: Filetype for the feature metadata output, defaulting to `parquet`. (Required).


* `path_meta`: This is where the prediction attribute data are stored. Not to be confused with `path_meta` in the attribute config file, which is what is used for defining the training predictor dataset location when using the legacy `proc.attr.hydfab` workfklow. However, in the legacy RaFTS workflow's `proc.attr.hydfab` processing, the strongly suggested default is the same as the attribute config entry: `"{dir_std_base}/{ds}/nldi_feat_{ds}_{ds_type}.{write_type}"`, where `ds_type` would be `'prediction'` rather than `'training`'. Required.


* `pred_file_comid_colname`: The column name containing the location identifiers used for prediction within the `path_meta` file. (Required).


* `path_tfrm_script` & `conda_env`: Filepath to the transformation script and its execution environment. (Required if transforming data).


* `algo_response_vars`: A list of the desired response variables or metrics to predict. (Required).


* `algo_type`: A list of the trained regressor algorithms desired for prediction, such as `rf` or `mlp`. (Required).


* `MAPIE_alpha`: Alpha parameters (between 0 and 1) in an array format to calculate MAPIE prediction intervals. (Optional).


* `uncn_bnd_pred`: Boolean flag determining whether physical minimum/maximum bounds should be applied when calculating uncertainty. (Optional).


---

## III. Input Dataset Description Template

When describing the input datasets within the RaFTS workflow, use the following template based on the `formulation_metadata` and `references` YAML schema. These metadata are mostly optional/ignored in further processing step.

```yaml
formulation_metadata:  
  - 'dataset_name': '' # [Required] The unique name of the dataset folder (e.g., 'hfatl_huc12_clust')
  - 'formulation_base': '' # [Required] Basename of formulation (e.g., 'test_huc12')
  - 'formulation_id': '' # [Optional] Alternative to automatically generated ID
  - 'formulation_ver': '' # [Optional] Version of the formulation
  - 'temporal_res': '' # [Optional] The temporal resolution corresponding to the modeled data
  - 'target_var': '' # [Required] The target variable modeled (e.g., 'param')
  - 'start_date': '' # [Required] The YYYY-MM-DD start date of the modeled timeseries
  - 'end_date': '' # [Required] The YYYY-MM-DD end date of the modeled timeseries
  - 'modeled notes': '' # [Optional] Any notes describing the dataset or test case
  - 'cal_status': '' # [Required] Was the formulation model fully calibrated? ('Y','N', or 'S')
  - 'start_date_cal': '' # [Optional] The YYYY-MM-DD start date of the calibration period
  - 'end_date_cal': '' # [Optional] The YYYY-MM-DD end date of the calibration period
  - 'cal_notes': '' # [Optional] Notes regarding the calibration process

references: 
  - 'input_filepath': '' # [Optional] Path to the raw input data file
  - 'source_url': '' # [Optional] URL where the data was sourced
  - 'dataset_doi': '' # [Optional] DOI for the dataset
  - 'literature_doi': '' # [Optional] DOI for associated literature

```


## IV. Data Requirements

To successfully execute the RaFTS hfATLAS workflow, you must provide three primary categories of input data: the target response variables, the spatial hydrofabric framework, and the predictor attributes. If you intend to run out-of-sample predictions, a fourth dataset of prediction locations is also required.

### 1. Response Variable Data (The "Targets")
This dataset contains the known values that the machine learning algorithms will be trained to predict. These are typically modeled outputs such as calibrated parameters, hydrologic signatures, parameter sensitivities, or formulation evaluation metrics (e.g., NSE, KGE).
* **Format:** Varies originally (e.g., `.csv`, `.parquet`, `.txt`), but your custom `prep_*.py` script is responsible for ingesting it and standardizing it into a NetCDF (`.nc`) file.
* **Key Constraints:** 
  * Must contain a distinct location identifier column (e.g., `site_id` or `gage_id`).
  * This identifier must map cleanly to the spatial hydrofabric.
* **Config Link:** Defined by `path_data` (or `dir_data`) in the Preparation Config.

### 2. Catchment Predictor Data (e.g., hfATLAS)
These are the physical, climatic, or anthropogenic characteristics (watershed attributes) used as the features/predictors during algorithm training. 
* **Format:** Tabular Parquet files (`.parquet`).
* **Key Constraints:**
  * Must contain a location identifier column that matches the hydrofabric (e.g., `divide_id` or `comid`).
  * Must contain the exact column names specified in the attribute config file's `hfatl_vars`.
  * *Note:* The RaFTS workflow is designed to automatically clean and parse complex PyArrow struct/tuple column headers commonly found in hfATLAS data as a stringified tuple (e.g. `('TOT_AET', 'mm')`), so use the column name without the pint unit.
* **Config Link:** Defined by `paths_hfatl` in the Attribute Config. If performing the basin aggregation step `fs_map_pred_hfatl.py`, the `paths_hfatl` may be from the entire hydrofabric domain.

### 3. Spatial Framework (Hydrofabric Geometries)
The spatial datasets provide the geographical backbone for the workflow, allowing RaFTS to map tabular data to real-world coordinates and generate  maps.
* **Format:** GeoPackage (`.gpkg`), provided as either a single consolidated file or a directory of subset files.
* **Key Constraints:**
  * Must contain the target geometric layers (e.g., `flowpaths`, `divides`).
  * If you are running `fs_agg_hfatl_basin.py` to aggregate attributes to a larger scale, the `.gpkg` **must** contain an explicit mapping linking the internal native divides (e.g., `divide_id`) to the terminal aggregated basin (e.g., `gage_id`).
  * Must contain the Vector Processing Unit identifier (`vpuid`) if VPU-partitioned processing is enabled.
* **Config Link:** Defined by `path_hf_gpkg` and `path_hf_basins_gpkg` in the Preparation Config.

### 4. Prediction Location Metadata (For Out-of-Sample Inference)
If you are moving beyond training/testing and want to use your trained algorithms to predict values in un-modeled locations, you must provide a list of those target locations.
* **Format:** Typically `.parquet` or `.csv`.
* **Key Constraints:** Must contain the location identifiers (e.g., `divide_id` or `featureID`) corresponding to the areas where you have predictor data but lack response variables. 
* **Config Link:** Defined by `path_meta` and `pred_file_comid_colname` in the Prediction Config.




---

## V. Usage Example & Execution Workflow

To run the end-to-end RaFTS pipeline using the modern `uv` Python package manager, execute the following scripts in order. *(Note: Adjust the configuration file paths to match your active directory structure).*

**1. Prepare the initial dataset:**

```bash
uv run python pkg/fs_prep/fs_prep/flow/prep_hfatl_test.py "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_prep_config.yaml"

```

**2. Grab or Aggregate Attributes:**
*(For native divides)*:

```bash
uv run python pkg/fs_prep/fs_prep/flow/fs_hfatlas_to_rafts_prep.py \
    --path_prep_config "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_prep_config.yaml" \
    --name_attr_config "hfatl_attr_config.yaml"

```

*(For basin aggregation)*:

```bash
uv run python pkg/fs_prep/fs_prep/flow/fs_agg_hfatl_basin.py \
    --path_prep_config "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_prep_config.yaml" \
    --path_attr_config "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_attr_config.yaml"

```

**3. Train & Test Algorithms:**

```bash
uv run python pkg/fs_algo/fs_algo/flow/fs_proc_algo_pool.py "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_algo_config_uncn.yaml" --chunk_size 4

```

**4. Perform Out-of-Sample Predictions:**

```bash
uv run python pkg/fs_algo/fs_algo/flow/fs_pred_algo.py "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_pred_config_uncn.yaml"

```

**5. Map Predictions:**

```bash
uv run python pkg/fs_algo/fs_algo/flow/fs_map_pred_hfatl.py "scripts/workflow_configs/00example_configs/hfatl_test2/hfatl_pred_config_uncn.yaml"

```

**6. Pair Donors and Receivers (Only for unsupervised clustering algorithms):**

```bash
uv run python pkg/fs_algo/fs_algo/flow/fs_pair_donors.py "scripts/workflow_configs/00example_configs/hfatl_test_clust/hfatl_pred_config.yaml"

```