# utils.py
from __future__ import annotations # enables the | operator for function typehints back to python 3.7
import inspect
import numpy as np
import pandas as pd
import xarray as xr
import pynhd as nhd
import dask.dataframe as dd
import os
from collections.abc import Iterable
from typing import List, Optional, Dict
from pathlib import Path
import itertools
import yaml
import logging
import re
from shapely.geometry import Point
import geopandas as gpd
import pyarrow as pa
import pyarrow.dataset as ds
import ast
from sklearn.model_selection import train_test_split

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# %% ALGO CONFIG FILE PARSER
class AlgoConfigParser:
    ## Initialize a new instance of the class AlgoConfigParser
    def __init__(self, path_algo_config: str | os.PathLike):
        self.path_algo_config = path_algo_config
        self.algo_cfg_unc_dict = dict()

    ## Define a function to read algo configuration parameters from the YAML config file
    def _read_algo_config(self ) -> dict:
        """A function that extracts parameters for the algorith and uncertainty from yaml configuration file.
        
        :param path_algo_config: Full path (folder and name of file) of algorithm config file
        :type path_algo_config: str | os.PathLike
        Returns a dictionary with two dictionaries containing parameter values.
            :param algorithms: Selected algorithm(s), refer to AlgoTrainEval.train_algos to see what options are present (e.g. rf, mlp)
            :type algorithms: string
                :param rf: Random Forest algorithm
                :type rf: string, OPTIONAL, STRONGLY RECOMMENDED
                    :Additional rf parameters can be included, refer to sklearn.ensemble.RandomForestRegressor for arguments to pass here - otherwise defaults will be used
                :param mlp: Multi-layer Perceptron Regressor Algorithm
                :type mlp: string, OPTIONAL
                    :Additional mlp parameters can be included, refer to sklearn.neural_network.MLPRegressor for arguments to pass here - otherwise defaults will be used
            :param test_size: The proportion of dataset to be used for testing, defaults to 0.3. All values should be between 0 and 1.
            :type test_size:  float
            :param seed: The starting point for the random number generator, defaults to 32
            :type seed: int
            :param read_type: 'filename' # Optional. Recommend 'filename' for faster data loading. Should all parquet files be lazy-loaded, assign 'all'. Otherwise just files with comids_resp in the file name? assign 'filename'.
            :param read_type: OPTIONAL. All parquet files loaded, assign 'all'. Just files with comids_resp in the filename, assign 'filename'. Defaults to 'all'
            :type read_type: str
            :param metrics: OPTIONAL. The metrics (hydraulic signature identifier) of interest for processing.
            Defaults to 'all', all metrics in the input dataset will be processed
            :type metrics: ??sublist structure??
            :param make_plots: If True plots are created & saved to file. Defaults to False
            :type make_plots: bool, OPTIONAL
            :param same_test_ids: Are all datasets being compared required to have the same test ID? If False, algos will be trained true to the test_size, 
            but the train_test split may not be the same across each dataset (particularly total basins differ). Defaults to True
            :type same_test_ids: bool, OPTIONAL
            :param verbose: Should the train/test/eval provide printouts on progress? Defaults to True.
            :type verbose: bool, OPTIONAL
            :param uncertainty_cfg: Uncertainty analyses to be run. Defaults to "{}"
            :type uncertainty_cfg: str, OPTIONAL
                :param confidence_levels: Confidence levels for confidence interval calculations.  
                :type confidence_levels: list, OPTIONAL, REQUIRED if a value was read for n_algos
                :param forestci: Applies forestci to estimate confidence intervals for the model training based on the variance of predictions from 
                trees in the random forest model. For more details, see: https://github.com/scikit-learn-contrib/forest-confidence-interval
                :type forestci: str, OPTIONAL
                    :param fci_flag: If True use Forestci model to calculate confidence interval for rf model
                    :type fci_flag: bool
                :param bagging: Enables bootstrap aggregating (bagging) to calculate confidence intervals during the model training 
                by training multiple models on resampled data. More broadly applicable than forestci.
                :type bagging: str, OPTIONAL
                    :param n_algos: Number of bootstrap runs for Bagging confidence interval calculation (integer). 
                    Bagging ci calculation is disabled if n_algos is empty
                    :type n_algos: int, OPTIONAL
                :param mapie: Applies the MAPIE (Model Agnostic Prediction Interval Estimator) framework to estimate **prediction intervals**, 
                which provide bounds around individual predicted values. Supports many model types
                See documentation for details: https://mapie.readthedocs.io/en/stable/index.html
                :type mapie: str, OPTIONAL
                    :param alpha: Alpha parameter (0 < α < 1) in an array format to calculate MAPIE prediction intervals
                    :type alpha: list, OPTIONAL, MAPIE prediction interval estimation will be enabled if not empty
                    :param method: MAPIE method: 'plus' (CV+) or 'minmax' (CV-minmax). For more information and other methods, refer to:
                    https://mapie.readthedocs.io/en/stable/theoretical_description_regression.html
                    :type method: str, OPTIONAL, but REQUIRED if MAPIE_alpha provided
                    :param cv: Specifies the number of cross-validation folds
                    :type cv: str, OPTIONAL, but REQUIRED if MAPIE_alpha provided
                    :param agg_function: Option selection, either 'mean' or 'median'
                    :type agg_function: str, OPTIONAL, but REQUIRED if MAPIE_alpha provided
        :return: Dictionary of algorithm input parameters, required and optional

        # Changelog/contributions
        #  2025-07-29 - Converted fs_proc_algo_viz.py config file read section to function, Justin Clark
        #. 2025-10-11 add AlgoTrainEval import & default seed, GL
        """
        
        if not Path(self.path_algo_config).exists():
            logging.error("Ensure that algo config file is defined")
            raise ValueError(f"Ensure that algo config file is defined" )
        
        with open(self.path_algo_config, 'r') as file:
            algo_cfg = yaml.safe_load(file)

        # Algorithm selection and parameters
        algo_config = algo_cfg.get('algorithms')
        if algo_config is None:
            logging.error("Missing required 'algorithms' section in YAML config.")
            raise KeyError("Missing required 'algorithms' section in YAML config.")

        # Ensure the string literal is converted to a tuple for `hidden_layer_sizes`
        if algo_config.get('mlp',None):
            if algo_config['mlp'][0].get('hidden_layer_sizes',None): # purpose: evaluate string literal to a tuple
                algo_config['mlp'][0]['hidden_layer_sizes'] = ast.literal_eval(algo_config['mlp'][0]['hidden_layer_sizes'])
       
        # In some situations, we only care about parsing config files, not running algorithms
        #. The following helps avoid import the algo module
        if not algo_cfg.get('seed') or not algo_cfg.get('test_size'):
            # Need to import here to avoid circular dependency
            try:
                from fs_algo.fs_algo_train import AlgoTrainEval
                # Use the signature of AlgoTrainEval for default values
                sig = inspect.signature(AlgoTrainEval.__init__)  
                seed = sig.parameters['rs'].default
                test_size = sig.parameters['test_size'].default
            except:
                seed = 32
                test_size = 0.3
        else:
            seed = 32
            test_size = 0.3
        # Generate dictionary "algo_cfg_dict" with primary training parameters
        algo_cfg_dict = {'algo_config' : algo_config,
                            'test_size': algo_cfg.get('test_size', test_size), # Must be between 0 and 1
                            'seed': algo_cfg.get('seed',seed ),
                            'read_type': algo_cfg.get('read_type', 'filename'), #DEFAULT to 'filename'
                            'metrics': algo_cfg.get('metrics', None),
                            'make_plots': algo_cfg.get('make_plots', False),
                            'same_test_ids': algo_cfg.get('same_test_ids', True),
                            'verbose': algo_cfg.get('verbose', True),
                            'name_attr_config': algo_cfg.get('name_attr_config', Path(self.path_algo_config).name.replace('algo', 'attr')),
                            'name_attr_csv': algo_cfg.get('name_attr_csv',None),
                            'colname_attr_csv': algo_cfg.get('colname_attr_csv',None)
                            }
        
        algo_cfg_dict['path_attr_config'] = build_cfig_path(self.path_algo_config, algo_cfg_dict['name_attr_config'])

        if not algo_cfg_dict['path_attr_config'].exists():
            logging.error(f"Ensure that 'name_attr_config' as defined inside {self.path_algo_config.name} \
                              \n is also in the same directory as the algo config file {self.path_algo_config.parent}")
            raise ValueError(f"Ensure that 'name_attr_config' as defined inside {self.path_algo_config.name} \
                              \n is also in the same directory as the algo config file {self.path_algo_config.parent}" )

        # Type checks for common required parameters
        if not isinstance(algo_cfg_dict['test_size'], (float, int)):
            logging.error(f"'test_size' must be a float. Got: {type(algo_cfg_dict['test_size'])}")
            raise TypeError(f"'test_size' must be a float. Got: {type(algo_cfg_dict['test_size'])}")
        if not (0 < algo_cfg_dict['test_size'] < 1):
            logging.error(f"'test_size' must be between 0 and 1. Got: {algo_cfg_dict['test_size']}")
            raise ValueError(f"'test_size' must be between 0 and 1. Got: {algo_cfg_dict['test_size']}")
        if not isinstance(algo_cfg_dict['seed'], int):
            logging.error(f"'seed' must be an integer. Got: {type(algo_cfg_dict['seed'])}")
            raise TypeError(f"'seed' must be an integer. Got: {type(algo_cfg_dict['seed'])}")
        if not isinstance(algo_cfg_dict['read_type'], str):
            logging.error(f"'read_type' must be a string. Got: {type(algo_cfg_dict['read_type'])}")
            raise TypeError(f"'read_type' must be a string. Got: {type(algo_cfg_dict['read_type'])}")
        if algo_cfg_dict['read_type'] not in ['all', 'filename']:
            logging.error(f"'read_type' must be either 'all' or 'filename'. Got: {algo_cfg_dict['read_type']}")
            raise ValueError(f"'read_type' must be either 'all' or 'filename'. Got: {algo_cfg_dict['read_type']}")
        if not isinstance(algo_cfg_dict['make_plots'], bool):
            logging.error(f"'make_plots' must be a boolean. Got: {type(algo_cfg_dict['make_plots'])}")
            raise TypeError(f"'make_plots' must be a boolean. Got: {type(algo_cfg_dict['make_plots'])}")
        if not isinstance(algo_cfg_dict['same_test_ids'], bool):
            logging.error(f"'same_test_ids' must be a boolean. Got: {type(algo_cfg_dict['same_test_ids'])}")
            raise TypeError(f"'same_test_ids' must be a boolean. Got: {type(algo_cfg_dict['same_test_ids'])}")
        if not isinstance(algo_cfg_dict['verbose'], bool):
            logging.error(f"'verbose' must be a boolean. Got: {type(algo_cfg_dict['verbose'])}")
            raise TypeError(f"'verbose' must be a boolean. Got: {type(algo_cfg_dict['verbose'])}")
        
        # Generate dictionary "algo_unc_dict" with uncertainty parameters
        algo_unc_dict = {'uncertainty_cfg': algo_cfg.get("uncertainty", {})}

        if not isinstance(algo_unc_dict["uncertainty_cfg"], dict):
            logging.error("The 'uncertainty' block must be a dictionary")
            raise TypeError("The 'uncertainty' block must be a dictionary")

        # Generate dictionary with combined training and uncertainty parameters
        self.algo_cfg_unc_dict = {
            'algo_cfg_dict': algo_cfg_dict,
            'algo_unc_dict': algo_unc_dict}

        # Get uncertainty configuration
        uncertainty_cfg = self.algo_cfg_unc_dict["algo_unc_dict"]["uncertainty_cfg"]

        # Error check for confidence levels data type, and use function default if not provided
        if not algo_unc_dict == {}:
            try:
                from fs_algo.fs_algo_train import AlgoTrainEval
                # Use the signature of AlgoTrainEval for default values
                sig = inspect.signature(AlgoTrainEval.__init__)
                confidence_levels = uncertainty_cfg.get("confidence_levels", sig.parameters['confidence_levels'].default)
            except:
                confidence_levels = [95]
        else:
            confidence_levels = [95]

        # Update the algo_cfg & class object just-in-case
        self.algo_cfg_unc_dict["algo_unc_dict"]["uncertainty_cfg"]['confidence_levels'] = confidence_levels
        if not isinstance(confidence_levels, list):
            logging.error(f"'confidence_levels' must be a list of numeric values (e.g., [90, 95]). Got: {type(confidence_levels).__name__}")
            raise TypeError(f"'confidence_levels' must be a list of numeric values (e.g., [90, 95]). Got: {type(confidence_levels).__name__}")
        for level in confidence_levels:
                if not isinstance(level, (int, float)) or not (0 < level <= 100):
                    logging.error(f"Each 'confidence_levels' entry must be a number between 0 and 100. Got: {level}")
                    raise ValueError(f"Each 'confidence_levels' entry must be a number between 0 and 100. Got: {level}")

        # Validate MAPIE parameters if provided
        mapie_cfg = uncertainty_cfg.get("mapie")
        if mapie_cfg:
            # Flatten the list of dicts into one dictionary
            mapie_params = {k: v for d in mapie_cfg for k, v in d.items()}

            # Check A. If 'alpha' is provided, check that it is a list of floats between 0 and 1
            alpha = mapie_params.get("alpha")
            if alpha is not None:
                if not isinstance(alpha, list) or not all(isinstance(a, float) and 0 < a < 1 for a in alpha):
                    logging.error(f"'alpha' in MAPIE must be a list of floats between 0 and 1. Got: {alpha}")
                    raise ValueError(f"'alpha' in MAPIE must be a list of floats between 0 and 1. Got: {alpha}")

                # Check B. If alpha is provided, 'method' must be 'plus' or 'minmax'
                method = mapie_params.get("method")
                if method not in ["plus", "minmax"]:
                    logging.error(f"If 'alpha' is provided, 'method' must be 'plus' or 'minmax'. Got: {method}")
                    raise ValueError(f"If 'alpha' is provided, 'method' must be 'plus' or 'minmax'. Got: {method}")

                # Check C. If alpha is provided, 'cv' must also be defined and an int
                cv = mapie_params.get("cv")
                if not isinstance(cv, int):
                    logging.error(f"If 'alpha' is provided, 'cv' must also be defined and must be an integer. Got: {cv}")
                    raise TypeError(f"If 'alpha' is provided, 'cv' must also be defined and must be an integer. Got: {cv}")

                # Check D. If alpha is provided, 'agg_function' must be 'mean' or 'median'
                agg_function = mapie_params.get("agg_function")
                if agg_function not in ["mean", "median"]:
                    logging.error(f"If 'alpha' is provided, 'agg_function' must be 'mean' or 'median'. Got: {agg_function}")
                    raise ValueError(f"If 'alpha' is provided, 'agg_function' must be 'mean' or 'median'. Got: {agg_function}")

# %% BASIN ATTRIBUTES (PREDICTORS) & RESPONSE VARIABLES (e.g. METRICS)
class AttrConfigAndVars:
    def __init__(self, path_attr_config: str | os.PathLike):
        self.path_attr_config = path_attr_config
        self.attrs_cfg_dict = dict()
        self.attr_config = dict()

    def _read_attr_config(self ) -> dict:
        """Extract the desired basin attribute variable names from yaml file

        :raises warnings.warn: Assumes all attributes desired if not specified
        :return: dictionary of required configuration items: 
            - `attrs_sel`: attributes `list[str]`
            - `dir_db_attrs`: directory where attribute .parquet files live `list[str]`
            - `dir_std_base`: directory of standardized data generated by :mod:`fs_prep`, `list[str]`
            - `dir_base`: base directory for file output, `list[str]`
            - `datasets`: dataset names, `list[str]`
        :rtype: dict
        """

        if not Path(self.path_attr_config).exists():
            logging.error("Attribute config path does not exist")
            raise ValueError(f"Attribute config path does not exist")

        # Attribute data location:
        with open(self.path_attr_config, 'r') as file:
            self.attr_config = yaml.safe_load(file)

        # identify attribute data of interest from attr_config
        attrs_all = [v for x in self.attr_config['attr_select'] for k,v in x.items() if '_vars' in k]
        attrs_all_filtered = [attr for attr in attrs_all if attr is not None]
        attrs_sel = [x for x in list(itertools.chain(*attrs_all_filtered)) if x is not None]

        if len(attrs_sel) == 0: # If no attributes generated, assume all attributes are of interest
            attrs_sel = 'all'
            logging.warning("No attributes discerned from 'attr_select'. Assuming all attributes desired.")
        
        home_dir = _define_home_dir(self.attr_config)

        dir_base = Path(list([x for x in self.attr_config['file_io'] if 'dir_base' in x][0].values())[0].format(home_dir=home_dir))
        # Location of attributes (predictor data):
        dir_db_attrs = Path(list([x for x in self.attr_config['file_io'] if 'dir_db_attrs' in x][0].values())[0].format(dir_base=dir_base, home_dir=home_dir))
        # parent location of response variable data:
        dir_std_base = Path(list([x for x in self.attr_config['file_io'] if 'dir_std_base' in x][0].values())[0].format(dir_base=dir_base, home_dir=home_dir))

        # The datasets of interest
        datasets = list([x for x in self.attr_config['formulation_metadata'] if 'datasets' in x][0].values())[0]

        # TODO The multidatasets_identifier remains un-tested until this note goes away!
        # multidatasets_identifier used in case multiple datasets exist inside each 'datasets' directory.
        mltidatasets_id = [x for x in self.attr_config['formulation_metadata'] if 'multidatasets_identifier' in x]
        if mltidatasets_id: 
            # Extract the match string used to identify each of the .nc datasets created by fs_prep.proc_eval_metrics.proc_col_schema()
            mltidatasets_str = mltidatasets_id[0]['multidatasets_id']
            for ds in datasets:
                all_dataset_paths = _std_fs_prep_ds_paths(dir_std_base,ds=ds,
                                                          mtch_str = '*' + mltidatasets_str)
                # Redefine datasets
                datasets = [Path(x).name() for x in all_dataset_paths]


        # Compile output
        self.attrs_cfg_dict = {'attrs_sel' : attrs_sel,
                            'dir_db_attrs': dir_db_attrs,
                            'dir_std_base': dir_std_base,
                            'dir_base': dir_base,
                            'home_dir': home_dir,
                            'datasets': datasets}

# %% PREDICTION CONFIGURATION
class PredConfigParser:
    def __init__(self, path_pred_config: str):
        self.path_pred_config = path_pred_config
        self.pred_cfg_dict = None

    def _read_pred_config(self) -> dict:
        """
        Read and parse the prediction configuration YAML file.

        Sets:
            self.pred_cfg_dict (dict): Parsed and formatted config items, including:
                - Required: `name_attr_config`, `name_algo_config`, `name_tfrm_config`,
                            `path_meta`, `write_type`, `ds_type`, `pred_file_comid_colname`,
                            `path_tfrm_script`, `conda_env`
                - Parsed from attribute config: `datasets`, `dir_base`, `dir_std_base`, `home_dir`
                - Optional: `algo_response_vars`, `algo_type`, `MAPIE_alpha`
        Returns:
            dict: Same dictionary stored in self.pred_cfg_dict
        Raises:
            ValueError: If any REQUIRED config fields are missing.
        """

        if not Path(self.path_pred_config).exists():
            logging.error(f"Prediction config file not found: {self.path_pred_config}")
            raise FileNotFoundError(f"Prediction config file not found: {self.path_pred_config}")

        # Load prediction config YAML
        with open(self.path_pred_config, 'r') as file:
            pred_cfg = yaml.safe_load(file)

        # --- Required top-level prediction keys ---
        required_pred_keys = [
            "name_attr_config", "name_algo_config",
            "ds_type", "write_type", "path_meta", "pred_file_comid_colname"
        ]

        missing_keys = [k for k in required_pred_keys if k not in pred_cfg or pred_cfg[k] is None]
        if missing_keys:
            logging.error(f"Missing required keys in prediction config file: {missing_keys}\nConfig path: {self.path_pred_config}\n")
            raise ValueError(
                f"Missing required keys in prediction config file: {missing_keys}\n"
                f"Config path: {self.path_pred_config}\n"
            )

        # Extract required fields
        name_attr_config     = pred_cfg["name_attr_config"]
        name_algo_config     = pred_cfg["name_algo_config"]
        write_type           = pred_cfg["write_type"]
        ds_type              = pred_cfg["ds_type"]
        path_meta            = pred_cfg["path_meta"]
        pred_file_comid_colname = pred_cfg["pred_file_comid_colname"]

        # Resolve full path to attribute config
        path_attr_config = build_cfig_path(self.path_pred_config, name_attr_config)
        # TODO Integrate AttrConfigParser here once available

        # --- Load attribute config using AttrConfigAndVars ---
        attr_cfg = AttrConfigAndVars(path_attr_config)
        attr_cfg._read_attr_config()

        home_dir     = attr_cfg.attrs_cfg_dict.get('home_dir') 
        dir_base     = attr_cfg.attrs_cfg_dict.get('dir_base')
        dir_std_base = attr_cfg.attrs_cfg_dict.get('dir_std_base')
        datasets     = attr_cfg.attrs_cfg_dict.get('datasets')

        # Check if dir_base exists
        if not Path(dir_base).exists():
            logging.error(f"Resolved dir_base path does not exist: {dir_base}")
            raise FileNotFoundError(f"Resolved dir_base path does not exist: {dir_base}")

        # Check if dir_std_base exists
        if not Path(dir_std_base).exists():
            logging.error(f"Resolved dir_base path does not exist: {dir_std_base}")
            raise FileNotFoundError(f"Resolved dir_base path does not exist: {dir_std_base}")

        # Optional prediction config values
        name_tfrm_config     = pred_cfg.get("name_tfrm_config", None)
        path_tfrm_script     = pred_cfg.get("path_tfrm_script", None)
        conda_env            = pred_cfg.get("conda_env", None)
        algo_response_vars = pred_cfg.get("algo_response_vars", [])
        algo_type = pred_cfg.get("algo_type", [])
        mapie_alpha = pred_cfg.get("MAPIE_alpha", None)
        uncn_bnd_pred = pred_cfg.get("uncn_bnd_pred", False)

        # Compile dictionary
        self.pred_cfg_dict = {
            'algo_response_vars': algo_response_vars,
            'algo_type': algo_type,
            'datasets': datasets,
            'path_meta': path_meta,
            'write_type': write_type,
            'ds_type': ds_type,
            'dir_std_base': Path(dir_std_base),
            'dir_base': Path(dir_base),
            'home_dir': home_dir,
            'name_attr_config': name_attr_config,
            'name_algo_config': name_algo_config,
            'name_tfrm_config': name_tfrm_config,
            'path_tfrm_script': path_tfrm_script,
            'conda_env': conda_env,
            'pred_file_comid_colname': pred_file_comid_colname,
            'mapie_alpha': mapie_alpha,
            'uncn_bnd_pred': uncn_bnd_pred,
            'path_pred_config': self.path_pred_config,
        }   

def _make_home_dir(home_dir_read:str|os.PathLike=[])-> os.PathLike:
    """Make the home directory based on what is passed or use default

    :param home_dir_read: A desired home directory, defaults to None which means the system default will be used
    :type home_dir_read: str | os.PathLike, optional
    :return: home directory path
    :rtype: os.PathLike
    """
    home_dir = str(Path.home()) # Initialize home_dir to a default value (system home)
    if len(home_dir_read) == 0:
        # home_dir is already set to Path.home()
        pass
    elif home_dir_read[0] is None:
        # home_dir is already set to Path.home()
        pass
    elif "~" in str(home_dir_read):
        if isinstance(home_dir_read, list) and home_dir_read and isinstance(home_dir_read[0], (str, Path)):
             # Assuming we mean to expand the passed value if it contains '~'
             home_dir = home_dir_read[0]
        home_dir = Path(home_dir).expanduser()
    elif not Path(home_dir_read[0]).exists():
        logging.warning(f"The user-defined home directory path {home_dir_read[0]} " \
            f"inside attribute config file does not exist. Using system default {Path.home()}")
        # home_dir remains set to the system default
        pass
    else:
        home_dir = home_dir_read[0]
    home_dir = Path(home_dir).expanduser()
    return home_dir

def _define_home_dir(attr_config:dict) -> os.PathLike:
    """Define the home directory of this system after parsing the attr config

    :param attr_config: The attribute config file object generated using fs_algo.utils.AttrConfigAndVars
    :type attr_config: dict
    :return: The filepath to the home directory
    :rtype: os.PathLike
    """
    # Determine if home_dir. Either defined in attribute config file or assumed to be system default.
    home_dir_read = [v for x in attr_config['file_io'] for k, v in x.items() if 'home_dir' in k ]
    home_dir = _make_home_dir(home_dir_read)
    return home_dir        

def _check_attr_rm_dupes(attr_df:pd.DataFrame, 
                   uniq_cols:list = ['featureID','featureSource','data_source','attribute','value'],
                   sort_col:str = 'dl_timestamp',
                   ascending=True)-> pd.DataFrame:
    """Check if duplicate attributes exist in the dataset. If so, remove them.

    :param attr_df: The standard dataframe of attributes, location identifierws and their values
    :type attr_df: pd.DataFrame
    :param uniq_cols: The columns in attr_df to be tested for duplication, defaults to ['featureID','featureSource','data_source','attribute','value']
    :type uniq_cols: list, optional
    :param sort_col: The column name of the timestamps. Default 'dl_timestamp'
    :type sort_col: str, optional
    :param ascending: The argument to pass into sort_values on the `sort_col`. If ascending = False, the most recent timestamp will be kept, and the oldest with True. Default True.
    :type ascending: bool, optional
    :return: The dataframe with removed attributes
    :rtype: pd.DataFrame

    note:: When ascending = False, the most recent timestamp will be kept, and the oldest with True.
    """

    if attr_df[['featureID','attribute']].duplicated().any():
        logging.info("Duplicate attribute data exist. Attempting to remove using fs_algo.utils._check_attr_rm_dupes().")
        attr_df = attr_df.sort_values(sort_col, ascending = ascending)
        attr_df = attr_df.drop_duplicates(subset=uniq_cols, keep='first')
    return attr_df

def fs_read_attr_comid(dir_db_attrs:str | os.PathLike, comids_resp:list | Iterable, attrs_sel: str | Iterable = 'all',
                       _s3 = None,storage_options=None,read_type:str=['all','filename'][0],
                       reindex:bool=False)-> pd.DataFrame:
    """Read attribute data acquired using proc.attr.hydfab R package & subset to desired attributes

    :param dir_db_attrs: directory where attribute .parquet files live
    :type dir_db_attrs: str | os.PathLike
    :param comids_resp: Unique location ID (e.g.USGS COMID, hf_uid) values of interest 
    :type comids_resp: list | Iterable
    :param attrs_sel: desired attributes to select from the attributes .parquet files, defaults to 'all'
    :type attrs_sel: str | Iterable, optional
    :param _s3: future feature, defaults to None
    :type _s3: future feature, optional
    :param storage_options: future feature, defaults to None
    :type storage_options: future feature, optional
    :param read_type: should all parquet files be lazy-loaded, assign 'all'
     otherwise just files with comids_resp in the file name? assign 'filename'. Defaults to 'all',
     which should be fastest when querying multiple locations. For single locations, the 'filename'
     approach is fastest.
    :type read_type: str
    :param reindex: Should attribute dataframe be reindexed? Default False
    :type reindex: bool
    :return: dict of the following keys:
        - `attrs_sel`
        - `dir_db_attrs`
        - `dir_std_base`
        - `dir_base`
        - `datasets`
    :rtype: pd.DataFrame
    """
    # Changelog/contributions
    #  2025-04-01 Add logic to remove empty parquet files
    #  2025-05-19 refactor: remove _NA_ parquet files, udpate 'all' to dd.read_parquet, GL
    #  2025-06-10 fix: rm accidental elif in entry point for read_type; add partitioning schema, GL
    #  2025-06-13 feat: add timestamp datetime coercion, GL
    #. 2025-09-30 fix: 'filename' option uses only dd instead of pd inside dd
    if _s3:
        storage_options={"anon",True} # for public
        # TODO  Setup the s3fs filesystem that will be used, with xarray to open the parquet files
        #_s3 = s3fs.S3FileSystem(anon=True)

    # Parquet files sized 0 bytes cause fatal errors when trying to read. Remove them.
    files_empty = [file for file in Path(dir_db_attrs).rglob("*.parquet") if file.stat().st_size == 0]
    # The location identifer of _NA_ should not exist. Remove it.
    file_NA = [file for file in Path(dir_db_attrs).rglob("*.parquet") if '_NA_' in str(file)]
    files_rm = files_empty + file_NA
    if len(files_rm) > 0:
        for file in files_rm:
            os.remove(file)

    # Define a pyarrow schema of the attribute data
    partitioning = ds.partitioning(
        pa.schema([pa.field("featureID", pa.string()), pa.field('featureSource', pa.string()),
                pa.field("data_source", pa.string()), pa.field('dl_timestamp', pa.string()),
                pa.field("attribute", pa.string()), pa.field('value', pa.float64())]),
                flavor="hive")

    # ------------------- Subset based on comids of interest ------------------
    if read_type == 'all': # Considering all parquet files inside directory
        # Read attribute data acquired using proc.attr.hydfab R package
        comids_resp_str = [str(s) for s in comids_resp]
        all_attr_ddf = dd.read_parquet(dir_db_attrs, storage_options = storage_options,
                                        engine='pyarrow',partitioning=partitioning)
        attr_ddf_subloc = all_attr_ddf[all_attr_ddf['featureID'].isin(comids_resp_str)]
    elif read_type == 'filename': # Read based on comid being located in the parquet filename
        substrings = [f'_{sub}_' for sub in comids_resp]
        pattern = re.compile('|'.join(map(re.escape,substrings)))
        all_files = [file for file in Path(dir_db_attrs).iterdir() if file.is_file()]
        matching_files = [file for file in all_files if pattern.search(str(file))]
        # Read in all matching filenames and proceed
        attr_ddf_subloc = dd.read_parquet(matching_files,
                                          engine='pyarrow',
                                          partitioning=partitioning)
    else:
        # Initialize attr_ddf_sub
        attr_ddf_sub = None
        logging.error(f"Unrecognized read_type provided in fs_read_attr_comid: {read_type}")
        raise ValueError(f"Unrecognized read_type provided in fs_read_attr_comid: {read_type}")
    
    if attr_ddf_subloc.shape[0].compute() == 0:
        logging.warning(f'None of the provided featureIDs exist in {dir_db_attrs}: \
                      \n {", ".join(attrs_sel)} ')
    
    # ------------------- Subset based on attributes of interest ------------------
    if attrs_sel == 'all':
        attrs_sel = attr_ddf_subloc['attribute'].unique().compute()

    attr_ddf_sub = attr_ddf_subloc[attr_ddf_subloc['attribute'].isin(attrs_sel)]
    
    attr_df_sub = attr_ddf_sub.compute() # This takes a while querying many files.

    if attr_df_sub.shape[0] == 0:
        logging.warning(f'The provided attributes do not exist with the retrieved featureIDs : \
                        \n {",".join(attrs_sel)}')
    # ------------------- Remove any duplicates & run checks -------------------
    attr_df_sub = _check_attr_rm_dupes(attr_df=attr_df_sub)

    # Run check that all variables are present across all basins
    dict_rslt = _check_attributes_exist(attr_df_sub,attrs_sel)
    attr_df_sub, attrs_sel_ser = dict_rslt['df_attr'], dict_rslt['attrs_sel']

    if not pd.api.types.is_float_dtype(attr_df_sub['value']):
        logging.warning("Forcing all attribute values to be float")
        attr_df_sub['value'] = np.float64(attr_df_sub['value'])

    if attr_df_sub['value'].isna().any():
        logging.warning('The attribute dataset contains unexpected NA values, \
                      which may be problematic for some algo training/testing. \
                      \nConsider reprocessing the attribute grabber (proc.attr.hydfab R package)')
    
    if reindex:
        attr_df_sub = attr_df_sub.reindex()

    # Coerce the timestamp column to datetime
    if 'dl_timestamp' in attr_df_sub.columns:
        attr_df_sub['dl_timestamp'] = pd.to_datetime(attr_df_sub['dl_timestamp'], errors='coerce')

    return attr_df_sub

def _check_attributes_exist(df_attr: pd.DataFrame, attrs_sel:pd.Series | Iterable) -> Dict[str, pd.DataFrame | pd.Series]:
    """ Checks if any COMIDs have different numbers of attributes. It's expected that they all have the same attributes.

    :param df_attr: The attribute data, as generated in :func:`fs_read_attr_comid()`
    :type df_attr: pd.DataFrame
    :param attrs_sel: the names of the attributes
    :type attrs_sel: pd.Series | Iterable
    :return: the same objects df_attr, and attrs_sel, but attrs_sel is ensured to be a pd.Series
    :rtype: Dict[pd.DataFrame, pd.Series]
    :seealso: :func:`fs_read_attr_comid()`

    """
    if not isinstance(attrs_sel,pd.Series):
        # Convert to a series for convenience of pd.Series.isin()
        attrs_sel = pd.Series(pd.Series(attrs_sel).unique())
    else:
        attrs_sel = pd.Series(attrs_sel.unique())

    # Run check that all attributes are present for all basins
    if df_attr.groupby('featureID')['attribute'].count().nunique() != 1:
        # multiple combos of comid/attrs exist. Find them and warn about it.
        counts = df_attr.groupby('featureID')['attribute'].count()
        vec_missing = counts != len(attrs_sel)

        if isinstance(vec_missing, pd.Series):
            bad_comids = counts.index[vec_missing].tolist()
        else: # Fallback in case vec_missing is a single boolean
            bad_comids = counts.index.tolist() if vec_missing else []
        # vec_missing = df_attr.groupby('featureID')['attribute'].count() != len(attrs_sel)
        # bad_comids = vec_missing.index.values[vec_missing]
        msg_tot_loc = f"    TOTAL unique locations with missing attributes: {len(bad_comids)} of {df_attr['featureID'].nunique()} total unique locations"
        logging.warning(msg_tot_loc)
        df_attr_sub_missing = df_attr[df_attr['featureID'].isin(bad_comids)]
    
        if isinstance(attrs_sel,list):
            missing_attrs = [attr for attr in attrs_sel if attr not in set(df_attr_sub_missing['attribute'])]
            missing_attrs = pd.DataFrame({'attribute':missing_attrs})
        else:
            missing_attrs = attrs_sel[~attrs_sel.isin(df_attr_sub_missing['attribute'])]
        msg_tot_miss = f"    TOTAL MISSING ATTRS: {len(missing_attrs)} of {len(attrs_sel)}"
        logging.warning(msg_tot_miss)
        str_missing = '\n    '.join(missing_attrs.values)

        warn_msg_missing_attrs = "\
        \n Not all featureID groupings (i.e. COMID groups) contain the same number of catchment attributes. \
        \n This could be problematic for model training and/or prediction. \
        \n Consider running attribute grabber with proc.attr.hydfab."
        warn_msg2 = "\nMissing attributes include: \n    " + str_missing
        if df_attr['featureID'].nunique() > 0:
            warn_msg_3 = "\n COMIDs with missing attributes include: \n" + ', '.join(bad_comids)
        else: # An empty dataframe was passed to this function, so we can't say what location IDs are missing
            warn_msg_3 = ''
        logging.warning(warn_msg_missing_attrs + warn_msg2 + warn_msg_3)
    return {'df_attr': df_attr, 'attrs_sel': attrs_sel}


def _id_attrs_sel_wrap(attr_cfig: AttrConfigAndVars,
                    path_cfig: str | os.PathLike = None,
                    name_attr_csv: str = None,
                    colname_attr_csv: str = None) -> list:
    """Get attributes of interest from a csv file with column name, or the attribute config object

    :param attr_cfig: The attribute config file object generated using fs_algo.utils.AttrConfigAndVars
    :type attr_cfig: AttrConfigAndVars
    :param path_cfig: Optional path to a file, that also lives in the same directory as the `name_attr_csv`, defaults to None
    :type path_cfig: str | os.PathLike
    :param name_attr_csv: The name of the csv file containing the attribute listing of interest, defaults to None
    :type name_attr_csv: str, optional
    :param colname_attr_csv: The column name inside the csv file containing the attributes of interest, defaults to None
    :type colname_attr_csv: str, optional
    :return: list of all attributes of interest, likely to use for training/prediction
    :rtype: list

    """
    # Changelog / contributions
    #  2024-04-15 Force into ensure uniqueness attributes, GL
    if name_attr_csv:
        path_attr_csv = build_cfig_path(path_cfig,name_attr_csv)
        attrs_sel = pd.read_csv(path_attr_csv)[colname_attr_csv].tolist()
    else:
        attrs_sel = attr_cfig.attrs_cfg_dict.get('attrs_sel', None)
    attrs_sel = list(set(attrs_sel))
    return attrs_sel

def _find_feat_srce_id(dat_resp: Optional[xr.core.dataset.Dataset] = None,
                       col_schema: Optional[list[Dict]]=None,
                       attr_config: Optional[Dict] = None) -> List[str]:
    """ Try grabbing :mod:`fs_prep` standardized dataset attributes &/or config file.

    :param dat_resp: The standardized dataset generated by :mod:`fs_prep`, defaults to None
    :type dat_resp: Optional[xr.core.dataset.Dataset], optional
    :param col_schema: Column schema mapping out the featureID f-string format
      (e.g. "USGS-{gage_id}") and featureSource. One f-string formatting completed,
       serves as argument used in nhdplusTools::get_nldi_features() in proc.attr.hydfab 
       R package, for example. Extracted from the attribute config file. defaults to None
    :type col_schema: Optional[list[Dict]], optional
    :param attr_config: The attribute config file object generated using
      fs_algo.utils.AttrConfigAndVars, defaults to None
    :type attr_config: Optional[Dict], optional
    :raises ValueError: featureSource could not be identified from the provided
    :raises ValueError: _description_
    :return: The featureSource and f-string formatted featureID, e.g. ['nwissite','USGS-{gage_id}']
    :rtype: List[str]

    note:: Standardized dataset attributes preferred in cases of processing multiple
      datasets & attributes differ by dataset). Otherwise, fallback on config file.
        At least one argument must be provided.

    # 2025-06-13 refactor: simplify to allow passing just the col_schema section of the attribute config file, GL
    """

    featureSource = None
    try: # dataset attributes first
        featureSource = dat_resp.attrs.get('featureSource', None)
    except (KeyError, StopIteration,AttributeError): # config file second
        pass
    if attr_config is not None:
        col_schema = attr_config.get('col_schema', None)

    if featureSource is None:
        try: 
            featureSource = next(x['featureSource'] for x in col_schema if 'featureSource' in x)
        except:
            pass
    
    if not featureSource:
        logging.error('The featureSource could not be found. Ensure it is present in the col_schema section of the attribute config file.')
        raise ValueError(f'The featureSource could not be found. Ensure it is present in the col_schema section of the attribute config file.')
    # Attempt to grab featureID from dataset attributes, fallback to the config file
    featureID = None
    try: # dataset attributes first
        featureID = dat_resp.attrs.get('featureID', None)
    except (KeyError, StopIteration,AttributeError): # config file second
        pass
    if featureID is None:
        try:
            featureID = next(x['featureID'] for x in col_schema if 'featureID' in x)
        except:
            pass
    if not featureID:
        logging.error('The featureID could not be found. Ensure it is present in the col_schema section of the attribute config file.')
        raise ValueError(f'The featureID could not be found. Ensure it is present in the col_schema section of the attribute config file.')
        # TODO need to map gage_id to location identifier in attribute data!

    return [featureSource, featureID]

def fs_retr_nhdp_comids_geom(featureSource:str,featureID:str,gage_ids: Iterable[str] 
                             ) -> gpd.geodataframe.GeoDataFrame:    
    """Retrieve response variable's comids & point geom, querying the shortest distance in the flowline

    :param featureSource: the datasource for featureID from the R function :mod:`nhdplusTools` :func:`get_nldi_features()`, e.g. 'nwissite'
    :type featureSource: str
    :param featureID: The conversion format of `gage_ids` into a recognizable string for :mod:`nhdplusTools`, which is an f-string configured conversion of `gage_id` e.g. `'USGS-{gage_id}'`. Expected to contain the string `"{gage_id}"`
    :type featureID: str
    :param gage_ids: The location identifiers compatible with the format specified in `featureID`
    :type gage_ids: Iterable[str]
    :raises warnings.warn: In case number of retrieved comids does not match total requested gage ids
    :return: The COMIDs, gage_id, & point geometry corresponding to the provided location identifiers, `gage_ids`
    :rtype: GeoDataFrame

    Changelog:
        2024-12-01 refactor: return GeoDataFrame with coordinates instead of a list of just comids, GL
        2025-03-07 add gage_id to return geodataframe
    """

    nldi = nhd.NLDI()
    
    gageids_miss = []
    comids_resp = []
    geom_pts = []
    feature_id = []
    for gage_id in gage_ids:
        feature_id.append(featureID.format(gage_id=gage_id))
        try:
            upstr_flowline = nldi.navigate_byid(
                fsource=featureSource,
                fid=featureID.format(gage_id=gage_id),
                navigation='upstreamMain',
                source='flowlines',
                distance=1
            ).loc[0]
            geom_pts.append(Point(upstr_flowline['geometry'].coords[0]))
            comid = upstr_flowline['nhdplus_comid']
            comids_resp.append(comid)
        except Exception as e:
            logging.info(f"Error processing gage_id {gage_id}: {e}")
            # Handle the error (e.g., log it, append None, or any other fallback mechanism)

            # TODO Attempt a different approach for retrieving comid:
            gageids_miss.append(gage_id)
            geom_pts.append(np.nan)
            comids_resp.append(np.nan)  # Appending NA for failed gage_id, or handle differently as needed

    # if len(comids_resp) != len(gage_ids) or comids_resp.count(None) > 0: # May not be an important check
    #     raise warnings.warn("The total number of retrieved comids does not match \
    #                   total number of provided gage_ids",UserWarning)

    gdf_comid = gpd.GeoDataFrame(pd.DataFrame({ 'comid': comids_resp,
                                               'gage_id': gage_ids}),
                                            geometry=geom_pts,crs=4326 
                                )

    return gdf_comid

def build_cfig_path(path_known_config:str | os.PathLike, path_or_name_cfig:str | os.PathLike) -> os.PathLike | None:
    """Build the expected configuration file path within the RAFTS framework

    :param path_known_config: path of the known configuration sent 
    :type path_known_config: str | os.PathLike
    :param path_or_name_cfig: Path or name of configuration file. If only name provided, it's assumed it resides in same directory as `path_known_config`
    :type path_or_name_cfig: str | os.PathLike
    :raises FileNotFoundError: The provided `path_known_config` does not exist
    :raises FileNotFoundError: The desired configuration file does not exist
    :return: The path to another relevant configuration file used for a different step in RAFTS processing
    :rtype: os.PathLike | None
    """
    dir_parent_cfig = Path(path_known_config).parent
    if not dir_parent_cfig.exists():
        logging.error(f"The provided 'known' configuration file does not exist: \n{path_known_config}")
        raise FileNotFoundError(f"The provided 'known' configuration file does not exist: \n{path_known_config}")
    if path_or_name_cfig: # Only perform if path_or_name_cfig not None
        path_cfig = Path(dir_parent_cfig/Path(path_or_name_cfig))
        if not path_cfig.exists():
            path_cfig = Path(path_or_name_cfig)
            if not path_cfig.exists():
                logging.error(f'The following configuration file could not be found: \n{path_or_name_cfig}')
                raise FileNotFoundError(f'The following configuration file could not be found: \n{path_or_name_cfig}')
    else:
        logging.warning("The configuration file may not have specified the path or file name.")
        path_cfig = None
    return path_cfig

def build_pred_locs_path(path_meta_template: str | os.PathLike,dir_std_base: str | os.PathLike,
    ds: str,ds_type: str,write_type: str) -> Path:
    """
    Build the full path to the prediction metadata location using formatting template.

    :param path_meta_template: f-string template for the path, e.g. "{dir_std_base}/{ds}/nldi_feat_{ds}_{ds_type}.{write_type}"
    :param dir_std_base: Base directory for standardized data
    :param ds: Dataset name
    :param ds_type: Dataset type, e.g., 'prediction'
    :param write_type: File extension/type, e.g., 'csv' or 'parquet'
    :return: Full formatted Path to prediction metadata file
    """
    path_str = path_meta_template.format(
        dir_std_base=dir_std_base,
        ds=ds,
        ds_type=ds_type,
        write_type=write_type
    )
    return Path(path_str)

def fs_save_algo_dir_struct(dir_base: str | os.PathLike ) -> dict:
    """Generate a standard file saving directory structure

    :param dir_base: The base directory for saving output
    :type dir_base: str | os.PathLike
    :raises ValueError: If the base directory does not exist
    :return: Full paths to the `output`, `trained_algorithms`,
     `analysis` and `data_visualization` directories
    :rtype: dict
    """

    if not Path(dir_base).exists():
        logging.error(f"The provided dir_base does not exist. \
                         \n Double check the config file to make sure \
                         \n an existing directory is provided. dir_base= \
                         \n{dir_base}")
        raise ValueError(f"The provided dir_base does not exist. \
                         \n Double check the config file to make sure \
                         \n an existing directory is provided. dir_base= \
                         \n{dir_base}")

    # Define the standardized directory structure for algorithm output
    # base save directory
    dir_out = Path(Path(dir_base).parent.absolute()/Path('output'))
    dir_out.mkdir(exist_ok=True)

    # The trained algorithm directory
    dir_out_alg_base = Path(dir_out/Path('trained_algorithms'))
    dir_out_alg_base.mkdir(exist_ok=True)

    # TODO consider compatibility with std_pred_path
    dir_preds_base = Path(dir_out/Path('algorithm_predictions'))
    dir_preds_base.mkdir(exist_ok=True)

    # The analysis directory
    dir_out_anlys_base = Path(dir_out/Path("analysis"))
    dir_out_anlys_base.mkdir(exist_ok=True)

    # The data visualization directory
    dir_out_viz_base = Path(dir_out/Path("data_visualizations"))
    # TODO insert dir that Lauren creates here

    out_dirs = {'dir_out': dir_out,
                'dir_out_alg_base': dir_out_alg_base,
                'dir_out_preds_base' : dir_preds_base,
                'dir_out_anlys_base' : dir_out_anlys_base,
                'dir_out_viz_base' : dir_out_viz_base}

    return out_dirs

def _std_fs_prep_ds_companion_gpkg_path(path_fs_prep:str|os.PathLike)->os.PathLike:
    """Create the standardized gpkg path for coordinate data & id mapping
      corresponding to the standardized input data

    :param path_fs_prep: Path used for the standardized dataset created using fs_prep.proc_eval_metrics.proc_col_schema()
    :type path_fs_prep: str | os.PathLike
    :return: Path storing the coordinates and id-mapping of each location of interest
    :rtype: os.PathLike
    """
    path_fs_prep =Path(path_fs_prep)
    sub_name_loc = path_fs_prep.with_suffix('')
    new_name_loc = str(sub_name_loc.name) + '_loc'
    new_name_gpkg = str(Path(new_name_loc).with_suffix('.gpkg'))
    path_gpkg_fs_prep = path_fs_prep.with_name(new_name_gpkg)
    return path_gpkg_fs_prep

def _std_fs_prep_ds_paths(dir_std_base: str|os.PathLike,ds:str,mtch_str='*.nc') -> list:
    """The standard .nc paths for standardized dataset created using fs_prep.proc_eval_metrics.proc_col_schema()

    :param dir_std_base:  The directory containing the standardized dataset generated from `fs_prep`
    :type dir_std_base: str | os.PathLike
    :param ds:  a string that's unique to the dataset of interest
    :type ds: str
    :param mtch_str: the desired matching string describing datasets of interests, defaults to '*.nc'
    :type mtch_str: str, optional
    :return: list of each filepath to a dataset
    :rtype: list
    """
    ls_ds_paths = [x for x in Path(dir_std_base/Path(ds)).glob(mtch_str) if x.is_file()]
    return ls_ds_paths

def _open_response_data_fs(dir_std_base: str | os.PathLike, ds:str, mtch_str:str='*.nc') -> xr.Dataset:
    """Read in standardized dataset generated from :mod:`fs_prep`

    :param dir_std_base: The directory containing the standardized dataset generated from `fs_prep`
    :type dir_std_base: str | os.PathLike
    :param ds: a string that represents the dataset of interest
    There should be a netcdf .nc or zarr .zarr file containing matches to this string
    :type ds: str
    :raises ValueError: The directory where the dataset file should live does not exist.
    :raises ValueError: Could not successfully read in the dataset `ds` as a .nc or .zarr
    :return: The hierarchical dataset as an xarray, as generated by :mod:`fs_prep`
    :rtype: xr.Dataset
    """
    # Implement a check to ensure each dataset directory exists
    if not Path(dir_std_base).exists:
        logging.error(f'The dir_std_base directory does not exist. Double check dir_std_base: \
                         \n{dir_std_base}')
        raise ValueError(f'The dir_std_base directory does not exist. Double check dir_std_base: \
                         \n{dir_std_base}')
    
    path_nc = _std_fs_prep_ds_paths(dir_std_base=dir_std_base,ds=ds,mtch_str=mtch_str)
    #path_nc = [x for x in Path(dir_std_base/Path(ds)).glob("*.nc") if x.is_file()]
    if len(path_nc) > 1:
        error_str = f"The following directory contains too many .nc files: {path_nc}"
        logging.error(error_str)
        raise ValueError(error_str)

    try:
        dat_resp = xr.open_dataset(path_nc[0], engine='netcdf4')
    except:
        path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr")]
        try:
            dat_resp = xr.open_dataset(path_zarr[0],engine='zarr')
        except:
            logging.error(f"Could not identify an approach to read in dataset via {path_nc} nor {path_zarr}")
            raise ValueError(f"Could not identify an approach to read in dataset via {path_nc} nor {path_zarr}")
    return dat_resp

def std_algo_path(dir_out_alg_ds:str | os.PathLike, algo: str, metric: str, dataset_id: str) -> str:
    """Standardize the algorithm save path
    :param dir_out_alg_ds:  Directory where algorithm's output stored.
    :type dir_out_alg_ds: str | os.PathLike
    :param algo: The type of algorithm
    :type algo: str
    :param metric:  The metric or hydrologic signature identifier of interest
    :type metric: str
    :param dataset_id: Unique identifier/descriptor of the dataset of interest
    :type dataset_id: str
    :return: full save path for joblib object
    :rtype: str
    """
    Path(dir_out_alg_ds).mkdir(exist_ok=True,parents=True)
    basename_alg_ds_metr = f'algo_{algo}_{metric}__{dataset_id}'
    path_algo = Path(dir_out_alg_ds) / Path(basename_alg_ds_metr + '.joblib')
    return path_algo

def std_pred_path(dir_out: str | os.PathLike, algo: str, metric: str, dataset_id: str
                  ) -> Path:
    """Standardize the prediction results save path

    :param dir_out: The base directory for saving output
    :type dir_out: str | os.PathLike
    :param algo: The type of algorithm
    :type algo: str
    :param metric: The metric or hydrologic signature identifier of interest
    :type metric: str
    :param dataset_id: Unique identifier/descriptor of the dataset of interest
    :type dataset_id: str
    :return: full save path for parquet dataframe object of results
    :rtype: str
    """
    # TODO consider refactoring this to pass in dir_out_preds_base instead
    dir_preds_base = Path(Path(dir_out)/Path('algorithm_predictions'))
    dir_preds_ds = Path(dir_preds_base/Path(dataset_id))
    dir_preds_ds.mkdir(exist_ok=True,parents=True)
    basename_pred_alg_ds_metr = f"pred_{algo}_{metric}__{dataset_id}.parquet"
    path_pred_rslt = Path(dir_preds_ds)/Path(basename_pred_alg_ds_metr)
    return path_pred_rslt

def std_eval_metrs_path(dir_out_viz_base: str|os.PathLike,
                      ds:str, metr:str
                      ) -> Path:
    """Standardize the filepath for saving model evaluation metrics table

    :param dir_out_viz_base: The base output directory
    :type dir_out_viz_base: str | os.PathLike
    :param ds: The dataset name
    :type ds: str
    :param metric: The metric or hydrologic signature identifier of interest
    :type metric: str
    :return: The model metrics filepath
    :rtype: pathlib.PosixPath
    """
    path_eval_metr = Path(f"{dir_out_viz_base}/{ds}/algo_eval_{ds}_{metr}.csv")
    path_eval_metr.parent.mkdir(parents=True,exist_ok=True)
    return path_eval_metr


def std_test_pred_obs_path(dir_out_anlys_base:str|os.PathLike,ds:str, metr:str
                      )->Path:
    """Generate the standardized path for saving the predicted & observed metric/coordinates from testing

    :param dir_out_anlys_base: Base analysis directory
    :type dir_out_anlys_base: str | os.PathLike
    :param ds: dataset name
    :type ds: str
    :param metr: metric/response variable of interest
    :type metr: str
    :return: save path to the pred_obs_{ds}_{metr}.csv file
    :rtype: pathlib.PosixPath
    """
    # Create the path for saving the predicted and observed metric/coordinates from testing
    path_pred_obs = Path(f"{dir_out_anlys_base}/{ds}/pred_obs_{ds}_{metr}.csv")
    path_pred_obs.parent.mkdir(exist_ok=True,parents=True)
    return path_pred_obs

def _read_pred_comid(path_pred_locs: str | os.PathLike, comid_pred_col:str ) -> list[str]:
    """Read the comids from a prediction file formatted as .csv

    :param path_pred_locs: The path to prediction data location, containing the comid
    :type path_pred_locs: str | os.PathLike
    :param comid_pred_col: The column name corresponding to the comid inside the prediction location dataset
    :type comid_pred_col: str
    :raises ValueError: Could not read the location data file and/or subselect the comid column
    :raises ValueError: File extension of location data file not recognized
    :return: list of comids
    :rtype: list[str]
    """
    # Changelog/contributions
    # 2025-03-30 Add in drop_duplicates(), GL
    if not Path(path_pred_locs).exists():
        logging.error(f"The path to prediction location data could not be found: \n{path_pred_locs} ")
        raise FileNotFoundError(f"The path to prediction location data could not be found: \n{path_pred_locs} ")
    if '.csv' in Path(path_pred_locs).suffix:
        try:
            comids_pred = pd.read_csv(path_pred_locs)[comid_pred_col].drop_duplicates().values            
        except:
            logging.error(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
            raise ValueError(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
    elif '.parquet' in Path(path_pred_locs).suffix:
        try:
            comids_pred = pd.read_parquet(path_pred_locs)[comid_pred_col].drop_duplicates().values
        except:
            logging.error(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
            raise ValueError(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
    elif '.parquet' in Path(path_pred_locs).suffix:
        try:
            comids_pred = pd.read_parquet(path_pred_locs)[comid_pred_col].drop_duplicates().values
        except:
            logging.error(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
            raise ValueError(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
    elif '.parquet' in Path(path_pred_locs).suffix:
        try:
            comids_pred = pd.read_parquet(path_pred_locs)[comid_pred_col].drop_duplicates().values
        except:
            logging.error(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
            raise ValueError(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
    else:
        logging.error(f"NEED TO ADD CAPABILITY THAT HANDLES {Path(path_pred_locs).suffix} file extensions")
        raise ValueError(f"NEED TO ADD CAPABILITY THAT HANDLES {Path(path_pred_locs).suffix} file extensions")
    comids_pred = [str(x) for x in comids_pred]
    return comids_pred


def find_common_comid(dict_gdf_comids:Dict[str,gpd.GeoDataFrame], column='featureID')->list:
    """Given a collection of multiple datasets, find the shared comids

    :param dict_gdf_comids: a dictionary of multiple datasets,
      each containing a geodataframe of comids as generated by
      :func:`fs_retr_nhdp_comids_geom`
    :type dict_gdf_comids: dict[str, geopandas.GeoDataFrame]
    :param column: The geodataframe column name for the comid, defaults to 'featureID'
    :type column: str, optional
    :seealso: :func:`split_train_test_comid_wrap`
    :seealso: :func:`fs_retr_nhdp_comids_geom`
    :return: list of the shared comids
    :rtype: list
    """
    # Changelog/contributions
    # FY25 originally created, GL
    # 2025-05-19 change default column to 'featureID'

    common_comid = None
    for df in dict_gdf_comids.values():
        if common_comid is None:
            common_comid = set(df[column])
        else:
            common_comid &= set(df[column])

    common_comid = list(common_comid)
    return common_comid


def fs_retr_nhdp_comids_geom_wrap(path_save_gpkg:str|os.PathLike,
                                  gage_ids:Iterable,
                                featureSource:str='nwissite', featureID:str='USGS-{gage_id}'
                                ) ->gpd.GeoDataFrame:
    """Read or generate a geodataframe that queries NHDplus for comid and coordinate based
    on provided gage_id, or compatible featureSource/featureID pairing

    :param path_save_gpkg: filepath where data are saved. This limits the number of hits to the NHDplus database
    :type path_save_gpkg: str | os.PathLike
    :param gage_ids: The identifiers of interest, e.g. the USGS gage id numbers
    :type gage_ids: Iterable
    :param featureSource: the datasource for featureID from the R function 
        :mod:`nhdplusTools` :func:`get_nldi_features()`,defaults to 'nwissite'
    :type featureSource: str
    :param featureID: The conversion format of `gage_ids` into a recognizable string for
        :mod:`nhdplusTools`, which is an f-string configured conversion of `gage_id`
        defaults to 'USGS-{gage_id}'. Expected to contain the string `"{gage_id}"`
    :type featureID: str
    :return: Geodataframe with the columns 'comid', 'geometry', 'gage_id'
    :rtype: gpd.GeoDataFrame
    :seealso: :func:`combine_resp_gdf_comid_wrap` A wrapper function that calls this function
    :seealso: :func:`_std_fs_prep_ds_companion_gpkg_path` The standardized path to use for path_save_gpkg
    :seealso: :mod:`proc.attr.hydfab`:func:`fs_retr_nhdp_comids_geom_wrap` The corresponding R function
    """
    path_save_gpkg = Path(path_save_gpkg)
    if path_save_gpkg.exists(): # Maybe we can skip the database connection!
        # Read the intermediate file & check if any gage_ids need comids:
        gdf_comid_in = gpd.read_file(path_save_gpkg,layer='outlet')
        cmmn_ids = np.intersect1d(gdf_comid_in['gage_id'], gage_ids)
        if len(cmmn_ids) == len(gage_ids): # All gage_ids accounted for
            gdf_comid = gdf_comid_in.copy()
            # TODO could consider check to try and fill in missing comid data here...
            # TODO MUST ensure these searches/fill ins ultimately maintain same sequence as gage_ids
            # if gdf_comid_in['comid'].isna().sum() >0:
            #     need_gage_ids = gdf_comid_in['gage_id'][gdf_comid_in['comid'].isna()]
            #     gid_chck = fs_retr_nhdp_comids_geom(featureSource=featureSource,
            #                                         featureID=featureID,
            #                                         gage_ids=need_gage_ids.values)
        else:
            # Grab the comid and associated coords/geodataframe 
            gdf_comid = fs_retr_nhdp_comids_geom(featureSource=featureSource,
                                                        featureID=featureID,
                                                        gage_ids=gage_ids)
            # Write to file
            gdf_comid.to_file(path_save_gpkg, layer = 'outlet',driver='GPKG')
    else:
        # Grab the comid and associated coords/geodataframe 
        gdf_comid = fs_retr_nhdp_comids_geom(featureSource=featureSource,
                                                    featureID=featureID,
                                                    gage_ids=gage_ids)
        # Write to file
        gdf_comid.to_file(path_save_gpkg, layer = 'outlet',driver='GPKG')
    return(gdf_comid)

def _read_metadata(path_attr_config:str|os.PathLike, ds:str) -> pd.DataFrame:
    """Read the metadata file for the dataset of interest

    :param attr_config: The path to attribute config file 
    :type attr_config: str | os.PathLike
    :param ds: The unique dataset identifier
    :type ds: str
    :return: The metadata dataframe
    :rtype: pd.DataFrame

    Changelog:
    2025-05-19 originally created, GL
    """

    attr_cfig = AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    # Define directories/datasets from the attribute config file
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')

    # Grab variables for building out the path to metadata (which contains comid-gage id mappings)
    ds_type = [x for x in attr_cfig.attr_config.get('file_io') if 'ds_type' in x][0]['ds_type']
    write_type = [x for x in attr_cfig.attr_config.get('file_io') if 'write_type' in x][0]['write_type']
    path_meta_fstr = [x for x in attr_cfig.attr_config.get('file_io') if 'path_meta' in x][0]['path_meta']

    vals = {'ds_type':ds_type,'write_type':write_type, 'dir_std_base':dir_std_base,'ds':ds}
    path_meta = path_meta_fstr.format(**vals)
    if not Path(path_meta).exists():
        logging.error(f"The dataset's metadata mapping file could not be found: \n{path_meta}")
        raise FileNotFoundError(f"The dataset's metadata mapping file could not be found: \n{path_meta}")
    if 'parquet' in Path(path_meta).suffix:
        df_meta = pd.read_parquet(path_meta)
    elif 'csv' in Path(path_meta).suffix:
        df_meta = pd.read_csv(path_meta)

    return df_meta



def combine_resp_gdf_comid_wrap(dir_std_base:str|os.PathLike,ds:str,path_attr_config:str|os.PathLike,
                          )->dict:
    """Standardize the response variable and geodataframe/comid retrieval for a single dataset in a wrapper function

    Removes data points from consideration if no comid could be found. Makes the gdf and response data consistent.

    :param dir_std_base: The directory containing the standardized dataset generated from `fs_prep`
    :type dir_std_base: str | os.PathLike
    :param ds:  The unique dataset identifier
    :type ds: str
    :param path_attr_config: path to the attribute configuration file
    :type attr_config: str | os.PathLike
    :return: dict of the response xarray dataset `'dat_resp'`,
      and the geodataframe with comids & coordinates `'gdf_comid'`
    :rtype: dict

    Changelog:
        2025-05-19 refactor: integrate path_meta for gage_id:featureID-featureSource mapping, GL   
    """

    dat_resp = _open_response_data_fs(dir_std_base,ds)

    # ----- Retrieve a dataset's metadata that maps the gage_id to the featureID
    df_meta = _read_metadata(path_attr_config=path_attr_config, ds=ds)

    # %% COMID & coord retrieval and assignment to response variable's coordinate
    # TODO quickfix: read attribute config file here:
    attr_cfig = AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()
        
    [featureSource,featureID] = _find_feat_srce_id(dat_resp,
                                   col_schema=attr_cfig.attr_config['col_schema']) # e.g. ['nwissite','USGS-{gage_id}']
    
    path_fs_dat_resp =  _std_fs_prep_ds_paths(dir_std_base=dir_std_base,ds=ds,mtch_str='*.nc')
    if len(path_fs_dat_resp) > 1:
        error_str = f"The following directory contains too many .nc files: {path_fs_dat_resp}"
        logging.error(error_str)
        raise ValueError(error_str)
    path_gpkg_fs_prep = _std_fs_prep_ds_companion_gpkg_path(path_fs_dat_resp[0])

    # Retrieve the geodataframe of comids
    gdf_comid = fs_retr_nhdp_comids_geom_wrap(path_save_gpkg=path_gpkg_fs_prep,
                                  gage_ids=dat_resp['gage_id'].values,
                                featureSource=featureSource, featureID=featureID)
   
    # --- map gdf_comid and df_meta for featureID
    # remove featureID/featureSource columns in gdf_comid that pertain to the gage_id, 
    # not the unique identifiers of those same cols in the df_meta
    if 'featureID' in gdf_comid.columns:
        gdf_comid = gdf_comid.drop(columns=['featureID'])
    if 'featureSource' in gdf_comid.columns:
        gdf_comid = gdf_comid.drop(columns=['featureSource'])
    # merge gdf_comid with df_meta based on gage_id column
    df_meta_map = df_meta[['featureID','featureSource','gage_id']].drop_duplicates()

    gdf_comid = gdf_comid.merge(df_meta_map, on='gage_id', how='left')



    # --- response data identifier alignment with comids & na removal --- #
    # Subset gdf to the gage_ids that are present in the standardized response variable
    sub_gdf_comid = gdf_comid[gdf_comid['gage_id'].isin(dat_resp['gage_id'].values)]
    if sub_gdf_comid.shape[0] != len(dat_resp['gage_id']):
        logging.warning(f"Warning: The number of gage_ids in the response variable ({len(dat_resp['gage_id'])}) does not match the number of gage_ids in the geodataframe ({sub_gdf_comid.shape[0]}).")
        # TODO consider dropping the gage_ids that are not present in the geodataframe
        if sub_gdf_comid.shape[0] < len(dat_resp['gage_id']):
            logging.error(f"The number of gage_ids in the response variable ({len(dat_resp['gage_id'])}) is less than the number of gage_ids in the geodataframe ({sub_gdf_comid.shape[0]}).")
            raise ValueError(f"The number of gage_ids in the response variable ({len(dat_resp['gage_id'])}) is less than the number of gage_ids in the geodataframe ({sub_gdf_comid.shape[0]}).")
            # TODO consider dropping the gage_ids that are not present in the geodataframe
    
    if('featureID' in sub_gdf_comid.columns):
        feature_id_col = 'featureID'
    elif('comid' in sub_gdf_comid.columns):
        feature_id_col = 'comid'
    else:
        logging.error("The geodataframe does not contain a column named 'featureID' or 'comid'.")
        raise ValueError(f"The geodataframe does not contain a column named 'featureID' or 'comid'.")

    gage_to_feat_source_map = sub_gdf_comid.set_index('gage_id')['featureSource']
    mapped_feat_source = pd.Series(dat_resp['gage_id'].values).map(gage_to_feat_source_map)
    gage_to_comid_map = sub_gdf_comid.set_index('gage_id')[feature_id_col]
    mapped_comids = pd.Series(dat_resp['gage_id'].values).map(gage_to_comid_map)
    dat_resp = dat_resp.assign_coords(featureID=("gage_id", mapped_comids.values),
                                      featureSource=("gage_id",mapped_feat_source))

    idxs_na_comid = list(np.where(sub_gdf_comid['featureID'].isna())[0])
    gage_id_mask = ~np.isin(np.arange(len(dat_resp['gage_id'])),idxs_na_comid)
    if len(idxs_na_comid) > 0:
        gage_ids_missing = dat_resp['gage_id'].isel(gage_id=~gage_id_mask).values
        logging.info(f"A total of {len(idxs_na_comid)} returned comids are NA values. \
               \nRemoving the following gage_ids from dataset: \
              \n{gage_ids_missing}")
        # Remove the unknown comids now that they've been matched up to the original dims in dat_resp:
        dat_resp = dat_resp.isel(gage_id=gage_id_mask)# remove NA vals from gage_id coord

    
    sub_gdf_comid = sub_gdf_comid.drop_duplicates().dropna(subset=['featureID'],axis=0)
    if any(sub_gdf_comid[feature_id_col].duplicated()):
        logging.info("Note that some duplicated comids found in dataset based on initial location identifier, gage_id")
    sub_gdf_comid['dataset'] = ds 

    dict_resp_gdf = dict({'dat_resp':dat_resp,
                        'gdf_comid': sub_gdf_comid})
    return(dict_resp_gdf)

def split_train_test_comid_wrap(dir_std_base:str|os.PathLike, 
                datasets:list, path_attr_config:str | os.PathLike,
                id_col='featureID', test_size:float=0.3,
                random_state:int=42) -> dict:
    """Create a train/test split based on shared comids across multiple datasets
    Helpful when multiple datasets desired for intercomparison share the same comids, but 
    some datasets don't have the same size (e.g. dataset A has 489 locations whereas dataset B has 512 locations)
    If datasets all share the same comids, or only one dataset provided, then proceeds with the standard train-test split.

    :param dir_std_base:  The directory containing the standardized dataset generated from `fs_prep`
    :type dir_std_base: str | os.PathLike
    :param datasets: The unique dataset identifiers as a list
    :type datasets: list
    :param path_attr_config: path to the attribute configuration file
    :type path_attr_config: str | os.PathLike
    :param id_col: The column name of the comid in geodataframe as returned by `fs_retr_nhdp_comids_geom`, defaults to 'featureID'
    :type id_col: str, optional
    :param test_size: The fraction of data reserved for test data, defaults to 0.3
    :type test_size: float, optional
    :param random_state: The random state/random seed number, defaults to 42
    :type random_state: int, optional
    :seealso: :func:`train_test_split`
    :return: A dictionary containing the following objects:
        'dict_gdf_comids': dict of dataset keys, each with the geodataframe of comids
        'sub_test_ids': the comids corresponding to testing
        'sub_train_ids': the comids corresponding to training
    :rtype: dict
    """
    # Changelog/contributions
    # 2025-05-19 refactor to pass path_attr_config in lieu of attr_config, change default id_col to featureID, GL
    

    dict_gdf_comids = dict()
    for ds in datasets:
    
        # Generate the geodataframe in a standard format
        dict_resp_gdf = combine_resp_gdf_comid_wrap(dir_std_base,ds,path_attr_config=path_attr_config )
    
        dict_gdf_comids[ds] = dict_resp_gdf['gdf_comid']

    if len(datasets) > 1:
        common_locid = find_common_comid(dict_gdf_comids, column = id_col)
    else:
        common_locid = dict_gdf_comids[ds][id_col].tolist()
    
    # Create the train/test split() of comids. Note that duplicates are possible and must be removed!
    df_common_locids = pd.DataFrame({id_col:common_locid}).dropna().drop_duplicates()
    train_ids, test_ids = train_test_split(df_common_locids, test_size=test_size, random_state=random_state)

    # Compile results into a standard structure
    split_dict = {'dict_gdf_comids' : dict_gdf_comids,
                'sub_test_ids': test_ids[id_col],
                'sub_train_ids': train_ids[id_col]}
    return split_dict

def _warn_if_out_of_bounds(predictions: np.ndarray, feature_ids: pd.Series, 
                          min_lim: float, max_lim: float, resp_var: str, 
                          correction_is_active: bool, prediction_type: str):
    """
    Checks if predictions are within bounds and logs a specific warning if they are not.

    :param predictions: The 1D (values) or 3D (intervals) prediction array.
    :type predictions: np.ndarray
    :param feature_ids: The feature IDs corresponding to the predictions.
    :type feature_ids: pd.Series
    :param min_lim: The minimum allowable value for the prediction.
    :type min_lim: float
    :param max_lim: The maximum allowable value for the prediction.
    :type max_lim: float
    :param resp_var: The name of the response variable for logging purposes.
    :type resp_var: str
    :param correction_is_active: Flag indicating if a correction will be applied.
    :type correction_is_active: bool
    :param prediction_type: A string ('values' or 'intervals') for the log message.
    :type prediction_type: str
    """
    if isinstance(min_lim, str) and min_lim.lower() == 'none':
        min_lim = None
    if isinstance(max_lim, str) and max_lim.lower() == 'none':
        max_lim = None
        
    if min_lim is None and max_lim is None:
        return

    clip_min = min_lim if min_lim is not None else -np.inf
    clip_max = max_lim if max_lim is not None else np.inf

    # Find where original values are out of bounds
    out_of_bounds_mask = (predictions < clip_min) | (predictions > clip_max)
    
    if np.any(out_of_bounds_mask):
        # Logic to find affected IDs works for both 1D and 3D arrays
        if predictions.ndim == 3:
            affected_indices = np.any(out_of_bounds_mask, axis=(1, 2))
        else: # Handles 1D arrays
            affected_indices = out_of_bounds_mask
        
        affected_feature_ids = feature_ids[affected_indices].tolist()
        
        message = (
            f"'{resp_var}' prediction {prediction_type} found outside allowable range "
            f"[{clip_min}, {clip_max}] for {len(affected_feature_ids)} location(s): "
            f"{affected_feature_ids}."
        )
        if correction_is_active:
            message += " Post-hoc correction will be applied."
        else:
            message += " Correction was NOT applied because the relevant flag is disabled."
        logging.warning(message)

def clip_predictions(y_pred: np.ndarray, min_lim: float, max_lim: float) -> np.ndarray:
    """Clips a 1D prediction array to the specified min/max bounds.

    :param y_pred: The 1D prediction array.
    :type y_pred: np.ndarray
    :param min_lim: The minimum allowable value for the prediction.
    :type min_lim: float
    :param max_lim: The maximum allowable value for the prediction.
    :type max_lim: float
    :return: The clipped 1D prediction array.
    :rtype: np.ndarray
    """
    if min_lim is None and max_lim is None:
        return y_pred
    clip_min = min_lim if min_lim is not None else -np.inf
    clip_max = max_lim if max_lim is not None else np.inf
    return np.clip(y_pred, clip_min, clip_max)

def clip_pis(y_pis: np.ndarray, min_lim: float, max_lim: float) -> np.ndarray:
    """Clips prediction intervals (y_pis) to the specified min/max bounds.
    
    :param y_pis: The prediction interval array from MAPIE.
    :type y_pis: np.ndarray
    :param min_lim: The minimum allowable value for the prediction.
    :type min_lim: float
    :param max_lim: The maximum allowable value for the prediction.
    :type max_lim: float
    :return: The clipped prediction interval array.
    :rtype: np.ndarray    
    """
    if min_lim is None and max_lim is None:
        return y_pis
    clip_min = min_lim if min_lim is not None else -np.inf
    clip_max = max_lim if max_lim is not None else np.inf
    return np.clip(y_pis, clip_min, clip_max)