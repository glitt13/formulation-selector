'''
Unit testing for the fs_algo functions split across the following files:
 - fs_algo_train.py
 - plots.py
 - tfrm_attr.py

example
> cd /path/to/fs_algo/fs_algo/tests/
> python -m unittest test_algo_train_eval

> coverage run -m unittest test_algo_train_eval.py  
> coverage report
> coverage html 

 # Useful for running in ipynb:
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

'''
import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import dask.dataframe as dd
from sklearn.ensemble import RandomForestRegressor, BaggingRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
import tempfile
from pathlib import Path
import fs_algo.fs_algo_train as fsalgo
import fs_algo.utils as fsutil
import fs_algo.plots as fsplots
import warnings
import xarray as xr
import os
import numpy as np
import forestci as fci
from scipy import stats as st
from sklearn.utils import resample
from sklearn.pipeline import Pipeline
from mapie.regression import MapieRegressor
import yaml
import shutil
import logging
import io
import sys
from contextlib import redirect_stdout
# Ignore some warnings that arise during testing:
import pytest
import geopandas as gpd
from shapely import Point
import joblib 
# Tell pytest natively to ignore these specific warnings for this entire file
pytestmark = pytest.mark.filterwarnings(
    "ignore:.*disp.*iprint.*:DeprecationWarning",
    "ignore:.*lbfgs failed to converge.*:sklearn.exceptions.ConvergenceWarning"
)

# %% UNIT TESTING FOR AttrConfigAndVars
parent_dir_test = Path(__file__).parent
dir_test_data = Path(parent_dir_test,"test_data")

# Set up logging, do not write to file!
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestAttrConfigAndVars(unittest.TestCase):
    print("Testing AttrConfigAndVars")
    @patch('builtins.open', new_callable=mock_open, read_data='''
            attr_select:
            - attr_vars: [attr1, attr2, attr3]
            file_io:
            - dir_base: "{home_dir}/base_dir"
            - dir_db_attrs: "{dir_base}/db_attrs"
            - dir_std_base: "{dir_base}/std_base"
            - home_dir: "{home_dir}"
            formulation_metadata:
            - datasets: ["dataset1", "dataset2"]
                ''')
    @patch('pathlib.Path.home', return_value='/mocked/home')
    def test_read_attr_config(self, mock_home, mock_file):
        print('    Testing _read_attr_config')
        path = Path(dir_test_data / 'attr_config.yaml')
        attr_obj = fsutil.AttrConfigAndVars(path)
        attr_obj._read_attr_config()

        # Test if the file is opened with the correct path
        mock_file.assert_called_once_with(path, 'r')

        # Test if Path.home() was called
        mock_home.assert_called()
        print(attr_obj.attrs_cfg_dict)
        # Test the parsed data from the config
        expected_home_dir = Path('/mocked/home')
        expected_attrs_cfg_dict = {
            'attrs_sel': ['attr1', 'attr2', 'attr3'],
            'dir_db_attrs': expected_home_dir / 'base_dir' / 'db_attrs',
            'dir_std_base': expected_home_dir / 'base_dir' / 'std_base',
            'dir_base': expected_home_dir / 'base_dir',
            'home_dir': expected_home_dir,
            'datasets': ['dataset1', 'dataset2']
        }
           
        self.assertEqual(attr_obj.attrs_cfg_dict, expected_attrs_cfg_dict)
        print("✅ test_read_attr_config test passed.")

class TestFsReadAttrComid(unittest.TestCase):   
    @patch('fs_algo.utils.dd.read_parquet')
    def test_fs_read_attr_comid(self, mock_dd_read_parquet):
        print("    Testing fs_read_attr_comid")

        # Create a Dask DataFrame from a Pandas DataFrame
        mock_pdf = pd.DataFrame({
            'data_source': ['hydroatlas__v1','hydroatlas__v1'],
            'dl_timestamp': ['2024-07-26 08:59:36','2024-07-26 08:59:36'],
            'attribute': ['pet_mm_s01', 'cly_pc_sav'],
            'value': [58, 21],
            'featureID': ['1520007','1520007'],
            'featureSource': ['COMID','COMID']
        })
        mock_ddf = dd.from_pandas(mock_pdf, npartitions=1)

        # Patch Dask read_parquet to return this mock DDF
        mock_dd_read_parquet.return_value = mock_ddf

        # Inputs
        dir_db_attrs = 'mock_dir'
        comids_resp = ['1520007']
        attrs_sel = ['pet_mm_s01', 'cly_pc_sav']

        # Call function
        result_df = fsutil.fs_read_attr_comid(
            dir_db_attrs=dir_db_attrs,
            comids_resp=comids_resp,
            attrs_sel=attrs_sel
        )

        # Assertions
        self.assertEqual(result_df.shape[0], 2)
        self.assertIn('1520007', result_df['featureID'].values)
        self.assertIn('pet_mm_s01', result_df['attribute'].values)
        self.assertIn('COMID', result_df['featureSource'].values)
        self.assertIn('value', result_df.columns)
        self.assertIn('data_source', result_df.columns)
        print("✅ fs_read_attr_comid muliple-row test passed.")

        # When only one attribute requested
        single_result = fsutil.fs_read_attr_comid(dir_db_attrs=dir_db_attrs,
                                                            comids_resp= comids_resp,attrs_sel= ['pet_mm_s01'])
        self.assertIn('pet_mm_s01',single_result['attribute'].values)
        self.assertNotIn('cly_pc_sav',single_result['attribute'].values)

        # When COMID requested that doesn't exist
        with self.assertLogs(level="INFO") as cm:
            fsutil.fs_read_attr_comid(
                dir_db_attrs=dir_db_attrs,
                comids_resp=['010101010'],
                attrs_sel=['pet_mm_s01']
            )
        self.assertTrue(
            any("None of the provided featureIDs exist" in m for m in cm.output)
        )

        # When attribute requested that doesn't exist
        with self.assertLogs(level="INFO") as cm:
            fsutil.fs_read_attr_comid(dir_db_attrs=dir_db_attrs,
                                            comids_resp= comids_resp,
                                            attrs_sel= ['nonexistent'])
        self.assertTrue(any("Missing attributes include:" in m for m in cm.output))
        self.assertTrue(any("nonexistent" in m for m in cm.output))
        print("✅ fs_read_attr_comid single-row test passed.")

class TestHomeDirUtilities(unittest.TestCase):
    def test_make_home_dir(self):
        # 1. Test None/Empty triggers Path.home()
        self.assertEqual(fsutil._make_home_dir([]), Path.home())
        self.assertEqual(fsutil._make_home_dir([None]), Path.home())

        # 2. Test tilde expansion
        self.assertEqual(fsutil._make_home_dir(["~/some/path"]), Path.home() / "some/path")

        # 3. Test explicit path that doesn't exist (logs warning, uses default system home)
        with self.assertLogs(level='WARNING'):
            res = fsutil._make_home_dir(["/fake/path/that/does/not/exist/12345"])
            self.assertEqual(res, Path.home())

# %% UNIT TESTING FOR AlgoConfigParser
class TestAlgoConfigParser(unittest.TestCase):
    print("Testing AlgoConfigParser")

    def setUp(self):
        self.test_data_dir = dir_test_data

    def load_yaml(self, filename):
        filepath = self.test_data_dir / filename
        with open(filepath, "r") as f:
            return yaml.safe_load(f), filepath

    def test_01_no_uncertainty_defaults(self):
        _, filepath = self.load_yaml("test_algo_config_01_nouncertainty.yaml")
        config = fsutil.AlgoConfigParser(filepath)
        config._read_algo_config()
        self.assertIsInstance(config.algo_cfg_unc_dict, dict)
        self.assertIn("algo_cfg_dict", config.algo_cfg_unc_dict)
        print("Completed Test AlgoConfig parsing - YAML file #1")

    def test_02_uncertainty_defaults(self):
        _, filepath = self.load_yaml("test_algo_config_02_uncertainty.yaml")
        config = fsutil.AlgoConfigParser(filepath)
        config._read_algo_config()
        self.assertIn("mapie", config.algo_cfg_unc_dict["algo_unc_dict"]["uncertainty_cfg"])
        print("Completed Test AlgoConfig parsing - YAML file #2")


    def test_03_uncertainty_params_custom(self):
        _, filepath = self.load_yaml("test_algo_config_03_uncertainty_nodefaults.yaml")
        config = fsutil.AlgoConfigParser(filepath)
        config._read_algo_config()

        self.assertIn("algo_unc_dict", config.algo_cfg_unc_dict)
        self.assertIn("uncertainty_cfg", config.algo_cfg_unc_dict["algo_unc_dict"])
        self.assertIn("bagging", config.algo_cfg_unc_dict["algo_unc_dict"]["uncertainty_cfg"])

        bagging_cfg = config.algo_cfg_unc_dict["algo_unc_dict"]["uncertainty_cfg"]["bagging"]
        self.assertIsInstance(bagging_cfg, list)
        self.assertIsInstance(bagging_cfg[0], dict)
        self.assertIn("n_algos", bagging_cfg[0])
        n_algos_test = 20
        self.assertEqual(bagging_cfg[0]["n_algos"], n_algos_test)
        print("Completed Test AlgoConfig parsing - YAML file #3")

    def test_04_missing_required_parameter(self):
        _, filepath = self.load_yaml("test_algo_config_04_errortest_parammissing.yaml")
        config = fsutil.AlgoConfigParser(filepath)
        with self.assertRaises(KeyError) as context:
            config._read_algo_config()
        self.assertIn("'algorithms'", str(context.exception))
        print("Completed Test AlgoConfig parsing - YAML file #4")

    def test_05_wrong_datatype(self):
        _, filepath = self.load_yaml("test_algo_config_05_errortest_paramdatatype.yaml")
        config = fsutil.AlgoConfigParser(filepath)
        with self.assertRaises(TypeError) as context:
            config._read_algo_config()
        self.assertIn("'seed' must be an integer", str(context.exception))
        print("Completed Test AlgoConfig parsing - YAML file #5")

    def test_06_invalid_uncertainty_structure(self):
        _, filepath = self.load_yaml("test_algo_config_06_errortest_unc_param_struct.yaml")
        config = fsutil.AlgoConfigParser(filepath)
        with self.assertRaises(TypeError) as context:
            config._read_algo_config()
        self.assertIn("The 'uncertainty' block must be a dictionary", str(context.exception))
        print("Completed Test AlgoConfig parsing - YAML file #6")


    def test_Print(self):
        print("✅ TestAlgoConfigParser test passed.")

class TestCheckAttributesExist(unittest.TestCase):
    print('Testing _check_attributes_exist')
    def test_check_attributes_exist(self):
        mock_pdf = pd.DataFrame({
            'data_source': 'hydroatlas__v1',
            'dl_timestamp': '2024-07-26 08:59:36',
            'attribute': ['pet_mm_s01', 'cly_pc_sav','pet_mm_s01', 'cly_pc_sav'],
            'value': [58, 21, 65,32],
            'featureID': ['1520007','1520007','1623207','1623207'],
            'featureSource': 'COMID'
            })
        
        with warnings.catch_warnings(record = True) as w:
            warnings.simplefilter("always")
            fsutil._check_attributes_exist(mock_pdf,pd.Series(['pet_mm_s01','cly_pc_sav']))
            self.assertEqual(len(w),0)

        mock_pdf_bad = mock_pdf.copy()
        mock_pdf_bad.drop(index=0, inplace=True)

        with self.assertLogs(level="INFO") as cm:
            fsutil._check_attributes_exist(
                mock_pdf_bad, pd.Series(['pet_mm_s01', 'cly_pc_sav'])
            )

        #self.assertTrue(any("None of the provided featureIDs exist" in m for m in cm.output))
        self.assertTrue(any("Not all featureID groupings" in m for m in cm.output))
        self.assertTrue(any("TOTAL unique locations with missing attributes" in m for m in cm.output))
        self.assertTrue(any("TOTAL MISSING ATTRS" in m for m in cm.output))

        # with self.assertWarns(UserWarning):
        #     fsutil._check_attributes_exist(mock_pdf_bad,pd.Series(['pet_mm_s01','cly_pc_sav']))
        
        print("✅ _check_attributes_exist test passed.")
class TestFsRetrNhdpComids(unittest.TestCase):

    def test_fs_retr_nhdp_comids(self):

        # Define test inputs
        featureSource = 'nwissite'
        featureID = 'USGS-{gage_id}'
        gage_ids = ["01031500", "08070000"]

        result = fsutil.fs_retr_nhdp_comids_geom(featureSource, featureID, gage_ids)

        # Assertions
        self.assertListEqual(result['comid'].tolist(), ['1722317', '1520007'])
        self.assertEqual(result.columns.tolist(), ['comid','gage_id', 'geometry'])
        print("✅ test_fs_retr_nhdp_comids test passed.")

    def test_fs_retr_nhdp_comids_geom_wrap_cached(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gpkg_path = Path(tmpdir) / "cache.gpkg"
            
            # Create a dummy cached file with our target gage_id
            gdf = gpd.GeoDataFrame({
                'gage_id': ['gage1'],
                'comid': ['111']
            }, geometry=[Point(0,0)], crs="EPSG:4326")
            gdf.to_file(gpkg_path, driver="GPKG", layer="outlet")
            
            # Request the exact same gage_id to hit the fully cached branch
            result = fsutil.fs_retr_nhdp_comids_geom_wrap(
                path_save_gpkg=gpkg_path,
                gage_ids=['gage1']
            )
            self.assertEqual(result['comid'].iloc[0], '111')

class TestFindFeatSrceId(unittest.TestCase):

    def test_find_feat_srce_id(self):
        attr_config = {'col_schema': [{'featureID': 'USGS-{gage_id}'},
                        {'featureSource': 'nwissite'}],
                        'loc_id_read': [{'gage_id': 'gage_id'},
                        {'loc_id_filepath': '{dir_std_base}/juliemai-xSSA/eval/metrics/juliemai-xSSA_Raven_blended.csv'},
                        {'featureID_loc': 'USGS-{gage_id}'},
                        {'featureSource_loc': 'nwissite'}],
                        }
        rslt = fsutil._find_feat_srce_id(attr_config = attr_config)
        self.assertEqual(rslt,['nwissite','USGS-{gage_id}'])
        print("✅ _find_feat_srce_id standard test passed.")
    # Raise error when featureSource not provided:
    def test_missing_feat_srce(self):
        attr_config_miss = {'col_schema': [{'featureID': 'USGS-{gage_id}'},
                                            {'fe0a0ur0eS0ou0r0ce': 'nwissite'}],
                            'loc_id_read': {'gage_id': 'gage_id'}}
        with self.assertRaises(ValueError):
            fsutil._find_feat_srce_id(attr_config = attr_config_miss, dat_resp=None)
        print("✅ _find_feat_srce_id missing test passed.")
    def test_netcdf_attributes(self):
         # Create a mock xarray.Dataset object w/ attributes
        mock_xr = MagicMock(spec=xr.Dataset)
        mock_xr.attrs = {'featureSource': 'nwissite',
                         'featureID': 'USGS-{gage_id}'}

        rslt = fsutil._find_feat_srce_id(mock_xr)
        self.assertEqual(rslt,['nwissite','USGS-{gage_id}'])
        print("✅ _find_feat_srce_id mock xarray test passed.")

    # Raise error when featureID not provided:
    def test_missing_feat_id(self):
         # Create a mock xarray.Dataset object
        mock_xr = MagicMock(spec=xr.Dataset)
        mock_xr.attrs = {'featureSource': 'nwissite',
                         'f0e1a0tu1reID': 'USGS-{gage_id}'}

        with self.assertRaises(ValueError):
            fsutil._find_feat_srce_id(mock_xr)
        print("✅ _find_feat_srce_id mock xarray missing featureID test passed.")
    # Test when dataset does not have any attributes but does have config:
    def test_missing_attrs(self):
        mock_xr = MagicMock(spec=xr.Dataset)
        mock_xr.attrs = {'notit': 'blah',
                         'alsonotit': 'bleh'}
        attr_config = {'col_schema': [{'featureID': 'USGS-{gage_id}'},
                {'featureSource': 'nwissite'}],
                'loc_id_read': [{'gage_id': 'gage_id'},
                {'loc_id_filepath': '{dir_std_base}/juliemai-xSSA/eval/metrics/juliemai-xSSA_Raven_blended.csv'},
                {'featureID_loc': 'USGS-{gage_id}'},
                {'featureSource_loc': 'nwissite'}],
                }
        rslt = fsutil._find_feat_srce_id(dat_resp = mock_xr, attr_config = attr_config)
        self.assertEqual(rslt,['nwissite','USGS-{gage_id}'])
        print("✅ _find_feat_srce_id missing attributes test passed.")

class build_cfig_path(unittest.TestCase):
    def test_build_cfig_path(self):
        dir_base = tempfile.gettempdir()
        dir_new = Path(dir_base)/Path('testingitout')
        dir_new.mkdir(exist_ok=True)
        with self.assertRaises(FileNotFoundError):
            path_cfig = fsutil.build_cfig_path(dir_new,'test.yaml')

        with self.assertRaises(FileNotFoundError):
            fsutil.build_cfig_path('this_dir/doesnt/exist','test.yaml')

        with self.assertLogs(level="INFO") as cm:
            fsutil.build_cfig_path(dir_new,'')
        self.assertTrue(any("configuration file may not have specified the path or file name." in m for m in cm.output))
        print("✅ test_build_cfig_path build config paths test passed.")

    @patch('pathlib.Path.exists')
    def test_file_exists(self, mock_exists):
        dir_base = tempfile.gettempdir()
        dir_new = Path(dir_base)/Path('testingitout')
        path_known_config = Path(dir_new)/Path('test.yaml')
        path_or_name_cfig = Path(dir_new)/Path('a_nother_config.yaml')

        # Mock the existence of the directories and files, side_effect attr of mock object allws specifying a function or iterable called e/ time mock is called
        mock_exists.side_effect = lambda: True # Tells the mock object to return True everytime the path.exists method called
        rslt = fsutil.build_cfig_path(path_known_config, path_or_name_cfig)
        # Assert
        self.assertEqual(rslt, path_or_name_cfig)
        self.assertEqual(mock_exists.call_count, 2)
        print("✅ build_cfig_path build config paths test with mock paths passed.")

class TestFsSaveAlgoDirStruct(unittest.TestCase):
    def test_fs_save_algo_dir_struct(self):
        dir_base = tempfile.gettempdir()
        rslt = fsutil.fs_save_algo_dir_struct(dir_base)
        self.assertIn('dir_out', rslt.keys())
        self.assertIn('dir_out_alg_base', rslt.keys())
        self.assertTrue(Path(rslt['dir_out_alg_base']).exists)

        with self.assertRaises(ValueError):
            fsutil.fs_save_algo_dir_struct(dir_base + '/not_a_dir/')
        print("✅ fs_save_algo_dir_struct creating directory structure for outputs passed.")
class TestOpenResponseDataFs(unittest.TestCase):
    dir_std_base = tempfile.gettempdir()

    def test_open_response_data_fs(self):

        with self.assertRaisesRegex(ValueError, 'Could not identify an approach to read in dataset'):
            fsutil._open_response_data_fs(self.dir_std_base,ds='not_a_ds')
        print("✅ _open_response_data_fs unable to read dataset test passed.")
#%% ALGO TRAIN & EVAL
class TestStdAlgoPath(unittest.TestCase):

    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.exists')
    def test_std_algo_path(self, mock_exists, mock_mkdir):
        dir_out_alg_ds = tempfile.gettempdir()
        algo = 'test_algo'
        metric = 'test_metric'
        dataset_id = 'test_dataset'
        expected_path = Path(dir_out_alg_ds) / 'algo_test_algo_test_metric__test_dataset.joblib'

        # Mock the existence of the directory
        mock_exists.return_value = True

        result = fsutil.std_algo_path(dir_out_alg_ds, algo, metric, dataset_id)
        mock_mkdir.assert_called_once_with(exist_ok=True, parents=True)
        self.assertEqual(result, expected_path)

class TestStdPredPath(unittest.TestCase):
    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.exists')
    def test_std_pred_path(self, mock_exists, mock_mkdir):
        dir_out = '/some/directory'
        algo = 'test_algo'
        metric = 'test_metric'
        dataset_id = 'test_dataset'
        expected_path = Path(dir_out) / 'algorithm_predictions' / dataset_id / 'pred_test_algo_test_metric__test_dataset.parquet'

        # Mock the existence of the directory
        mock_exists.return_value = True
        result = fsutil.std_pred_path(dir_out, algo, metric, dataset_id)

        mock_mkdir.assert_called_once_with(exist_ok=True, parents=True)
        self.assertEqual(result, expected_path)

class TestReadPredComid(unittest.TestCase):
    @patch('pathlib.Path.exists')
    @patch('pandas.read_csv')
    def test_read_pred_comid(self, mock_read_csv, mock_exists):
        # Arrange
        path_pred_locs = '/some/directory/predictions.csv'
        comid_pred_col = 'comid'
        mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame({comid_pred_col: [1, 2, 3]})

        result = fsutil._read_pred_comid(path_pred_locs, comid_pred_col)
        mock_exists.assert_called_once_with()
        mock_read_csv.assert_called_once_with(path_pred_locs)
        self.assertEqual(result, ['1', '2', '3'])

    @patch('pathlib.Path.exists')
    @patch('pandas.read_csv')
    def test_read_csv_error(self, mock_read_csv, mock_exists):
        path_pred_locs = '/some/directory/predictions.csv'
        comid_pred_col = 'comid'
        mock_exists.return_value = True
        mock_read_csv.side_effect = Exception("Read CSV error")
        with self.assertRaises(ValueError):
            fsutil._read_pred_comid(path_pred_locs, comid_pred_col)

    @patch('pathlib.Path.exists')
    def test_unsupported_file_extension(self, mock_exists):
        path_pred_locs = '/some/directory/predictions.txt'
        comid_pred_col = 'comid'
        mock_exists.return_value = True
        with self.assertRaises(ValueError):
            fsutil._read_pred_comid(path_pred_locs, comid_pred_col)


# %% UNIT TEST FOR AlgoTrainEval class
class TestAlgoTrainEval(unittest.TestCase):
    print("Testing AlgoTrainEval")
    def setUp(self):
        # Create a simple DataFrame for testing
        data = {
            'comid': [f'id_{i}' for i in range(20)],
            'attr1': list(range(1, 21)),
            'attr2': list(range(21, 1, -1)),
            'metric1': [0.1, 0.9, 0.3, 0.1, 0.8, 0.2, 0.7, 0.5, 0.3, 0.6,
                        0.2, 0.2, 0.3, 0.6, 0.5, 0.2, 0.7, 0.9, 0.3, 0.1]
        }
        self.df = pd.DataFrame(data)

        # Variables and configurations for algorithms
        self.attrs = ['attr1', 'attr2']
        self.algo_config = {
            'rf': {'n_estimators': 10},
            'mlp': {'hidden_layer_sizes': (10,), 'max_iter': 2000}
        }
        # self.bagging_ci_params = {'n_algos': 5}  # Example parameters
        self.dataset_id = 'test_dataset'
        self.metric = 'metric1'
        self.verbose = False
        # Output directory
        self.dir_out_alg_ds = tempfile.gettempdir()
        self.confidence_levels = [90, 95]  # Example parameters
        # self.mapie_alpha = [0.1, 0.2]
        uncertainty_cfg = {
            'forestci': [{'fci_flag': True}],
            'bagging': [{'n_algos': 10}],
            'mapie': [{
                'alpha': [0.05, 0.32],
                'method': 'plus',
                'cv': 10,
                'agg_function': 'median'
            }]
        }
        
        # Instantiate AlgoTrainEval class
        self.train_eval = fsalgo.AlgoTrainEval(df=self.df, attrs=self.attrs, 
                                        algo_config=self.algo_config,
                                        uncertainty=uncertainty_cfg,
                                 dir_out_alg_ds=self.dir_out_alg_ds, dataset_id=self.dataset_id,
                                 metr=self.metric, test_size=0.4, rs=42,
                                 test_id_col='comid',
                                 confidence_levels = self.confidence_levels,
                                 )

    def test_split_data(self):
        # Test data splitting
        self.train_eval.split_data()
        self.assertEqual(len(self.train_eval.X_train), 12)
        self.assertEqual(len(self.train_eval.X_test), 8)
        self.assertEqual(len(self.train_eval.y_train), 12)
        self.assertEqual(len(self.train_eval.y_test), 8)

    def test_train_algos(self):
        # Test algorithm training
        self.train_eval.split_data()
        self.train_eval.train_algos()

        self.assertIn('rf', self.train_eval.algs_dict)
        self.assertIsInstance(self.train_eval.algs_dict['rf']['algo'], RandomForestRegressor)

        self.assertIn('mlp', self.train_eval.algs_dict)
        self.assertIsInstance(self.train_eval.algs_dict['mlp']['algo'], MLPRegressor)

        #self.assertEqual(len(self.algo_config), len(self.train_eval))

    def test_predict_algos(self):
        # Test algorithm predictions
        self.train_eval.split_data()
        self.train_eval.train_algos()

        preds = self.train_eval.predict_algos()

        self.assertIn('rf', preds)
        self.assertIn('mlp', preds)
        self.assertEqual(len(preds['rf']['y_pred']), len(self.train_eval.X_test))  # Number of test samples
        self.assertEqual(len(preds['mlp']['y_pred']), len(self.train_eval.X_test))

    def test_evaluate_algos(self):
        # Test evaluation of algorithms
        self.train_eval.split_data()
        self.train_eval.train_algos()
        self.train_eval.predict_algos()

        eval_dict = self.train_eval.evaluate_algos()

        self.assertIn('rf', eval_dict)
        self.assertIn('mlp', eval_dict)
        self.assertIn('mse', eval_dict['rf'])
        self.assertIn('r2', eval_dict['mlp'])

    @patch('joblib.dump')
    def test_save_algos(self, mock_dump):
        # Test saving algorithms to disk
        self.train_eval.split_data()
        self.train_eval.train_algos()

        # Mock joblib.dump to avoid file operations
        self.train_eval.save_algos()
        self.assertTrue(mock_dump.called)

        for algo in self.train_eval.algs_dict.keys():
            self.assertIn('file_pipe', self.train_eval.algs_dict[algo])

    def test_org_metadata_alg(self):
        # Test organizing metadata
        self.train_eval.split_data()
        self.train_eval.train_algos()
        self.train_eval.predict_algos()
        self.train_eval.evaluate_algos()

        # Mock saving algorithms and call organization
        with patch('joblib.dump'):
            self.train_eval.save_algos()

        self.train_eval.org_metadata_alg()

        # Check eval_df is correctly populated
        self.assertFalse(self.train_eval.eval_df.empty)
        self.assertIn('dataset', self.train_eval.eval_df.columns)
        self.assertIn('file_pipe', self.train_eval.eval_df.columns)
        self.assertIn('algo', self.train_eval.eval_df.columns)
        self.assertEqual(self.train_eval.eval_df['dataset'].iloc[0], self.dataset_id)

    def test_calculate_forestci_uncertainty(self):
        # Test the calculate_forestci_uncertainty method
        self.train_eval.split_data()
        self.train_eval.train_algos()

        rf = self.train_eval.algs_dict['rf']['algo']
        ci_dict = self.train_eval.calculate_forestci_uncertainty(rf, self.train_eval.X_train, self.train_eval.X_test)

        self.assertIn('ci_95', ci_dict)  # Check for 95% confidence interval
        self.assertIn('lower_bound', ci_dict['ci_95'])
        self.assertIn('upper_bound', ci_dict['ci_95'])
        self.assertEqual(len(ci_dict['ci_95']['lower_bound']), len(self.train_eval.X_test))
        self.assertEqual(len(ci_dict['ci_95']['upper_bound']), len(self.train_eval.X_test))

    def test_calculate_bagging_ci(self):
        # Test the calculate_bagging_ci method
        self.train_eval.split_data()
        self.train_eval.train_algos()

        best_algo = self.train_eval.algs_dict['rf']['algo']  # Use the trained RF model
        self.train_eval.calculate_bagging_ci('rf', best_algo)

        # Check if uncertainty data is stored
        self.assertIn('Uncertainty', self.train_eval.algs_dict['rf'])
        self.assertIn('bagging_mean_pred', self.train_eval.algs_dict['rf']['Uncertainty'])
        self.assertIn('bagging_std_pred', self.train_eval.algs_dict['rf']['Uncertainty'])
        self.assertIn('bagging_confidence_intervals', self.train_eval.algs_dict['rf']['Uncertainty'])
        
        # Check confidence intervals
        ci = self.train_eval.algs_dict['rf']['Uncertainty']['bagging_confidence_intervals']
        self.assertIn('confidence_level_90', ci)
        self.assertIn('confidence_level_95', ci)
        
        self.assertEqual(len(ci['confidence_level_90']['lower_bound']), len(self.train_eval.X_test))
        self.assertEqual(len(ci['confidence_level_90']['upper_bound']), len(self.train_eval.X_test))
        self.assertEqual(len(ci['confidence_level_95']['lower_bound']), len(self.train_eval.X_test))
        self.assertEqual(len(ci['confidence_level_95']['upper_bound']), len(self.train_eval.X_test))

    def test_calculate_mapie(self):
        """Test that calculate_mapie correctly fits MapieRegressor and stores it in algs_dict."""
        self.train_eval.split_data()
        self.train_eval.train_algos()

        self.train_eval.calculate_mapie()
        
        # Debugging: print algs_dict to check its structure
        print("algs_dict content:", self.train_eval.algs_dict)
    
        # Ensure 'rf' exists in algs_dict before accessing it
        self.assertIn('rf', self.train_eval.algs_dict, "Key 'rf' not found in algs_dict")
    
        # Ensure 'mapie' is stored correctly under 'rf'
        self.assertIn('mapie', self.train_eval.algs_dict['rf'], "Key 'mapie' not found under 'rf' in algs_dict")
        self.assertIsInstance(self.train_eval.algs_dict['rf']['mapie'], MapieRegressor)
        
        # Check that MapieRegressor is fitted
        mapie = self.train_eval.algs_dict['rf']['mapie']

class TestAlgoTrainEvalMlti(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        data = {
            'comid':[f'id_{i}' for i in range(15)],
            'attr1': [1, 2, 3, 4, 5,1, 2, 3, 4, 5,1, 2, 3, 4, 5],
            'attr2': [5, 4, 3, 2, 1,5, 4, 3, 2, 1,5, 4, 3, 2, 1],
            'metric': [0.1, 0.9, 0.3, 0.1, 0.8,0.1, 0.9, 0.3, 0.1, 0.8,0.1, 0.9, 0.3, 0.1, 0.8]
        }
        self.df = pd.DataFrame(data)
        self.attrs = ['attr1', 'attr2']
        self.algo_config = {
            'rf': [{'n_estimators': [10, 50]}],
            'mlp': [{'hidden_layer_sizes': [(10,), (5, 5)], 'max_iter': [2000]}]
        }
        self.dir_out_alg_ds = './'
        self.dataset_id = 'test_dataset'
        self.metric = 'metric'
        self.test_size = 0.3
        self.test_id_col = 'comid'
        self.rs = 32
        self.verbose = False

        # self.bagging_ci_params = {'n_algos': 5}  # Example parameters
        self.confidence_levels = [90, 95]  # Example parameters
        # self.mapie_alpha = [0.1, 0.2]
        uncertainty_cfg = {
            'forestci': [{'fci_flag': True}],
            'bagging': [{'n_algos': 10}],
            'mapie': [{
                'alpha': [0.05, 0.32],
                'method': 'plus',
                'cv': 10,
                'agg_function': 'median'
            }]
        }

        self.algo_train_eval = fsalgo.AlgoTrainEval(df=self.df, attrs=self.attrs, algo_config=self.algo_config,
                                             uncertainty=uncertainty_cfg,
                                              dir_out_alg_ds=self.dir_out_alg_ds,dataset_id=self.dataset_id,
                                              metr=self.metric, test_size=self.test_size, rs=self.rs,
                                              test_id_col=self.test_id_col,
                                              verbose=self.verbose,
                                              confidence_levels = self.confidence_levels,
                                              )
    def test_initialization(self):
        self.assertEqual(self.algo_train_eval.df.shape, self.df.shape)
        self.assertEqual(self.algo_train_eval.attrs, self.attrs)
        self.assertEqual(self.algo_train_eval.algo_config, self.algo_config)
        self.assertEqual(self.algo_train_eval.dir_out_alg_ds, self.dir_out_alg_ds)
        self.assertEqual(self.algo_train_eval.metric, self.metric)
        self.assertEqual(self.algo_train_eval.test_size, self.test_size)
        self.assertEqual(self.algo_train_eval.rs, self.rs)
        self.assertEqual(self.algo_train_eval.dataset_id, self.dataset_id)
        self.assertEqual(self.algo_train_eval.verbose, self.verbose)

    def test_split_data(self):
        self.algo_train_eval.split_data()
        self.assertFalse(self.algo_train_eval.X_train.empty)
        self.assertFalse(self.algo_train_eval.X_test.empty)
        self.assertFalse(self.algo_train_eval.y_train.empty)
        self.assertFalse(self.algo_train_eval.y_test.empty)
        self.assertEqual(len(self.algo_train_eval.X_train) + len(self.algo_train_eval.X_test), len(self.df.dropna()))

    def test_select_algs_grid_search(self):
        self.algo_train_eval.select_algs_grid_search()
        self.assertIn('mlp', self.algo_train_eval.grid_search_algs)
        self.assertNotIn('mlp', self.algo_train_eval.algo_config)
        self.assertIn('mlp', self.algo_train_eval.algo_config_grid)


    def test_train_algos(self):
        self.algo_train_eval.split_data()
        self.algo_train_eval.select_algs_grid_search()
        self.algo_train_eval.train_algos_grid_search()
        self.assertTrue('rf' in  self.algo_train_eval.algo_config_grid)
        self.assertIn('mlp', self.algo_train_eval.algs_dict)



    def test_empty_dict(self):
        d = {}
        self.algo_train_eval.convert_to_list(d)
        self.assertEqual(d, {})

    def test_single_level_dict(self):
        d = {'a': 1, 'b': 2}
        self.algo_train_eval.convert_to_list(d)
        self.assertEqual(d, {'a': [1], 'b': [2]})

    def test_nested_dict(self):
        d = {'a': {'sub1': 1, 'sub2': 2}, 'b': {'sub1': 3, 'sub2': {'subsub1': 4}}, 'c': 5}
        self.algo_train_eval.convert_to_list(d)
        self.assertEqual(d, {'a': {'sub1': [1], 'sub2': [2]}, 'b': {'sub1': [3], 'sub2': {'subsub1': [4]}}, 'c': [5]})

    def test_already_list(self):
        d = {'a': [1, 2], 'b': {'sub1': [3, 4]}}
        self.algo_train_eval.convert_to_list(d)
        self.assertEqual(d, {'a': [1, 2], 'b': {'sub1': [3, 4]}})

    # def test_calculate_forestci_uncertainty(self):
    #     # Test the calculate_forestci_uncertainty method
    #     self.algo_train_eval.split_data()
    #     self.algo_train_eval.train_algos()

    #     rf = self.algo_train_eval.algs_dict['rf']['algo']
    #     ci_dict = self.algo_train_eval.calculate_forestci_uncertainty(rf, self.algo_train_eval.X_train, self.algo_train_eval.X_test)

    #     self.assertIn('ci_95', ci_dict)  # Check for 95% confidence interval
    #     self.assertIn('lower_bound', ci_dict['ci_95'])
    #     self.assertIn('upper_bound', ci_dict['ci_95'])
    #     self.assertEqual(len(ci_dict['ci_95']['lower_bound']), len(self.algo_train_eval.X_test))
    #     self.assertEqual(len(ci_dict['ci_95']['upper_bound']), len(self.algo_train_eval.X_test))

    # def test_calculate_bagging_ci(self):
    #     # Test the calculate_bagging_ci method
    #     self.algo_train_eval.split_data()
    #     self.algo_train_eval.train_algos()

    #     best_algo = self.algo_train_eval.algs_dict['rf']['algo']  # Use the trained RF model
    #     self.algo_train_eval.calculate_bagging_ci('rf', best_algo)

    #     # Check if uncertainty data is stored
    #     self.assertIn('Uncertainty', self.algo_train_eval.algs_dict['rf'])
    #     self.assertIn('bagging_mean_pred', self.algo_train_eval.algs_dict['rf']['Uncertainty'])
    #     self.assertIn('bagging_std_pred', self.algo_train_eval.algs_dict['rf']['Uncertainty'])
    #     self.assertIn('bagging_confidence_intervals', self.algo_train_eval.algs_dict['rf']['Uncertainty'])
        
    #     # Check confidence intervals
    #     ci = self.algo_train_eval.algs_dict['rf']['Uncertainty']['bagging_confidence_intervals']
    #     self.assertIn('confidence_level_90', ci)
    #     self.assertIn('confidence_level_95', ci)
        
    #     self.assertEqual(len(ci['confidence_level_90']['lower_bound']), len(self.algo_train_eval.X_test))
    #     self.assertEqual(len(ci['confidence_level_90']['upper_bound']), len(self.algo_train_eval.X_test))
    #     self.assertEqual(len(ci['confidence_level_95']['lower_bound']), len(self.algo_train_eval.X_test))
    #     self.assertEqual(len(ci['confidence_level_95']['upper_bound']), len(self.algo_train_eval.X_test))
        
class TestAlgoTrainEvalSngl(unittest.TestCase):
    # An algo_config with singular hyperparameter value
    def setUp(self):
        # Sample data for testing
        self.df = pd.DataFrame({
            'comid': [f'id_{i}' for i in range(5)],
            'attr1': [1, 2, 3, 4, 5],
            'attr2': [5, 4, 3, 2, 1],
            'metric': [1, 0, 1, 0, 1]
        })
        self.attrs = ['attr1', 'attr2']
        self.algo_config = {'mlp': {'max_iter': [100]}}
        uncertainty_cfg = {
            'forestci': [{'fci_flag': True}],
            'bagging': [{'n_algos': 10}],
            'mapie': [{
                'alpha': [0.32],
                'method': 'plus',
                'cv': 10,
                'agg_function': 'median'
            }]
        }
        self.dir_out_alg_ds = tempfile.gettempdir()
        self.dataset_id = 'dataset_1'
        self.metr = 'metric'
        self.test_size = 0.3
        self.rs = 32
        self.verbose = False
        self.algo_config_grid = dict()
        self.grid_search_algs=list()

        self.algo_train_eval = fsalgo.AlgoTrainEval(
            df=self.df, attrs=self.attrs, algo_config=self.algo_config, 
            uncertainty=uncertainty_cfg,
            dir_out_alg_ds=self.dir_out_alg_ds,
            dataset_id=self.dataset_id, metr=self.metr, test_size=self.test_size, 
            rs=self.rs, verbose=self.verbose,
            test_id_col='comid'
        )

    @patch('fs_algo.fs_algo_train.joblib.dump')
    @patch.object(fsalgo.AlgoTrainEval, 'calculate_bagging_ci')
    @patch.object(fsalgo.AlgoTrainEval, 'calculate_mapie')
    @patch.object(fsalgo.AlgoTrainEval, 'split_data')
    @patch.object(fsalgo.AlgoTrainEval, 'select_algs_grid_search')
    @patch.object(fsalgo.AlgoTrainEval, 'train_algos_grid_search')
    @patch.object(fsalgo.AlgoTrainEval, 'train_algos')
    def test_train_eval(self, mock_train_algos, mock_train_algos_grid_search,
                        mock_select_algs_grid_search, mock_split_data,
                        mock_calc_bagging_ci,mock_calc_mapie, mock_dump):
        # Mock the methods to avoid actual execution
        mock_split_data.return_value = None
        mock_select_algs_grid_search.return_value = None
        mock_train_algos_grid_search.return_value = None
        mock_train_algos.return_value = None
        mock_calc_bagging_ci.return_value = None
        mock_calc_mapie.return_value = None

        # Mock pipeline with a .predict() method
        mock_pipeline = MagicMock()
        test_indices = [1, 4]  # Example indices from the original df
    
        # Create a realistic X_test DataFrame with a valid index
        self.algo_train_eval.X_test = self.algo_train_eval.df.loc[test_indices, self.algo_train_eval.attrs]
        self.algo_train_eval.y_test = self.algo_train_eval.df.loc[test_indices, self.algo_train_eval.metric]
    
        # Update mock prediction length to match the new X_test size
        mock_pipeline.predict.return_value = np.random.rand(len(test_indices))
        self.algo_train_eval.preds_dict = {}
        self.algo_train_eval.uncertainty = {
            'forestci': [{'fci_flag': True}],
            'bagging': [{'n_algos': 10}],
            'mapie': [{
                'alpha': [0.32],
                'method': 'plus',
                'cv': 10,
                'agg_function': 'median'
            }]
        }   
        
        # Setup algs_dict with required keys
        self.algo_train_eval.algs_dict = {
            'mlp': {
                'algo': MLPRegressor(),
                'pipeline': mock_pipeline,
                'type': 'MLP',
                'metric': 'metric',
                'Uncertainty': {}
            }
        }

        # Call the method
        self.algo_train_eval.train_eval()

        # Assert that the methods were or were not called
        mock_split_data.assert_called_once()
        mock_select_algs_grid_search.assert_called_once()
        mock_train_algos_grid_search.assert_not_called()
        mock_train_algos.assert_called_once()
        mock_calc_bagging_ci.assert_called_once()
        mock_calc_mapie.assert_called_once()

class TestAlgoTrainEvalBasic(unittest.TestCase):

    def setUp(self):
        # Set up a small test dataframe
        self.df = pd.DataFrame({
            'comid': [f'id_{i}' for i in range(15)],
            'attr1': [1, 2, 3, 4, 5,1, 2, 3, 4, 5,1, 2, 3, 4, 5],
            'attr2': [5, 4, 3, 2, 1,5, 4, 3, 2, 1,5, 4, 3, 2, 1],
            'target': [10, 15, 20, 25, 30,10, 15, 20, 25, 30,10, 15, 20, 25, 30]
        })
        self.attrs = ['attr1', 'attr2']
        self.algo_config = {
            'rf': [{'n_estimators': [10,30,40]}],
            'mlp': [{'hidden_layer_sizes': (50,)}]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)

        self.dir_out_alg_ds = temp_dir
        self.dataset_id = 'test_ds'
        self.metric = 'target'
        self.test_size = 0.2
        self.rs = 42
        self.verbose = False
        self.algo_config_grid = dict()

        # self.bagging_ci_params = {'n_algos': 5}  # Example parameters
        self.confidence_levels = [90, 95]  # Example parameters
        # self.mapie_alpha = [0.1, 0.2]
        uncertainty_cfg = {
            'forestci': [{'fci_flag': True}],
            'bagging': [{'n_algos': 10}],
            'mapie': [{
                'alpha': [0.2, 0.32],
                'method': 'plus',
                'cv': 10,
                'agg_function': 'median'
            }]
        }
        
        self.algo = fsalgo.AlgoTrainEval(df=self.df, attrs=self.attrs, algo_config=self.algo_config,
                                  uncertainty=uncertainty_cfg,
                                  dir_out_alg_ds=self.dir_out_alg_ds, dataset_id=self.dataset_id, 
                                  metr=self.metric, test_size=self.test_size, rs=self.rs, 
                                  test_id_col='comid',
                                  verbose=self.verbose,
                                  confidence_levels = self.confidence_levels,
                                  )

    @patch('joblib.dump')  # Mock saving the model to disk
    @patch('sklearn.model_selection.train_test_split', return_value=(pd.DataFrame(), pd.DataFrame(), pd.Series(), pd.Series()))
    @patch('sklearn.ensemble.RandomForestRegressor')
    @patch('sklearn.neural_network.MLPRegressor')
    def test_train_eval(self, MockMLP, MockRF, mock_train_test_split, mock_joblib_dump):
        # Mocking train algorithms
        mock_rf_model = MagicMock()
        mock_mlp_model = MagicMock()

        # Assign these mock models to the mock class
        MockRF.return_value = mock_rf_model
        MockMLP.return_value = mock_mlp_model

        # Mock the predictions
        mock_rf_model.predict.return_value = [10, 20, 30]
        mock_mlp_model.predict.return_value = [15, 25, 35]

        # Run the method
        self.algo.train_eval()

        # Check if the train_test_split was called correctly
        #mock_train_test_split.assert_called()

        # Check that the RandomForest and MLP models were trained
        # MockRF.assert_called_once()
        # MockMLP.assert_called_once()
        # self.assertIn('rf',self.algo_config_grid)
        # self.assertIn('mlp',self.algo_config)

        # Check predictions and evaluations were made
        self.assertIn('rf', self.algo.preds_dict)
        self.assertIn('mlp', self.algo.preds_dict)



        self.assertIn('rf', self.algo.eval_dict)
        self.assertIn('mlp', self.algo.eval_dict)

        # Check if models were saved
        mock_joblib_dump.assert_called()

        # Check eval dataframe was created
        self.assertIsInstance(self.algo.eval_df, pd.DataFrame)
        self.assertFalse(self.algo.eval_df.empty)
    
# %%

class TestReadMetadata(unittest.TestCase):
    @patch('pandas.read_parquet')
    def test_read_metadata_file_not_found(self, mock_read_parquet):
        """Test that FileNotFoundError is raised when metadata file is missing."""
        path_attr_config = Path(dir_test_data, "attr_config.yaml")
        # Update 2026-04-24: changed to return None to accommodate param regionalization ignoring attr_config.yaml
        result = fsutil._read_metadata(path_attr_config, ds='dataset_name')
        self.assertIsNone(result)

    @patch('pandas.read_parquet')
    def test_read_metadata_reads_parquet(self, mock_read_parquet):
        """Test reading metadata when parquet file is present and correct."""
        # Arrange
        mock_df = pd.DataFrame({'gage_id': [12345, 25432], 'featureID': ['prvi-cat-135', 'ak-cat-123']})
        mock_read_parquet.return_value = mock_df

        # Use real attr_config.yaml
        path_attr_config = Path(dir_test_data, "attr_config.yaml")
        attr_cfig = fsutil.AttrConfigAndVars(path_attr_config)
        
        # Patch AttrConfigAndVars and mock its instance
        with patch("fs_algo.utils.AttrConfigAndVars") as MockAttrClass:
            mock_instance = MockAttrClass.return_value
            mock_instance._read_attr_config.return_value = None

            mock_instance.attr_config = {
                'file_io': [
                    {'ds_type': 'training'},
                    {'write_type': 'parquet'},
                    {'path_meta': "{dir_std_base}/{ds}/{ds}_{ds_type}.{write_type}"}
                ]
            }
            # Only override dir_std_base key
            mock_instance.attrs_cfg_dict = {'dir_std_base': dir_test_data}

            # Act
            result = fsutil._read_metadata(path_attr_config, ds="dataset_name")

            # Assert
            mock_read_parquet.assert_called_once()
            pd.testing.assert_frame_equal(result, mock_df)


# %% Creating Unit tests for PredConfigParser
class TestPredConfigParser(unittest.TestCase):

    def setUp(self):
        # Create temporary directory for config files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)

        # Create dummy dir_base and dir_std_base
        self.dir_base = self.test_path / "base"
        self.dir_base.mkdir()
        self.dir_std_base = self.dir_base / "std"
        self.dir_std_base.mkdir()

        # Also create dir_db_attrs if referenced
        self.dir_db_attrs = self.dir_base / "db_attrs"
        self.dir_db_attrs.mkdir()

        # Attribute config file
        self.attr_config = {
            'file_io': [
                {'home_dir': str(self.test_path)},
                {'dir_base': str(self.dir_base)},
                {'dir_std_base': str(self.dir_std_base)},
                {'dir_db_attrs': str(self.dir_db_attrs)}
            ],
            'formulation_metadata': [
                {'datasets': ['test_dataset']}
            ],
            'attr_select': [
                {'static_vars': ['slope', 'elevation']}
            ]
        }
        
        self.path_attr_config = self.test_path / "attr_config.yaml"
        with open(self.path_attr_config, 'w') as f:
            yaml.dump(self.attr_config, f)

        # Prediction config file
        self.pred_config = {
            "name_attr_config": self.path_attr_config.name,
            "name_algo_config": "algo.yaml",
            "name_tfrm_config": "tfrm.yaml",
            "path_tfrm_script": "/some/script.py",
            "conda_env": "test_env",
            "ds_type": "eval",
            "write_type": "parquet",
            "path_meta": "path/meta/{ds}",
            "pred_file_comid_colname": "feature_id",
            "algo_response_vars": ["runoff"],
            "algo_type": ["rf"],
            "MAPIE_alpha": 0.1
        }
        self.path_pred_config = self.test_path / "pred_config.yaml"
        with open(self.path_pred_config, 'w') as f:
            yaml.dump(self.pred_config, f)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_read_pred_config_success(self):
        parser = fsutil.PredConfigParser(str(self.path_pred_config))
        parser._read_pred_config()

        self.assertEqual(parser.pred_cfg_dict['ds_type'], 'eval')
        self.assertEqual(parser.pred_cfg_dict['write_type'], 'parquet')
        self.assertEqual(parser.pred_cfg_dict['datasets'], ['test_dataset'])
        self.assertEqual(parser.pred_cfg_dict['algo_type'], ['rf'])
        self.assertEqual(parser.pred_cfg_dict['path_pred_config'], str(self.path_pred_config))
        self.assertEqual(parser.pred_cfg_dict['dir_base'], self.dir_base)
        self.assertEqual(parser.pred_cfg_dict['dir_std_base'], self.dir_std_base)

    def test_read_pred_config_missing_required(self):
        # Remove a required field
        del self.pred_config["write_type"]
        with open(self.path_pred_config, 'w') as f:
            yaml.dump(self.pred_config, f)

        parser = fsutil.PredConfigParser(str(self.path_pred_config))
        with self.assertRaises(ValueError) as context:
            parser._read_pred_config()
        self.assertIn("Missing required keys", str(context.exception))

    def test_nonexistent_pred_config_file(self):
        parser = fsutil.PredConfigParser(str(self.test_path / "nonexistent.yaml"))
        with self.assertRaises(FileNotFoundError):
            parser._read_pred_config()

    def test_missing_dir_base_or_std(self):
        # Delete dir_base to simulate missing directory
        self.dir_std_base.rmdir()
        shutil.rmtree(self.dir_base)

        parser = fsutil.PredConfigParser(str(self.path_pred_config))
        with self.assertRaises(FileNotFoundError) as context:
            parser._read_pred_config()
        self.assertIn("Resolved dir_base path does not exist", str(context.exception))

def test_build_pred_locs_path():
    # Given
    template = "{dir_std_base}/{ds}/pred_{ds}_{ds_type}.{write_type}"
    dir_std_base = "/test/standardized"
    ds = "camels"
    ds_type = "prediction"
    write_type = "parquet"

    # When
    result_path = fsutil.build_pred_locs_path(template, dir_std_base, ds, ds_type, write_type)

    # Then
    expected = Path("/test/standardized/camels/pred_camels_prediction.parquet")
    assert result_path == expected

# %% UNIT TESTING FOR WARNING AND CLIPPING FUNCTIONS
class TestWarningAndClippingFunctions(unittest.TestCase):
    print("Testing warning and clipping helper functions")

    def setUp(self):
        """Set up common data for all test cases."""
        self.feature_ids = pd.Index(['ID_01', 'ID_02', 'ID_03', 'ID_04'])
        self.resp_var = 'test_metric'
        
        # Data for 1D prediction values
        self.y_pred = np.array([0.5, -0.2, 1.3, 0.8])
        
        # Data for 3D prediction intervals
        self.y_pis = np.array([
            [[0.1], [0.9]],  # In bounds
            [[-0.5], [0.8]], # Out of bounds (low)
            [[0.2], [1.5]],  # Out of bounds (high)
            [[-0.7], [1.7]]  # Out of bounds (both)
        ])

    # --- Tests for warn_if_out_of_bounds ---

    def test_warn_values_correction_active(self):
        """Test warning for 1D values when correction is ON."""
        with self.assertLogs(level='WARNING') as cm:  # root logger
            fsutil._warn_if_out_of_bounds(
                self.y_pred, self.feature_ids, 0.0, 1.0, self.resp_var,
                correction_is_active=True, prediction_type="values"
            )
            self.assertEqual(len(cm.output), 1)
            self.assertIn("Post-hoc correction will be applied", cm.output[0])
            self.assertIn("['ID_02', 'ID_03']", cm.output[0])
        print("✅ test_warn_values_correction_active passed.")

    def test_warn_intervals_correction_inactive(self):
        """Test warning for 3D intervals when correction is OFF."""
        with self.assertLogs(level='WARNING') as cm:  # root logger
            fsutil._warn_if_out_of_bounds(
                self.y_pis, self.feature_ids, 0.0, 1.0, self.resp_var,
                correction_is_active=False, prediction_type="intervals"
            )
            self.assertEqual(len(cm.output), 1)
            self.assertIn("Correction was NOT applied", cm.output[0])
            self.assertIn("['ID_02', 'ID_03', 'ID_04']", cm.output[0])
        print("✅ test_warn_intervals_correction_inactive passed.")

    @patch('logging.getLogger')
    def test_no_warning_when_in_bounds(self, mock_get_logger):
        """Test that no warning is logged when all values are within bounds."""
        # Configure the patch to return a mock logger we can inspect
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Run the function that would normally create and use a logger
        fsutil._warn_if_out_of_bounds(
            np.array([0.1, 0.5, 0.9]), self.feature_ids, 0.0, 1.0, self.resp_var,
            correction_is_active=True, prediction_type="values"
        )

        # Assert that the .warning() method on our mock logger was never called
        mock_logger.warning.assert_not_called()
        print("✅ test_no_warning_when_in_bounds passed.")

    # --- Tests for clip_predictions (1D) ---

    def test_clip_predictions_correctly(self):
        """Test that 1D prediction values are clipped correctly."""
        expected = np.array([0.5, 0.0, 1.0, 0.8])
        result = fsutil.clip_predictions(self.y_pred, 0.0, 1.0)
        np.testing.assert_array_equal(result, expected)
        print("✅ test_clip_predictions_correctly passed.")

    def test_clip_predictions_no_bounds(self):
        """Test that 1D predictions are unchanged if no bounds are provided."""
        result = fsutil.clip_predictions(self.y_pred, None, None)
        np.testing.assert_array_equal(result, self.y_pred)
        print("✅ test_clip_predictions_no_bounds passed.")

    # --- Tests for clip_pis (3D) ---

    def test_clip_pis_correctly(self):
        """Test that 3D prediction intervals are clipped correctly."""
        expected = np.array([
            [[0.1], [0.9]],
            [[0.0], [0.8]],
            [[0.2], [1.0]],
            [[0.0], [1.0]]
        ])
        result = fsutil.clip_pis(self.y_pis, 0.0, 1.0)
        np.testing.assert_array_equal(result, expected)
        print("✅ test_clip_pis_correctly passed.")

    def test_clip_pis_min_only(self):
        """Test that 3D PIs are clipped correctly with only a min bound."""
        expected = np.array([
            [[0.1], [0.9]],
            [[0.0], [0.8]],
            [[0.2], [1.5]],
            [[0.0], [1.7]]
        ])
        result = fsutil.clip_pis(self.y_pis, 0.0, None)
        np.testing.assert_array_equal(result, expected)
        print("✅ test_clip_pis_min_only passed.")

class TestValidationUtilities(unittest.TestCase):
    def setUp(self):
        # Reuse existing dummy data strategy
        self.good_df = pd.DataFrame({
            'comid': ['123', '456'],
            'attr1': [1.0, 2.0]
        })
        self.bad_df = pd.DataFrame({
            'comid': [123, 456], # Int instead of string!
            'attr1': ['bad', 'data']
        })

    def test_validate_input_attributes_exit(self):
        # 1. Test happy path (no exit)
        fsutil.validate_input_attributes(self.good_df, arg_val=False) 
        
        # 2. Test fatal exit using your existing bad dataframe
        with self.assertRaises(SystemExit):
            fsutil.validate_input_attributes(self.bad_df, arg_val=True)

    def test_validate_dat_resp_schema_exit(self):
        # Create an xarray dataset missing the required 'gage_id' coordinate
        bad_xr = xr.Dataset(
            {"NSE": (("wrong_id",), [0.8])},
            coords={"wrong_id": ["G1"]}
        )
        bad_xr.attrs['metric_mappings'] = 'NSE'
        
        # Trigger the fatal Pandera validation failure
        with self.assertRaises(SystemExit):
            fsutil.validate_dat_resp_schema(
                dat_resp=bad_xr, 
                valid_metrics=["NSE"], 
                col_locid="featureID", 
                arg_val=True
            )

class TestCombineRespGdfComidWrap(unittest.TestCase):
    def test_combine_resp_gdf_comid_wrap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            ds_dir = base_dir / "test_ds"
            ds_dir.mkdir()
            
            # 1. Create a real temporary NetCDF file
            ds = xr.Dataset(
                {"metric1": (("gage_id",), [10.0, 20.0])},
                coords={"gage_id": ["gage1", "gage2"]}
            )
            ds.to_netcdf(ds_dir / "test_dataset.nc")
            
            # 2. Create a real temporary GPKG file
            gdf = gpd.GeoDataFrame({
                "gage_id": ["gage1", "gage2"],
                "comid": ["111", "222"]
            }, geometry=[Point(0,0), Point(1,1)], crs="EPSG:4326")
            gdf.to_file(ds_dir / "test_dataset_loc.gpkg", driver="GPKG", layer="outlet")
            
            # 3. Use an existing config file path for the test
            path_attr_config = dir_test_data / "attr_config.yaml"
            
            # 4. Run the wrapper
            result = fsutil.combine_resp_gdf_comid_wrap(
                dir_std_base=base_dir,
                ds="test_ds",
                path_attr_config=path_attr_config
            )
            
            self.assertIn('dat_resp', result)
            self.assertIn('gdf_comid', result)
            self.assertEqual(result['gdf_comid'].shape[0], 2)

class TestSimpleUtilities(unittest.TestCase):
    
    def test_check_attr_rm_dupes(self):
        # Create a dataframe with a duplicate attribute entry
        df = pd.DataFrame({
            'featureID': ['1', '1', '2'],
            'featureSource': ['src', 'src', 'src'],
            'data_source': ['ds', 'ds', 'ds'],
            'attribute': ['A', 'A', 'B'],
            'value': [10, 10, 20],
            'dl_timestamp': ['2023-01-01', '2023-01-02', '2023-01-01']
        })
        
        # Test that it drops the older duplicate (keeps 2023-01-02 by default ascending=True)
        # Note: ascending=True actually keeps the 'first' row after sorting.
        cleaned_df = fsutil._check_attr_rm_dupes(df, sort_col='dl_timestamp', ascending=False)
        self.assertEqual(len(cleaned_df), 2)
        
    def test_find_common_comid(self):
        # Pass a dictionary of multiple DataFrames to find intersecting IDs
        df1 = gpd.GeoDataFrame({'featureID': ['A', 'B', 'C']})
        df2 = gpd.GeoDataFrame({'featureID': ['B', 'C', 'D']})
        
        common = fsutil.find_common_comid({'ds1': df1, 'ds2': df2}, column='featureID')
        self.assertCountEqual(common, ['B', 'C'])

    def test_standard_path_generators(self):
        # Quickly hit all the basic Path generation functions
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "fake_dir"
            
            p1 = fsutil._std_fs_prep_ds_companion_gpkg_path(base / "data.nc")
            self.assertEqual(p1.name, "data_loc.gpkg")
            
            p2 = fsutil.std_eval_metrs_path(base, "my_ds", "NSE")
            self.assertEqual(p2.name, "algo_eval_my_ds_NSE.csv")
            
            p3 = fsutil.std_test_pred_obs_path(base, "my_ds", "NSE")
            self.assertEqual(p3.name, "pred_obs_my_ds_NSE.csv")

    def test_other_validation_wrappers(self):
        # 1. validate_gdf_comid_schema
        good_gdf = gpd.GeoDataFrame({'comid': ['123'], 'geometry': [Point(0,0)]})
        bad_gdf = pd.DataFrame({'wrong_col': [1]}) # Not a GeoDataFrame, missing cols
        
        fsutil.validate_gdf_comid_schema(good_gdf, arg_val=False) # Happy path (disabled)
        with self.assertRaises(SystemExit):
            fsutil.validate_gdf_comid_schema(bad_gdf, arg_val=True) # Fatal crash
            
        # 2. validate_df_comids
        good_attr = pd.DataFrame({'featureID': ['1'], 'attribute': ['A'], 'value': [1.0]})
        bad_attr = pd.DataFrame({'featureID': [1]}) # Int instead of str
        
        fsutil.validate_df_comids(good_attr, arg_val=False)
        with self.assertRaises(SystemExit):
             fsutil.validate_df_comids(bad_attr, arg_val=True)
             
        # 3. write_validated_prediction_output
        bad_pred = pd.DataFrame({'bad': ['data']})
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "out.parquet"
            with self.assertRaises(SystemExit):
                fsutil.write_validated_prediction_output(bad_pred, out_path, True, ["NSE"])

class TestValidationUtilities(unittest.TestCase):
    def setUp(self):
        # Reuse existing dummy data strategy
        self.good_df = pd.DataFrame({
            'comid': ['123', '456'],
            'attr1': [1.0, 2.0]
        })
        self.bad_df = pd.DataFrame({
            'comid': [123, 456], # Int instead of string!
            'attr1': ['bad', 'data']
        })

    def test_validate_input_attributes_exit(self):
        # 1. Test happy path (no exit)
        fsutil.validate_input_attributes(self.good_df, arg_val=False) 
        
        # 2. Test fatal exit using your existing bad dataframe
        with self.assertRaises(SystemExit):
            fsutil.validate_input_attributes(self.bad_df, arg_val=True)

    def test_other_validation_wrappers(self):
        # 1. validate_gdf_comid_schema
        good_gdf = gpd.GeoDataFrame({'comid': ['123'], 'geometry': [Point(0,0)]})
        bad_gdf = pd.DataFrame({'wrong_col': [1]}) # Not a GeoDataFrame, missing cols
        
        fsutil.validate_gdf_comid_schema(good_gdf, arg_val=False) # Happy path (disabled)
        with self.assertRaises(SystemExit):
            fsutil.validate_gdf_comid_schema(bad_gdf, arg_val=True) # Fatal crash
            
        # 2. validate_df_comids
        good_attr = pd.DataFrame({'featureID': ['1'], 'attribute': ['A'], 'value': [1.0]})
        bad_attr = pd.DataFrame({'featureID': [1]}) # Int instead of str
        
        fsutil.validate_df_comids(good_attr, arg_val=False)
        with self.assertRaises(SystemExit):
             fsutil.validate_df_comids(bad_attr, arg_val=True)
             
        # 3. write_validated_prediction_output
        bad_pred = pd.DataFrame({'bad': ['data']})
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "out.parquet"
            with self.assertRaises(SystemExit):
                fsutil.write_validated_prediction_output(bad_pred, out_path, True, ["NSE"])

    # DROP THE NEW TESTS RIGHT HERE!
    def test_load_validated_pipeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipe_path = Path(tmpdir) / "model.joblib"
            
            # Dump a dummy dictionary using joblib
            dummy_pipe = {"model_type": "RandomForest", "target": "NSE"}
            joblib.dump(dummy_pipe, pipe_path)
            
            # Happy path (no validation, just loads the file)
            loaded = fsutil.load_validated_pipeline(pipe_path, arg_val=False)
            self.assertEqual(loaded["target"], "NSE")
            
            # Fatal path: Missing file
            with self.assertRaises(FileNotFoundError):
                fsutil.load_validated_pipeline(Path(tmpdir) / "nope.joblib")
                
            # Fatal path: Fails Pydantic validation (assuming dummy_pipe is incomplete)
            with self.assertRaises(SystemExit):
                fsutil.load_validated_pipeline(pipe_path, arg_val=True)

    def test_write_validated_evaluation_output_exit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            bad_df = pd.DataFrame({"random_col": [1, 2]}) # Wrong schema
            
            # Triggers the validation exit
            with self.assertRaises(SystemExit):
                fsutil.write_validated_evaluation_output(
                    rslt_eval_df=bad_df, 
                    dir_out_alg_ds=out_dir, 
                    ds="my_ds", 
                    valid_metrics=["NSE"], 
                    arg_val=True
                )

class TestTrainTestSplitWrap(unittest.TestCase):
    def test_split_train_test_comid_wrap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            ds_dir = base_dir / "test_ds"
            ds_dir.mkdir()
            
            # Create dummy NetCDF
            ds = xr.Dataset(
                {"metric1": (("gage_id",), [10.0, 20.0, 30.0, 40.0])},
                coords={"gage_id": ["g1", "g2", "g3", "g4"]}
            )
            ds.to_netcdf(ds_dir / "test_dataset.nc")
            
            # Create dummy GPKG
            gdf = gpd.GeoDataFrame({
                "gage_id": ["g1", "g2", "g3", "g4"],
                "comid": ["11", "22", "33", "44"]
            }, geometry=[Point(0,0), Point(1,1), Point(2,2), Point(3,3)], crs="EPSG:4326")
            gdf.to_file(ds_dir / "test_dataset_loc.gpkg", driver="GPKG", layer="outlet")
            
            path_attr_config = dir_test_data / "attr_config.yaml"
            
            # Run the split wrapper
            result = fsutil.split_train_test_comid_wrap(
                dir_std_base=base_dir,
                datasets=["test_ds"],
                path_attr_config=path_attr_config,
                id_col='comid',
                test_size=0.5 # Split the 4 rows perfectly in half
            )
            
            # Validate the output dictionary
            self.assertIn('dict_gdf_comids', result)
            self.assertEqual(len(result['sub_test_ids']), 2)
            self.assertEqual(len(result['sub_train_ids']), 2)

if __name__ == '__main__':

    unittest.main()
    logging.shutdown()
    