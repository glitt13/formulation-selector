import subprocess
import unittest
from pathlib import Path
import fs_prep.proc_eval_metrics as pem
from fs_algo.fs_algo_train_eval import _make_home_dir, _open_response_data_fs
import shutil

class TestFsPrepIntegration(unittest.TestCase):
    """
    Integration test for the fs_prep.py script.
    """
    # ----------------------------------------------------------------------- #
    # Parse the config file & run input checks:
    # ----------------------------------------------------------------------- #
    def setUp(self):
        """
        Read the config file and set up necessary paths.
        """
        self.dir_tests = Path(__file__).parent.resolve()
        self.path_cfg_prep = self.dir_tests / "config" / "xssa" / "xssa_prep_config.yaml"
        self.path_fs_prep = self.dir_tests / "config" / "xssa" / "prep_xssa_metrics.py"
        
        # Parse the config file
        self.home_dir = _make_home_dir([])
        self.col_schema_df = pem.read_schm_ls_of_dict(schema_path=self.path_cfg_prep)
        
        self.dir_save = Path(self.col_schema_df['dir_save'].iloc[0].format(home_dir=self.home_dir))
        self.dataset = self.col_schema_df['dataset_name'].iloc[0]
        self.formulation_id = pem.std_form_id(self.col_schema_df)
        
        self.dir_std_base = pem.dir_std_dataset(self.dir_save, self.dataset).parent
        self.dir_ds = pem.dir_std_dataset(self.dir_save, self.dataset)
        self.save_path_nc = pem.path_std_dataset(dir_save=self.dir_save,
                                                dataset_name=self.dataset,
                                                formulation_id=self.formulation_id,
                                                fmt='nc')
        

    # ----------------------------------------------------------------------- #
    #.                  RUN INTEGRATION TEST FOR FS_PREP
    # ----------------------------------------------------------------------- #
    def test_fs_prep_script_run(self):
        """
        Execute the fs_prep.py script as a subprocess and validate outputs.
        """
        # --- Pre-run checks and cleanup ---
        path_camels = Path(self.col_schema_df['path_camels'].iloc[0].format(home_dir=self.home_dir))
        path_data = Path(self.col_schema_df['path_data'].iloc[0].format(home_dir=self.home_dir))
        if not path_camels.exists():
            self.fail(f"Required input {path_camels} not specified.")
        if not path_data.exists():
            self.fail(f"Required input {path_data} not specified.")

        if self.save_path_nc.exists():
            self.save_path_nc.unlink()

        # --- Run the subprocess ---
        cmd = ["python", str(self.path_fs_prep), str(self.path_cfg_prep)]
        try:
            rslt = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"Subprocess stdout: {rslt.stdout}")
        except subprocess.CalledProcessError as e:
            self.fail(f"Subprocess failed with return code {e.returncode}.\nStdout: {e.stdout}\nStderr: {e.stderr}")
        print("Running fs_prep tests")
        # --- Post-run assertions ---
        self.assertTrue(self.dir_save.exists(), "The dir_save directory was not created.")
        self.assertTrue(self.save_path_nc.exists(), "The output .nc file was not created.")
        self.assertTrue(self.dir_std_base.exists(), "The dir_std_base directory was not created.")
        self.assertTrue(self.dir_ds.exists(), "The dir_ds directory was not created.")

        # --- Validate the content of the .nc file ---
        xr_dat = _open_response_data_fs(dir_std_base=self.dir_std_base,
                                        ds=self.dataset,
                                        mtch_str="*.nc")
        
        self.assertEqual(len(xr_dat['basin_name']), 220, "Incorrect number of basins.")
        self.assertIn('KGE', xr_dat.variables, "KGE variable is missing.")
        self.assertIn('RMSE', xr_dat.variables, "RMSE variable is missing.")
        self.assertIn('NSE', xr_dat.variables, "NSE variable is missing.")
        self.assertIn('gage_id', xr_dat.attrs.keys(), "gage_id attribute is missing.")
        self.assertEqual(xr_dat.attrs.get('gage_id'), 'basin_id', "Incorrect gage_id attribute value.")
        self.assertIn('featureID', xr_dat.attrs.keys(), "featureID attribute is missing.")
        self.assertEqual(xr_dat.attrs.get('featureID'), 'USGS-{gage_id}', "Incorrect featureID attribute value.")
        self.assertEqual(xr_dat.attrs.get('featureSource'), 'nwissite', "Incorrect featureSource attribute value.")
        self.assertEqual(xr_dat.coords._names, {'gage_id'}, "Incorrect coordinates.")

    def tearDown(self):
        """
        Clean up the test environment after each test method is run.
        """
        if self.dir_std_base.exists():
            shutil.rmtree(self.dir_std_base)
            print(f"Cleaned up directory: {self.dir_std_base}")


if __name__ == '__main__':
    unittest.main()