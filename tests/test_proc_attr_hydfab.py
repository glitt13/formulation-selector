import subprocess
import unittest
from pathlib import Path
import fs_prep.proc_eval_metrics as pem
import fs_algo.fs_algo_train_eval as fsate
import shutil
import pandas as pd
import geopandas as gpd

class TestFsPrepFsGrabIntegration(unittest.TestCase):
    """
    Integration test for the fs_prep.proc_eval_metrics -> proc.attr.hydfab's 
    fs_attrs_grab.R script.
    This is the point where non-transformed data have been fully acquired.
    """
    # ----------------------------------------------------------------------- #
    # Parse the config file & run input checks:
    # ----------------------------------------------------------------------- #
    def setUp(self):
        """
        Read the config files and set up necessary paths.
        """
        self.dir_tests = Path(__file__).parent.resolve()
        self.path_cfg_prep = self.dir_tests / "config" / "xssa" / "xssa_prep_config.yaml"
        self.path_fs_prep = self.dir_tests / "config" / "xssa" / "prep_xssa_metrics.py"

        self.path_fs_attrs_grab = self.dir_tests.parent / "pkg" / "proc.attr.hydfab" / "flow" / "fs_attrs_grab.R"
        # Parse the fs_prep config file
        self.home_dir = fsate._make_home_dir([])
        self.col_schema_df = pem.read_schm_ls_of_dict(schema_path=self.path_cfg_prep)
        
        self.dir_save = Path(self.col_schema_df['dir_save'].iloc[0].format(home_dir=self.home_dir))
        self.dataset = self.col_schema_df['dataset_name'].iloc[0]
        self.formulation_id = pem.std_form_id(self.col_schema_df)
        
        self.dir_std_base = pem.dir_std_dataset(self.dir_save, self.dataset).parent
        #self.dir_ds = pem.dir_std_dataset(self.dir_save, self.dataset)
        self.save_path_nc = pem.path_std_dataset(dir_save=self.dir_save,
                                                dataset_name=self.dataset,
                                                formulation_id=self.formulation_id,
                                                fmt='nc')

        # Parse the fs_algo config file
        self.path_attr_cfig = self.dir_tests / "config" / "xssa" / "xssa_attr_config.yaml"
        attr_cfig = fsate.AttrConfigAndVars(self.path_attr_cfig)
        attr_cfig._read_attr_config()
        dir_base = [x.get('dir_base') for x in attr_cfig.attr_config['file_io'] if 'dir_base' in x.keys()][0].format(home_dir=self.home_dir )
        self.dir_db_attrs = [x.get('dir_db_attrs') for x in attr_cfig.attr_config['file_io'] if 'dir_db_attrs' in x.keys()][0].format(dir_base=dir_base)
        self.dir_std_base = [x.get('dir_std_base') for x in attr_cfig.attr_config['file_io'] if 'dir_std_base' in x.keys()][0].format(dir_base=dir_base)
        self.ds_type = [x.get('ds_type') for x in attr_cfig.attr_config['file_io'] if 'ds_type' in x.keys()][0]
        write_type =  [x.get('write_type') for x in attr_cfig.attr_config['file_io'] if 'write_type' in x.keys()][0]

        if not write_type == 'parquet':
            raise ValueError(f"Testing expects the write_type ='parquet' in the attribute config file {self.apth_attr_cfig}")
        
        vals = {'ds_type':self.ds_type,'write_type':write_type,
                'dir_std_base':self.dir_std_base,'ds':self.dataset}
        path_meta_fstr = [x.get('path_meta') for x in attr_cfig.attr_config['file_io'] if 'path_meta' in x.keys()][0]
        self.path_meta = Path(path_meta_fstr.format(**vals))
        print(f"Metadata path: {self.path_meta}")
        if self.path_meta.exists():
            self.path_meta.unlink()
            print(f"Deleted {self.path_meta} for integration testing relating to fs_attrs_grab.R")
        # ----------------------------------------------------------------------- #
        # Run fs_prep first to generate the .nc as input for fs_attrs_grab.R
        # ----------------------------------------------------------------------- #
        # --- Pre-run checks and cleanup ---
        path_camels = Path(self.col_schema_df['path_camels'].iloc[0].format(home_dir=self.home_dir))
        path_data = Path(self.col_schema_df['path_data'].iloc[0].format(home_dir=self.home_dir))
        if not path_camels.exists():
            self.fail(f"Required input {path_camels} not specified.")
        if not path_data.exists():
            self.fail(f"Required input {path_data} not specified.")

        if self.save_path_nc.exists():
            self.save_path_nc.unlink()
            print(f"Deleted {self.save_path_nc} for integration testing relating to fs_prep->fs_attrs_grab")

        # --- Run the subprocess ---
        cmd_prep = ["python", str(self.path_fs_prep), str(self.path_cfg_prep)]
        try:
            rslt_prep = subprocess.run(cmd_prep, check=True, capture_output=True, text=True)
            print(f"{self.path_fs_prep} stdout: {rslt_prep.stdout}")
            print("Completed fs_prep")
        except subprocess.CalledProcessError as e:
            self.fail(f"Subprocess {self.path_fs_prep} failed with return code {e.returncode}.\nStdout: {e.stdout}\nStderr: {e.stderr}")
        
    def test_fs_attrs_grab_run(self):
        cmd_attrs_grab = ["Rscript", str(self.path_fs_attrs_grab), str(self.path_attr_cfig)]
        print(f"Running {cmd_attrs_grab}")
        try: 
            rslt_grab = subprocess.run(cmd_attrs_grab,check=True,capture_output=True,text=True)
            print(f"{self.path_fs_attrs_grab} stdout: {rslt_grab.stdout}")
        except subprocess.CalledProcessError as e:
            self.fail(f"Subprocess {self.path_fs_attrs_grab} failed with return code {e.returncode}.\nStdout: {e.stdout}\nStderr: {e.stderr}")

        parq_files = [x for x in Path(self.dir_db_attrs).iterdir() if x.is_file()]

        

        xr_dat = fsate._open_response_data_fs(dir_std_base=self.dir_std_base,
                                        ds=self.dataset,
                                        mtch_str="*.nc")
        # Run tests on attribute file generation
        self.assertGreaterEqual(xr_dat.sizes['gage_id'],len(parq_files)) # Note: not all locations could be acquired from hydrofabric data (3 out of 220 missing!)
        self.assertEqual('.parquet', parq_files[0].suffix)
        # Parquet content testing#
        first_parq_file = pd.read_parquet(parq_files[0])
        self.assertIn('featureSource',first_parq_file.columns)
        self.assertIn('featureID',first_parq_file.columns)
        self.assertIn('attribute',first_parq_file.columns)
        self.assertIn('value',first_parq_file.columns)
        # Metadata testing
        self.assertTrue(Path(self.path_meta).exists())
        df_meta = pd.read_parquet(self.path_meta)
        self.assertEqual(df_meta['gage_id'].nunique(),xr_dat.dims['gage_id'])
        self.assertTrue(self.ds_type in str(self.path_meta))
        print("Completed test_proc_attr_hydfab.R")

        # Ensure all_locs.gpkg created
        path_all_locs = Path(self.dir_db_attrs).parent / 'gpkg' / 'all_locs.gpkg' # Refer to proc.attr.hydfab::std_path_gpkg_db if this ever changes.
        self.assertTrue(path_all_locs.exists())

        # Ensure that the dataset-specific .gpkg created inside dataset dir
        path_fs_dat_resp = fsate._std_fs_prep_ds_paths(
            dir_std_base=self.dir_std_base,ds=self.dataset,mtch_str='*.nc')
        path_gpkg_ds = fsate._std_fs_prep_ds_companion_gpkg_path(path_fs_dat_resp[0])
        self.assertTrue(Path(path_gpkg_ds).exists())
        gpkg_ds = gpd.read_file(path_gpkg_ds)
        self.assertEqual(gpkg_ds.shape[0], df_meta['gage_id'].nunique())
        print("COMPLETED integration test for fs_pred -> proc.attr.hydfab's fs_attrs_grab.R")
if __name__ == '__main__':
    unittest.main()
