'''
Unit tests for the hfATLAS to RaFTS conversion module

example::
> cd /path/to/fs_prep/fs_prep/tests/
> python -m unittest test_fs_hfatlas_to_rafts.py
or if interested in unit testing coverage:
> python -m coverage run -m unittest
> python -m coverage report 
# and may also run the following to generate an html: > python -m coverage html 

notes::
Changelog/contributions
    2026-04-28 Originally created to test hfATLAS data formatting and VPU generation. Create by SS with the help of AI.
'''

import unittest
import pandas as pd
from pathlib import Path

# Updated import path to reflect the new location in fs_prep/flow
from fs_prep.flow.fs_hfatlas_to_rafts import clean_hfatlas_columns, generate_vpu_attr_filepath

class TestHfAtlasToRafts(unittest.TestCase):

    def test_clean_hfatlas_columns(self):
        """Test that pint-aware string representations of tuples are correctly parsed."""
        # Setup mock dataframe with complex string column headers
        data = {
            "('TOT_AET_hfa', 'millimeter')": [1.0, 2.0],
            "('TOT_CLAYAVE_hfa', 'percent')": [30.0, 40.0],
            "standard_col": [1, 2]
        }
        df = pd.DataFrame(data)
        
        # Execute
        cleaned_df = clean_hfatlas_columns(df)
        
        # Verify
        expected_cols = ['TOT_AET_hfa', 'TOT_CLAYAVE_hfa', 'standard_col']
        self.assertListEqual(list(cleaned_df.columns), expected_cols)
        
    def test_clean_hfatlas_columns_invalid_tuple(self):
        """Test that malformed tuple strings fail gracefully without crashing."""
        data = {
            "('TOT_AET_hfa', 'millimeter": [1.0], # Missing parenthesis
            "TOT_BFI_hfa": [50.0]
        }
        df = pd.DataFrame(data)
        cleaned_df = clean_hfatlas_columns(df)
        
        # It should ignore the malformed string and leave it as is
        self.assertIn("('TOT_AET_hfa', 'millimeter", cleaned_df.columns)

    def test_generate_vpu_attr_filepath(self):
        """Test that standard file paths are correctly constructed."""
        base_dir = Path("/mock/dir_db_attrs")
        dataset_name = "hfatlas"
        vpuid = "01"
        
        # Execute
        result_path = generate_vpu_attr_filepath(base_dir, dataset_name, vpuid)
        
        # Verify
        expected_path = Path("/mock/dir_db_attrs/hfatlas/01/attr_01.parquet")
        self.assertEqual(result_path, expected_path)

if __name__ == '__main__':
    unittest.main()