"""Unit tests for rafts_algo.qa_utils.resolve_qa_context.

Follows the project convention (see the mocking note atop
test_algo_train_all.py / README.claude "Unit tests should avoid mocking"):
builds a real, temporary attr_config/pred_config chain and a real directory
tree on disk (mirroring TestPredConfigParser's setUp in
test_algo_train_all.py), rather than mocking PredConfigParser or
AttrConfigAndVars.
"""
import tempfile
import unittest
from pathlib import Path

import yaml

import rafts_algo.qa_utils as qa_utils
import rafts_algo.utils as raftsutil


class TestResolveQaContext(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)

        self.dir_base = self.test_path / "base"
        self.dir_base.mkdir()
        self.dir_std_base = self.dir_base / "std"
        self.dir_std_base.mkdir()
        self.dir_db_attrs = self.dir_base / "db_attrs"
        self.dir_db_attrs.mkdir()

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

        self.pred_config = {
            "name_attr_config": self.path_attr_config.name,
            "name_algo_config": "algo.yaml",
            "ds_type": "eval",
            "write_type": "parquet",
            "path_meta": "path/meta/{ds}",
            "pred_file_comid_colname": "feature_id",
            "algo_response_vars": ["runoff"],
            "algo_type": ["rf"],
            "MAPIE_alpha": 0.1,
        }
        self.path_pred_config = self.test_path / "pred_config.yaml"

    def _write_pred_config(self):
        with open(self.path_pred_config, 'w') as f:
            yaml.dump(self.pred_config, f)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_resolve_qa_context_with_crosswalk(self):
        # Real crosswalk file, laid out like production:
        # {dir_base}/raw_workflow/crosswalk/test_crosswalk.parquet
        dir_crosswalk = self.dir_base / "raw_workflow" / "crosswalk"
        dir_crosswalk.mkdir(parents=True)
        path_crosswalk = dir_crosswalk / "test_crosswalk.parquet"
        path_crosswalk.touch()

        self.pred_config['path_crosswalk_ids'] = "{dir_base}/raw_workflow/crosswalk/test_crosswalk.parquet"
        self._write_pred_config()

        ctx = qa_utils.resolve_qa_context(self.path_pred_config)

        self.assertIsInstance(ctx.pred_cfg, raftsutil.PredConfigParser)
        # .resolve() on both sides: dir_base itself is unresolved (built from
        # tempfile.TemporaryDirectory(), which on macOS is a /var -> /private/var
        # symlink), so only a symmetric comparison is meaningful here.
        self.assertEqual(ctx.path_crosswalk_ids.resolve(), path_crosswalk.resolve())
        self.assertTrue(ctx.path_crosswalk_ids.exists())
        # crosswalk/ -> its parent (raw_workflow/)
        self.assertEqual(ctx.dir_raw_root.resolve(), (self.dir_base / "raw_workflow").resolve())

        # Same derivation rafts_regn_params_gpkg.py uses.
        expected_dir_regionalization = (
            self.dir_base / "output" / "regionalization" / self.path_pred_config.parent.name
        ).resolve()
        self.assertEqual(ctx.dir_regionalization.resolve(), expected_dir_regionalization)

        # dir_out/analysis/<dataset_name>/, dataset_name from formulation_metadata.
        expected_dir_qa_out = (self.dir_base / "output" / "analysis" / "test_dataset").resolve()
        self.assertEqual(ctx.dir_qa_out.resolve(), expected_dir_qa_out)

    def test_resolve_qa_context_no_crosswalk(self):
        # path_crosswalk_ids intentionally absent (e.g. divide-level prediction
        # configs that don't need a huc12 crosswalk).
        self._write_pred_config()

        ctx = qa_utils.resolve_qa_context(self.path_pred_config)

        self.assertIsNone(ctx.path_crosswalk_ids)
        self.assertIsNone(ctx.dir_raw_root)
        # The regionalization/analysis directories still resolve even without
        # a crosswalk -- they don't depend on path_crosswalk_ids.
        expected_dir_qa_out = (self.dir_base / "output" / "analysis" / "test_dataset").resolve()
        self.assertEqual(ctx.dir_qa_out.resolve(), expected_dir_qa_out)

    def test_resolve_qa_context_dataset_name_fallback(self):
        # An attr_config that resolves 'datasets' to an empty list (rather
        # than raising) should fall back to the pred_config's own parent
        # directory name for dir_qa_out, instead of crashing on datasets[0].
        self.attr_config['formulation_metadata'] = [{'datasets': []}]
        with open(self.path_attr_config, 'w') as f:
            yaml.dump(self.attr_config, f)
        self._write_pred_config()

        ctx = qa_utils.resolve_qa_context(self.path_pred_config)

        expected_dir_qa_out = (
            self.dir_base / "output" / "analysis" / self.path_pred_config.parent.name
        ).resolve()
        self.assertEqual(ctx.dir_qa_out.resolve(), expected_dir_qa_out)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
