# scripts/qa/ + rafts_algo/flow/qa/ -- standardized post-hoc QA checks

Diagnostic checks for regionalization workflows, meant to run *after* a
workflow's normal processing steps as an optional sanity pass -- not part of
the pipeline's own pass/fail contract. Every check here always exits 0,
regardless of what it finds; a gap is something to print and report, not a
reason to fail a build. Read the printed summary (and any `qa_out_*.csv` it
writes) to see whether anything needs follow-up.

## Why these exist

Divide-level maps and regionalized parameter tables both depend on a chain
of joins -- attributes -> huc12 crosswalk -> hydrofabric geometry -> SQLite
output -- and a silent gap anywhere in that chain shows up downstream as
"missing" locations that are easy to misdiagnose as a rendering bug rather
than a data coverage issue (see the divide-level map fixes on the
`fix-map-crosswalk-dropped-divides` branch, which is exactly how these
checks came about: several real gaps and one that turned out *not* to be a
gap at all were only distinguishable by checking the actual divide_id sets
at each stage, not by eyeballing a plot).

## What's here

The checks themselves live inside the `rafts_algo` package (so they're
importable, testable, and installed the same way as every other flow
script), while this directory holds the standalone driver and docs:

- **`pkg/rafts_algo/rafts_algo/qa_utils.py`** -- shared config resolution.
  Every check takes a single `--path_pred_config` and derives
  `path_crosswalk_ids`, the compiled regionalization output directory, and
  the per-dataset analysis output directory from it, using the exact same
  logic `rafts_regn_params_gpkg.py`, `rafts_map_pred_hfatl.py`, and
  `raftsutil.std_corr_path`/`std_test_pred_obs_path` use in production (see
  `resolve_qa_context`). Keep this in sync if those derivations ever change.
  Unit tested in `pkg/rafts_algo/rafts_algo/tests/test_qa_utils.py`.
- **`pkg/rafts_algo/rafts_algo/flow/qa/qa_check_attrs_crosswalk_coverage.py`**
  -- do the raw hfATLAS divide-based attribute files have any divide_id the
  crosswalk doesn't (or vice versa)?
- **`pkg/rafts_algo/rafts_algo/flow/qa/qa_check_sqlite_crosswalk_coverage.py`**
  -- does every crosswalk divide_id actually make it into the compiled
  `compiled_regionalized_params_*.sqlite` tables `rafts_regn_params_gpkg.py`
  writes? Checked per-table as well as in aggregate, so a gap specific to
  one formulation/algo combination doesn't get masked by another table that
  happens to cover the same divides.
- **`scripts/qa/run_qa_checks.sh`** -- runs both checks against one
  pred_config.

## Running

Standalone, against any pred_config:

```bash
cd pkg
uv run python rafts_algo/flow/qa/qa_check_attrs_crosswalk_coverage.py \
    --path_pred_config ../scripts/workflow_configs/<workflow>/<model>_pred_config.yaml
```

Or both at once:

```bash
scripts/qa/run_qa_checks.sh scripts/workflow_configs/<workflow>/<model>_pred_config.yaml
```

From a workflow's own processing script (see `regn_all_proc.sh`'s
`OPTIONAL QA CHECKS` block near the end for the exact pattern): call
`run_qa_checks.sh` once per workflow run, pointing at *any single* model's
pred_config within that workflow -- `qa_check_sqlite_crosswalk_coverage.py`
in particular checks the whole workflow's compiled output directory (shared
across every formulation), not just the one model named in the config, so
looping it over every model would just repeat the same check.

## Related: crosswalk_gap_diagnosis/

`scripts/qa/crosswalk_gap_diagnosis/` is a sibling directory following this
same driver-script-plus-docs pattern, for a deeper, spatially-aware
investigation of *why* specific HUC12s are missing from the crosswalk (not
just *that* they are) -- see its own README for the strategy and findings.

## Adding a new check

- Add it to `pkg/rafts_algo/rafts_algo/flow/qa/`, alongside the existing
  checks -- not to `scripts/qa/`, which now holds only the driver script and
  this doc.
- Take `--path_pred_config` as the only required argument; derive
  everything else from `rafts_algo.qa_utils.resolve_qa_context` where
  possible, with an explicit override flag (e.g. `--dir_attrs`) for anything
  that isn't part of the standard pred_config/attr_config schema.
- `[SKIP]`, don't error, when an optional input (a config key, a directory)
  isn't present -- these checks need to degrade gracefully across workflows
  that don't populate every field.
- Always `sys.exit(0)`. If a machine-readable pass/fail signal is ever
  needed, add it as a separate flag or a written report file, not the
  process exit code -- these checks were designed to feed a human reading
  terminal output, not to gate CI.
- Write any output list to
  `qa_out_<check>_<pred_config_stem>.csv` under `ctx.dir_qa_out`
  (`dir_out/analysis/<dataset_name>/` -- the same directory
  `raftsutil.std_corr_path` and `raftsutil.std_test_pred_obs_path` already
  write into) rather than inventing a new location, so results land next to
  a dataset's other analysis output and the filename keeps them from
  colliding across models/workflows.
- Add it to `scripts/qa/run_qa_checks.sh` so it runs as part of the
  standard sweep.
