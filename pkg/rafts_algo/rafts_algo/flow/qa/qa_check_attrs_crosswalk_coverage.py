"""qa_check_attrs_crosswalk_coverage.py

QA check: which divide_ids appear in the hfATLAS divide-based attribute
files but have no entry at all in the huc12 crosswalk (and vice versa)?

A divide present in the attribute data but absent from the crosswalk can
never receive a huc12-level prediction when rafts_map_pred_hfatl.py broadcasts
predictions down to the divide scale (see the divide-level "no crosswalk
data" gap handling in rafts_algo/plots.py / rafts_map_pred_hfatl.py). This
check identifies exactly which divide_ids those are, sourced from the raw
attribute files rather than the master hydrofabric GPKG.

hfATLAS writes these parquet files with dequantified pint-aware tuple column
headers (e.g. "('divide_id', 'No Unit')") -- per CLAUDE.md, always resolve
those through raftsutil.read_hfatlas_wrap_dask rather than hand-parsing the
tuple strings.

Path resolution:
    --path_pred_config is the only required input. path_crosswalk_ids is
    read from that config exactly as rafts_map_pred_hfatl.py /
    rafts_regn_params_gpkg.py do. --dir_attrs defaults to a sibling
    "attrs_divs/" directory next to the crosswalk file's own directory
    (e.g. .../raw/<workflow>/crosswalk/x.parquet -> .../raw/<workflow>/attrs_divs/)
    -- override it explicitly if a given workflow doesn't follow that layout.
    --path_out_csv (if any divide_ids are missing) defaults to the standard
    dir_out/analysis/<dataset_name>/ directory raftsutil.std_corr_path and
    raftsutil.std_test_pred_obs_path also write into.

Exit code is always 0 (see scripts/qa/README.md): this is a diagnostic, not
a pass/fail gate, so it never aborts a calling shell pipeline.

Example:
    >>> cd /path/to/rafts/pkg
    >>> uv run python rafts_algo/flow/qa/qa_check_attrs_crosswalk_coverage.py \\
    ...     --path_pred_config ../scripts/workflow_configs/regn_fy26_hfv4_conus_clst/regn_casam_pred_config_hf4.yaml
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
import rafts_algo.utils as raftsutil
from rafts_algo.qa_utils import resolve_qa_context


def get_attrs_divide_ids(path_parquet: Path) -> set:
    """Extract the unique set of divide_ids from an hfATLAS wide-format parquet file.

    :param path_parquet: Path to an hfATLAS attribute/parameter parquet file
        (pint-aware dequantified tuple column headers).
    :type path_parquet: Path
    :return: Unique divide_id values present in the file.
    :rtype: set
    """
    # attrs_sel=[] forces read_hfatlas_wrap_dask to only pull map_id_col,
    # skipping the (potentially large) attribute columns entirely.
    df = raftsutil.read_hfatlas_wrap_dask(
        paths_hfatl=[path_parquet], attrs_sel=[], map_id_col="divide_id"
    )
    return set(df["divide_id"].astype(str))


def run(path_pred_config: Path, dir_attrs: Path = None, path_out_csv: Path = None) -> int:
    """Run the attrs-vs-crosswalk coverage check.

    :return: Count of divide_ids with attribute data but no crosswalk entry
        (0 means fully covered).
    :rtype: int
    """
    ctx = resolve_qa_context(path_pred_config)
    if ctx.path_crosswalk_ids is None:
        print(f"[SKIP] {path_pred_config.name} has no 'path_crosswalk_ids' set -- nothing to check.")
        return 0
    if not ctx.path_crosswalk_ids.exists():
        print(f"[SKIP] Crosswalk file does not exist: {ctx.path_crosswalk_ids}")
        return 0

    if dir_attrs is None:
        if ctx.dir_raw_root is None:
            print("[SKIP] Could not derive a raw-data root to guess --dir_attrs from; pass it explicitly.")
            return 0
        dir_attrs = ctx.dir_raw_root / "attrs_divs"
    dir_attrs = Path(dir_attrs).expanduser()

    attrs_files = sorted(dir_attrs.glob("*.parquet"))
    if not attrs_files:
        print(f"[SKIP] No .parquet files found in {dir_attrs} (pass --dir_attrs to override).")
        return 0

    print(f"path_pred_config: {path_pred_config}")
    print(f"path_crosswalk_ids: {ctx.path_crosswalk_ids}")
    print(f"dir_attrs: {dir_attrs}")
    print(f"Found {len(attrs_files)} attribute file(s):")

    attrs_divide_ids = set()
    for f in attrs_files:
        ids = get_attrs_divide_ids(f)
        print(f"  {f.name}: {len(ids):,} unique divide_id")
        attrs_divide_ids |= ids
    print(f"Union across all attribute files: {len(attrs_divide_ids):,} unique divide_id\n")

    df_crosswalk = pd.read_parquet(ctx.path_crosswalk_ids, columns=["divide_id"])
    crosswalk_divide_ids = set(df_crosswalk["divide_id"].astype(str))
    print(f"Crosswalk ({ctx.path_crosswalk_ids.name}): {len(crosswalk_divide_ids):,} unique divide_id\n")

    missing = sorted(attrs_divide_ids - crosswalk_divide_ids)
    pct = 100 * len(missing) / max(len(attrs_divide_ids), 1)
    print(f"Divides with attribute data but NO crosswalk entry: {len(missing):,} "
          f"of {len(attrs_divide_ids):,} ({pct:.3f}%)")

    if missing:
        print(f"Sample missing divide_ids: {missing[:10]}")
        if path_out_csv is None:
            path_out_csv = ctx.dir_qa_out / f"qa_out_attrs_missing_from_crosswalk_{path_pred_config.stem}.csv"
        path_out_csv = Path(path_out_csv)
        path_out_csv.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"divide_id": missing}).to_csv(path_out_csv, index=False)
        print(f"Wrote full list to {path_out_csv}")
    else:
        print("None -- every divide_id with attribute data also has a crosswalk entry.")

    no_attrs = crosswalk_divide_ids - attrs_divide_ids
    print(f"\n(For reference) Crosswalk divides with NO attribute data: {len(no_attrs):,} "
          f"of {len(crosswalk_divide_ids):,}")

    return len(missing)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path_pred_config", type=Path, required=True,
                         help="Path to a prediction config YAML (path_crosswalk_ids is read from it).")
    parser.add_argument("--dir_attrs", type=Path, default=None,
                         help="Directory of hfATLAS divide-based attribute .parquet files. "
                              "Defaults to a sibling 'attrs_divs/' next to the crosswalk file's directory.")
    parser.add_argument("--path_out_csv", type=Path, default=None,
                         help="Where to write the list of missing divide_ids, if any. "
                              "Defaults to dir_out/analysis/<dataset_name>/qa_out_attrs_missing_from_crosswalk_<pred_config_stem>.csv.")
    args = parser.parse_args()
    n_missing = run(args.path_pred_config.expanduser(), args.dir_attrs, args.path_out_csv)
    sys.exit(0)  # diagnostic only -- see scripts/qa/README.md
