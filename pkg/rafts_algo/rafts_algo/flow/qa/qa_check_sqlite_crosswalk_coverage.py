"""qa_check_sqlite_crosswalk_coverage.py

QA check: which divide_ids are present in the huc12 crosswalk but missing
from the compiled regionalized-parameter SQLite tables written by
rafts_regn_params_gpkg.py (raftsutil.update_database())? And the reverse --
any divide_id in a SQLite table that isn't in the crosswalk at all?

Each compiled_regionalized_params_*.sqlite file can hold one table per
formulation/algo combination (table names look like
"{formulation}__v___{dataset}_{algo}"), so this scans every table in every
matching .sqlite file rather than assuming a single table per file.

Path resolution:
    --path_pred_config is the only required input. Both path_crosswalk_ids
    and the compiled-SQLite output directory
    (dir_out/regionalization/<pred_config's parent dir name>/) are derived
    from it using the exact same logic rafts_regn_params_gpkg.py uses to
    decide where to write those files -- see rafts_algo.qa_utils.resolve_qa_context.
    --dir_sqlite overrides the derived directory if needed.

    Since rafts_regn_params_gpkg.py writes one compiled *.sqlite per algo
    into a directory shared by every formulation in a given workflow (the
    directory name comes from the pred_config's own parent directory, not
    the dataset), running this check against any single formulation's
    pred_config within a workflow is enough to cover that whole workflow's
    compiled output -- it does not need to be run once per formulation.

    --dir_out (if any divide_ids are missing) defaults to the standard
    dir_out/analysis/<dataset_name>/ directory raftsutil.std_corr_path and
    raftsutil.std_test_pred_obs_path also write into.

Exit code is always 0 (see scripts/qa/README.md): this is a diagnostic, not
a pass/fail gate, so it never aborts a calling shell pipeline.

Example:
    >>> cd /path/to/rafts/pkg
    >>> uv run python rafts_algo/flow/qa/qa_check_sqlite_crosswalk_coverage.py \\
    ...     --path_pred_config ../scripts/workflow_configs/regn_fy26_hfv4_conus_clst/regn_casam_pred_config_hf4.yaml
"""
import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd
from rafts_algo.qa_utils import resolve_qa_context


def get_table_divide_ids(path_sqlite: Path) -> dict:
    """Read the unique divide_id set from every table in a SQLite file.

    :param path_sqlite: Path to a compiled_regionalized_params_*.sqlite file.
    :type path_sqlite: Path
    :return: Mapping of table_name -> set of divide_id values.
    :rtype: dict
    """
    result = {}
    with sqlite3.connect(path_sqlite) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            cur.execute(f'PRAGMA table_info("{table}")')
            cols = [c[1] for c in cur.fetchall()]
            if "divide_id" not in cols:
                print(f"  [skip] {table}: no divide_id column ({cols})")
                continue
            df = pd.read_sql(f'SELECT DISTINCT divide_id FROM "{table}"', conn)
            result[table] = set(df["divide_id"].astype(str))
    return result


def run(path_pred_config: Path, dir_sqlite: Path = None, dir_out: Path = None) -> int:
    """Run the compiled-SQLite-vs-crosswalk coverage check.

    :return: Count of crosswalk divide_ids missing from every SQLite table
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

    dir_sqlite = Path(dir_sqlite).expanduser() if dir_sqlite else ctx.dir_regionalization
    sqlite_files = sorted(dir_sqlite.glob("*.sqlite"))
    if not sqlite_files:
        print(f"[SKIP] No .sqlite files found in {dir_sqlite} "
              f"(pass --dir_sqlite to override, or run rafts_regn_params_gpkg.py first).")
        return 0

    print(f"path_pred_config: {path_pred_config}")
    print(f"path_crosswalk_ids: {ctx.path_crosswalk_ids}")
    print(f"dir_sqlite: {dir_sqlite}")

    df_crosswalk = pd.read_parquet(ctx.path_crosswalk_ids, columns=["divide_id"])
    crosswalk_ids = set(df_crosswalk["divide_id"].astype(str))
    print(f"Crosswalk ({ctx.path_crosswalk_ids.name}): {len(crosswalk_ids):,} unique divide_id\n")

    print(f"Found {len(sqlite_files)} SQLite file(s):")
    all_table_ids = {}
    for f in sqlite_files:
        print(f"{f.name}:")
        tables = get_table_divide_ids(f)
        for table, ids in tables.items():
            print(f"  {table}: {len(ids):,} unique divide_id")
        all_table_ids.update({f"{f.stem}::{t}": ids for t, ids in tables.items()})

    if not all_table_ids:
        print("[SKIP] No tables with a divide_id column found in any SQLite file.")
        return 0

    union_ids = set().union(*all_table_ids.values())
    print(f"\nUnion across all tables: {len(union_ids):,} unique divide_id")

    not_regionalized = sorted(crosswalk_ids - union_ids)
    pct = 100 * len(not_regionalized) / max(len(crosswalk_ids), 1)
    print(f"\nCrosswalk divides missing from every SQLite table: {len(not_regionalized):,} "
          f"of {len(crosswalk_ids):,} ({pct:.3f}%)")
    if not_regionalized:
        print(f"  sample: {not_regionalized[:10]}")
        if dir_out is None:
            dir_out = ctx.dir_qa_out
        dir_out = Path(dir_out)
        dir_out.mkdir(parents=True, exist_ok=True)
        out_path = dir_out / f"qa_out_sqlite_missing_from_crosswalk_{path_pred_config.stem}.csv"
        pd.DataFrame({"divide_id": not_regionalized}).to_csv(out_path, index=False)
        print(f"  wrote full list to {out_path}")

    print("\nPer-table gap vs. crosswalk:")
    any_per_table_gap = False
    for name, ids in all_table_ids.items():
        gap = crosswalk_ids - ids
        extra = ids - crosswalk_ids
        if gap or extra:
            any_per_table_gap = True
        print(f"  {name}: {len(gap):,} crosswalk divides missing, {len(extra):,} extra divide_ids not in crosswalk")
    if not any_per_table_gap:
        print("  None -- every table's divide_id set exactly matches the crosswalk.")

    return len(not_regionalized)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path_pred_config", type=Path, required=True,
                         help="Path to a prediction config YAML (path_crosswalk_ids and the compiled "
                              "SQLite output directory are both derived from it).")
    parser.add_argument("--dir_sqlite", type=Path, default=None,
                         help="Directory containing compiled_regionalized_params_*.sqlite files. "
                              "Defaults to dir_out/regionalization/<pred_config's parent dir name>/.")
    parser.add_argument("--dir_out", type=Path, default=None,
                         help="Where to write the CSV of missing divide_ids, if any. "
                              "Defaults to dir_out/analysis/<dataset_name>/.")
    args = parser.parse_args()
    n_missing = run(args.path_pred_config.expanduser(), args.dir_sqlite, args.dir_out)
    sys.exit(0)  # diagnostic only -- see scripts/qa/README.md
