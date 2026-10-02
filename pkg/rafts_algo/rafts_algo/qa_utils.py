"""qa_utils.py

Shared config-resolution helpers for the post-hoc QA checks in
rafts_algo/flow/qa/ (see scripts/qa/README.md and scripts/qa/run_qa_checks.sh
for how those checks are run against a workflow).

Every QA check starts from a single --path_pred_config argument (the same
YAML any rafts_algo/flow/*.py script takes) and derives whatever paths it
needs from there, rather than hardcoding absolute paths -- so a check written
against one regionalization workflow (e.g.
scripts/workflow_configs/regn_fy26_hfv4_conus_clst/) works unmodified against
any other, as long as it follows the same pred_config/attr_config conventions.

The path derivations here intentionally mirror the production flow scripts
exactly (rafts_regn_params_gpkg.py for dir_regionalization / path_crosswalk_ids,
rafts_map_pred_hfatl.py for the same crosswalk resolution) -- if those change,
update both sides together, or these checks will silently look in the wrong
place.
"""
from pathlib import Path
from typing import NamedTuple, Optional

import geopandas as gpd
import pandas as pd
import rafts_algo.utils as raftsutil


class QaConfigContext(NamedTuple):
    """Resolved config context for a single pred_config, enough to locate
    the crosswalk file, the compiled regionalization output directory, and
    the raw hfATLAS data root QA checks need.

    :param pred_cfg: The parsed prediction config.
    :type pred_cfg: raftsutil.PredConfigParser
    :param path_crosswalk_ids: Resolved path to the huc12<->divide_id crosswalk
        file declared in the pred_config's `path_crosswalk_ids`, or None if
        that key isn't set.
    :type path_crosswalk_ids: Optional[Path]
    :param dir_regionalization: The same
        `dir_out/regionalization/<pred_config's parent dir name>` directory
        rafts_regn_params_gpkg.py writes its compiled_regionalized_params_*.sqlite
        files and master GPKG copy into.
    :type dir_regionalization: Path
    :param dir_raw_root: The parent of path_crosswalk_ids's directory (e.g.
        crosswalk/ -> its parent), used as the default base for locating
        sibling raw-data directories like attrs_divs/. None if
        path_crosswalk_ids is unset.
    :type dir_raw_root: Optional[Path]
    :param dir_qa_out: The standard per-dataset analysis output directory
        (`dir_out/analysis/<dataset_name>/`), the same base
        `raftsutil.std_corr_path` and `raftsutil.std_test_pred_obs_path` write
        into -- the default location for `qa_out_*.csv` files.
    :type dir_qa_out: Path
    :param dir_out_viz_base: The standard `dir_out/data_visualizations/` base
        directory `rafts_algo.plots.gen_conus_basemap` caches its downloaded
        CONUS state-boundary shapefile into -- pass this as a QA plot's
        `dir_out_basemap` so it reuses that cache instead of re-downloading
        a second copy into the QA output directory.
    :type dir_out_viz_base: Path
    """
    pred_cfg: "raftsutil.PredConfigParser"
    path_crosswalk_ids: Optional[Path]
    dir_regionalization: Path
    dir_raw_root: Optional[Path]
    dir_qa_out: Path
    dir_out_viz_base: Path


def resolve_qa_context(path_pred_config: Path) -> QaConfigContext:
    """Load a pred_config (and its linked attr_config) and resolve the paths
    QA checks need, using the same logic rafts_regn_params_gpkg.py and
    rafts_map_pred_hfatl.py use in production.

    :param path_pred_config: Path to a prediction config YAML.
    :type path_pred_config: Path
    :return: The resolved context.
    :rtype: QaConfigContext
    """
    path_pred_config = Path(path_pred_config).expanduser().resolve()

    pred_cfg = raftsutil.PredConfigParser(path_pred_config)
    pred_cfg._read_pred_config()

    path_attr_config = raftsutil.build_cfig_path(
        path_pred_config, pred_cfg.pred_cfg_dict.get('name_attr_config')
    )
    attr_cfig = raftsutil.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    context = {
        'dir_base': str(attr_cfig.attrs_cfg_dict.get('dir_base')),
        'dir_std_base': str(attr_cfig.attrs_cfg_dict.get('dir_std_base')),
        'dir_db_attrs': str(attr_cfig.attrs_cfg_dict.get('dir_db_attrs')),
        'home_dir': str(attr_cfig.attrs_cfg_dict.get('home_dir')),
    }

    path_crosswalk_ids_raw = pred_cfg.pred_cfg_dict.get('path_crosswalk_ids')
    path_crosswalk_ids = None
    dir_raw_root = None
    if path_crosswalk_ids_raw:
        path_crosswalk_ids = Path(raftsutil.resolve_fstrings(path_crosswalk_ids_raw, context))
        # e.g. .../raw/fy26_regn_hfv4/crosswalk/foo.parquet -> .../raw/fy26_regn_hfv4
        dir_raw_root = path_crosswalk_ids.parent.parent

    # Exactly rafts_regn_params_gpkg.py's dir_regionalization_sub derivation.
    dirs_std_dict = raftsutil.rafts_save_algo_dir_struct(attr_cfig.attrs_cfg_dict.get('dir_base'))
    dir_out = Path(dirs_std_dict.get('dir_out'))
    dir_regionalization = dir_out / "regionalization" / path_pred_config.parent.name

    # Same dir_out/analysis/<dataset_name>/ base raftsutil.std_corr_path and
    # raftsutil.std_test_pred_obs_path write into -- reuse it for QA output
    # rather than inventing a new location. `datasets` is normally a single
    # dataset_name for a given pred_config; fall back to the pred_config's
    # own parent dir name if it can't be resolved.
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') or []
    dataset_name = datasets[0] if datasets else path_pred_config.parent.name
    dir_qa_out = Path(dirs_std_dict.get('dir_out_anlys_base')) / dataset_name

    return QaConfigContext(
        pred_cfg=pred_cfg,
        path_crosswalk_ids=path_crosswalk_ids,
        dir_regionalization=dir_regionalization,
        dir_raw_root=dir_raw_root,
        dir_qa_out=dir_qa_out,
        dir_out_viz_base=Path(dirs_std_dict.get('dir_out_viz_base')),
    )


def resolve_huc12_layer(pred_cfg_dict: dict, context: dict, id_zfill_width: int = None) -> gpd.GeoDataFrame:
    """Read the WBD-style aggregation-unit layer a workflow's pred_config points to for
    receiver geometries (the same layer rafts_map_donor_receiver.py and
    rafts_map_pred_hfatl.py use). Despite the name (kept for continuity with this
    diagnosis's HUC12 origin -- see scripts/qa/crosswalk_gap_diagnosis/), this does not
    assume the identifier column is literally named 'huc12': it keeps whatever
    `pred_gpkg_id_col` is configured to be, so the same crosswalk-gap diagnosis works
    against any WBD-style aggregation unit (HUC10, HUC14, a custom basin id, etc.), not
    just HUC12.

    :param pred_cfg_dict: ``PredConfigParser.pred_cfg_dict`` for the prediction config.
    :type pred_cfg_dict: dict
    :param context: f-string resolution context, e.g. ``{'dir_std_base':..., 'home_dir':...}``.
    :type context: dict
    :param id_zfill_width: If given, zero-pads the id column (as a string) to this width --
        e.g. 12 for standard 12-digit HUC12 codes, where a plain `str()` cast can otherwise
        drop a leading zero that got lost to an int/float dtype on read. Defaults to None
        (no padding), since not every aggregation unit's id is a fixed-width zero-padded code.
    :type id_zfill_width: int, optional
    :return: Polygons with the configured `pred_gpkg_id_col`-named id column (cast to str,
        zero-padded if `id_zfill_width` is given) and a 'states' column.
    :rtype: gpd.GeoDataFrame
    :raises FileNotFoundError: If ``path_gpkg_pred`` is unset or doesn't resolve to a real file.
    :raises KeyError: If the layer lacks the configured id column or a 'states' column.
    """
    path_gpkg_pred = pred_cfg_dict.get('path_gpkg_pred')
    pred_gpkg_lyr = pred_cfg_dict.get('pred_gpkg_lyr')
    pred_gpkg_id_col = pred_cfg_dict.get('pred_gpkg_id_col')
    if not (path_gpkg_pred and pred_gpkg_lyr and pred_gpkg_id_col):
        raise FileNotFoundError("path_gpkg_pred/pred_gpkg_lyr/pred_gpkg_id_col not all set in pred_config.")

    path_gpkg_pred_rslv = Path(str(path_gpkg_pred).format(**context))
    if not path_gpkg_pred_rslv.exists():
        raise FileNotFoundError(f"Prediction-locations GPKG not found: {path_gpkg_pred_rslv}")

    gdf = gpd.read_file(path_gpkg_pred_rslv, layer=pred_gpkg_lyr, engine='pyogrio')
    if pred_gpkg_id_col not in gdf.columns:
        raise KeyError(f"'{pred_gpkg_id_col}' not found in {pred_gpkg_lyr} layer of {path_gpkg_pred_rslv}.")
    if 'states' not in gdf.columns:
        raise KeyError(f"'states' column not found in {pred_gpkg_lyr} layer of {path_gpkg_pred_rslv} "
                        f"(expected for a USGS WBD-style aggregation-unit layer).")

    gdf[pred_gpkg_id_col] = gdf[pred_gpkg_id_col].astype(str)
    if id_zfill_width:
        gdf[pred_gpkg_id_col] = gdf[pred_gpkg_id_col].str.zfill(id_zfill_width)
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    return gdf


def resolve_divides_layer(pred_cfg_dict: dict, context: dict, divide_id_col: str) -> gpd.GeoDataFrame:
    """Read the hydrofabric divides layer a workflow's crosswalk targets
    (``path_hf_finl_gpkg`` / ``layr_hf_finl_gpkg``).

    :param pred_cfg_dict: ``PredConfigParser.pred_cfg_dict`` for the prediction config.
    :type pred_cfg_dict: dict
    :param context: f-string resolution context, e.g. ``{'dir_std_base':..., 'home_dir':...}``.
    :type context: dict
    :param divide_id_col: Column name holding the divide identifier (e.g. 'divide_id').
    :type divide_id_col: str
    :return: Divide polygons with `divide_id_col` and geometry only (attribute columns dropped for speed).
    :rtype: gpd.GeoDataFrame
    :raises FileNotFoundError: If ``path_hf_finl_gpkg`` is unset or doesn't resolve to a real file.
    """
    path_hf_finl_gpkg_raw = pred_cfg_dict.get('path_hf_finl_gpkg')
    if not path_hf_finl_gpkg_raw:
        raise FileNotFoundError("path_hf_finl_gpkg not set in pred_config.")
    path_hf_finl_gpkg = Path(raftsutil.resolve_fstrings(path_hf_finl_gpkg_raw, context))
    if not path_hf_finl_gpkg.exists():
        raise FileNotFoundError(f"Master hydrofabric GPKG not found: {path_hf_finl_gpkg}")

    layer = pred_cfg_dict.get('layr_hf_finl_gpkg') or 'divides'
    gdf = gpd.read_file(path_hf_finl_gpkg, layer=layer, columns=[divide_id_col, 'geometry'], engine='pyogrio')
    gdf[divide_id_col] = gdf[divide_id_col].astype(str)
    return gdf


def find_huc12_divide_overlaps(gdf_huc12: gpd.GeoDataFrame, gdf_divides: gpd.GeoDataFrame,
                                divide_id_col: str, min_frac: float = 0.001,
                                huc12_col: str = 'huc12') -> pd.DataFrame:
    """Find every (huc12, divide) pair that really overlaps, and by how much.

    Uses an intersects-predicate spatial join to find candidate pairs cheaply via
    spatial indexing, then computes exact intersection area only for those
    candidates (not a full cross product). Overlap is expressed as a fraction of
    *the HUC12's own area* -- NOT the divide's -- because a single hydrofabric
    divide can legitimately span many HUC12s (hfv4's large non-contributing
    "landscape"-type divides, e.g. in the Everglades, are a real example: they
    are far bigger than any one HUC12). Normalizing by the divide's footprint
    would make a HUC12 fully covered by such a divide register as a negligible
    sliver of that divide's total area, and get wrongly discarded.

    :param gdf_huc12: HUC12 polygons, with a `huc12_col` identifier column.
    :type gdf_huc12: gpd.GeoDataFrame
    :param gdf_divides: Hydrofabric divide polygons, with `divide_id_col`.
    :type gdf_divides: gpd.GeoDataFrame
    :param divide_id_col: Column name holding the divide identifier in `gdf_divides`.
    :type divide_id_col: str
    :param min_frac: Minimum fraction of a HUC12's own area a divide must cover to
        be kept -- filters out boundary-vertex/digitization-noise slivers, defaults to 0.001
    :type min_frac: float, optional
    :param huc12_col: Column name holding the HUC12 identifier in `gdf_huc12`, defaults to 'huc12'
    :type huc12_col: str, optional
    :return: One row per real (huc12, divide) overlap: `huc12_col`, `divide_id_col`, 'frac_of_huc'.
    :rtype: pd.DataFrame
    """
    # Equal-area CRS for CONUS so area fractions are meaningful (both inputs may
    # arrive in EPSG:4326, where "area" is not remotely comparable across latitudes).
    equal_area_crs = 'EPSG:5070'
    huc_ea = gdf_huc12[[huc12_col, 'geometry']].to_crs(equal_area_crs)
    div_ea = gdf_divides[[divide_id_col, 'geometry']].to_crs(equal_area_crs)
    huc_ea['_huc_area'] = huc_ea.geometry.area

    candidates = gpd.sjoin(huc_ea, div_ea, how='inner', predicate='intersects')
    if candidates.empty:
        return pd.DataFrame(columns=[huc12_col, divide_id_col, 'frac_of_huc'])

    huc_geom_by_id = huc_ea.set_index(huc12_col).geometry
    div_geom_by_id = div_ea.set_index(divide_id_col).geometry
    huc_area_by_id = huc_ea.set_index(huc12_col)['_huc_area']

    rows = []
    for row in candidates.itertuples(index=False):
        huc12 = getattr(row, huc12_col)
        divide_id = getattr(row, divide_id_col)
        huc_area = huc_area_by_id[huc12]
        if huc_area <= 0:
            continue
        inter_area = huc_geom_by_id[huc12].intersection(div_geom_by_id[divide_id]).area
        frac_of_huc = inter_area / huc_area
        if frac_of_huc >= min_frac:
            rows.append((huc12, divide_id, frac_of_huc))

    return pd.DataFrame(rows, columns=[huc12_col, divide_id_col, 'frac_of_huc'])
