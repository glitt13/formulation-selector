# scripts/qa/crosswalk_gap_diagnosis/ -- root-cause investigation for "missing" HUC12s

A deeper, spatially-aware follow-up to the coverage checks in
`scripts/qa/` (see that directory's own README): those checks report *that*
a divide_id or HUC12 is missing from some downstream table; this directory
investigates *why*, specifically for HUC12s absent from the huc12<->divide_id
crosswalk (`path_crosswalk_ids`) -- the gap that showed up as hatched
"No donor pairing" regions on `rafts_map_donor_receiver.py`'s Florida map.

Like `scripts/qa/`, the check *logic* lives in the `rafts_algo` package
(`pkg/rafts_algo/rafts_algo/flow/qa/` and `qa_utils.py`) so it's importable
and testable; this directory holds only the standalone driver and this doc.

Despite the HUC12-flavored naming (kept for continuity with this diagnosis's
origin), none of the scripts assume the aggregation-unit id column is
literally named `'huc12'` -- they read whatever the workflow's own
`pred_gpkg_id_col` is configured to be (per CLAUDE.md's "don't hardcode
identifier columns" rule) and thread that through. `--id_zfill_width`
(default 12, pass 0 to disable) controls zero-padding for fixed-width codes
like HUC12 -- override it for a different aggregation unit (e.g. HUC10) or
drop it if the id isn't a zero-padded fixed-width code at all.

## The strategy

1. **Does a hydrofabric divide actually exist inside the missing HUC12?**
   (`qa_check_missing_huc12_divide_existence.py`) Splits "missing" HUC12s
   into `no_divide_overlap` (probably a structural gap -- open water, an
   island, a closed basin with no flowline-connected divide) vs.
   `divides_exist_not_crosswalked` (a real, actionable data gap).
2. **For the actionable ones, where did the overlapping divide actually get
   crosswalked to instead?** (`qa_check_huc12_divide_reassignment.py`) The
   crosswalk is strictly 1:1, so every such divide was either never
   crosswalked at all, or assigned to some *other* HUC12. This distinguishes
   an assignment that's explainable by a "majority share of the divide" rule
   (not a defect -- see the large-divide note below) from one that isn't
   (the strongest available evidence of an actual crosswalk-build defect).
3. **Size the problem CONUS-wide**, not just in one region, by running check
   1 with no `--states` filter -- the same script handles both scopes.

## A correctness pitfall this diagnosis exists to avoid

Both checks measure divide/HUC12 overlap as a fraction of **the HUC12's own
area**, never the divide's. hfv4 has divides much larger than a single
HUC12 -- e.g. a ~4,089 sqkm non-contributing "landscape"-type divide near
the Everglades, 42x Florida's median HUC12 area, that fully covers several
HUC12s at once. An earlier version of this diagnosis normalized by the
*divide's* area instead, which made a HUC12 completely covered by such a
divide register as a negligible sliver of that divide's total footprint and
get wrongly bucketed as "no overlap." Confirmed empirically on the first
CONUS-wide run with each version: normalizing by divide area undercounted
the actionable bucket; normalizing by HUC12 area, plus the majority-share
comparison in check 2, gives a result consistent with the underlying
geometry. If you extend either script, keep the denominator on the HUC12's
side -- see the docstrings in both scripts for the full reasoning, including
a second empirical example (a divide correctly assigned to the HUC12 that
holds 80.9% of it, not the HUC12 whose own area it happens to fill 99.5% of).

## Running

Against one region (defaults to Florida if you don't pass `--states`):

```bash
scripts/qa/crosswalk_gap_diagnosis/run_crosswalk_gap_diagnosis.sh \
    scripts/workflow_configs/<workflow>/<model>_pred_config.yaml
```

Against a different region:

```bash
scripts/qa/crosswalk_gap_diagnosis/run_crosswalk_gap_diagnosis.sh \
    scripts/workflow_configs/<workflow>/<model>_pred_config.yaml --states WA OR
```

Or run a single check standalone:

```bash
cd pkg
uv run python rafts_algo/rafts_algo/flow/qa/qa_check_missing_huc12_divide_existence.py \
    --path_pred_config ../scripts/workflow_configs/<workflow>/<model>_pred_config.yaml \
    --states FL
```

Each check writes a CSV to the standard `dir_out/analysis/<dataset_name>/`
directory (the same base `raftsutil.std_corr_path` and the `scripts/qa/`
checks write into) -- see each script's `--help` / docstring for the exact
filename pattern and every column's meaning. Exit code is always 0 (these
are diagnostics, not a pass/fail gate -- see `scripts/qa/README.md`).

## Plots (for human inspection, alongside each CSV)

Numbers alone are easy to over-trust; both checks also render a map into the
same `dir_out/analysis/<dataset_name>/` directory as their CSV, so a person
can visually sanity-check the finding against the real geometry before
acting on it:

- `qa_huc12_gap_classification_map_<region>.png`
  (`qa_check_missing_huc12_divide_existence.py`) -- every HUC12 in scope,
  with `divides_exist_not_crosswalked` filled red and `no_divide_overlap`
  hatched blue over a light-gray "has crosswalk entry" background. At CONUS
  scale this is also what caught a real bug while building this diagnosis:
  HUC2 region 22 (Pacific Islands/Oceania) HUC12s had a blank `states` field
  in this WBD layer, so the original `states`-text exclusion silently let
  them through and stretched the map's extent out to the Pacific. Fixed by
  scoping the CONUS default on the HUC12 id's own HUC2 prefix
  (`is_conus_huc12()` in that script) instead of trusting the `states`
  field alone -- worth knowing if you extend the region-scoping logic.
- `qa_huc12_divide_reassignment_defects_<region>.png`
  (`qa_check_huc12_divide_reassignment.py`) -- a small-multiples grid of the
  top N (default 10, `--top_n_plot`) `assigned_elsewhere_minority_share_of_divide`
  rows, one zoomed-in panel per row: the divide's real footprint (blue fill)
  against the "missing" HUC12's boundary (red dashed) and the HUC12 the
  crosswalk actually assigned it to (green solid). Only this bucket is
  plotted -- `majority_share` rows are already explained by the large-divide
  limitation above and aren't defects, and `divide_itself_not_in_crosswalk`
  rows have no assigned HUC12 to compare against; both stay fully in the CSV.

Both plotting calls are wrapped in `try/except` in their respective scripts
-- a rendering failure never turns an otherwise-successful diagnostic run
(CSV already written) into one that looks like it failed.

## Downstream consumer: rafts_map_donor_receiver.py

This diagnosis isn't only for QA reading -- `rafts_map_donor_receiver.py`
(the production donor-receiver pairing map, not a QA script) reuses the same
`find_huc12_divide_overlaps`/`resolve_divides_layer` machinery to *fix* the
gap it causes, not just report it. A HUC12 with no crosswalk entry gets no
prediction and so shows up hatched "No donor pairing" on that map; where the
cause is a single divide bigger than the HUC12 (the same condition this
diagnosis flags), that script substitutes the divide's own geometry as a
proxy receiver, borrowing the pairing from whichever HUC12 the crosswalk
actually assigned the divide to (`build_divide_proxy_receivers()` in that
script). On casam/hfv4, this resolved 93 of Florida's 121 hatched gaps and
126 of the Pacific Northwest's 153 -- both numbers matching this diagnosis's
own `divides_exist_not_crosswalked` counts for those regions.

One layout pitfall surfaced while building this: a proxy divide only needs a
small corner inside the mapped region to be selected, but its full footprint
can extend far outside that region (a Great-Basin-scale divide clipped a
WA/OR map's bounding box hundreds of miles into California on the first
attempt). The map's axis bounds are computed from the ordinary HUC12-shaped
receivers only, excluding proxy geometries -- they still render in full,
just naturally clipped by the view instead of distorting it.

## Findings so far (casam, hfv4, regn_fy26_hfv4_conus_clst)

CONUS-wide sizing pass: of 2,073 HUC12s missing from the crosswalk (after
the HUC2-prefix scoping fix above), **88.0% (1,824) are
`divides_exist_not_crosswalked`** -- a real hydrofabric divide sits inside
them, but never made it into the crosswalk. Only 12.0% (249, overwhelmingly
`hutype` W/water and I/island) are genuinely divide-less. The classification
map shows the actionable gap concentrated in the intermountain West/Great
Basin, the Rockies, and interior Florida (the Everglades), while the
structural gap traces the coastline almost exactly. This revised an earlier,
less rigorous pass at this same question (using the divide-area-normalized
metric described above) that had characterized most of this gap as likely
structural (coastal fringe / closed-basin terrain) -- it is not; it is
predominantly a fixable crosswalk-build gap.

Florida reassignment diagnostic: of 100 actionable divide/HUC12 pairs, 74%
are explainable by a majority-share-of-divide rule (not a defect); 26% are
not, led by a case where the "missing" HUC12 holds 80.9% of a normally-sized
(72 sqkm) divide's footprint while the HUC12 the crosswalk actually assigned
it to holds only 11.4% -- a strong, unambiguous crosswalk-build defect on
that specific divide, worth escalating to whoever regenerates
`hfv4_parameters_CONUS_pre-crosswalk_huc12_network_crosswalk.parquet`.
