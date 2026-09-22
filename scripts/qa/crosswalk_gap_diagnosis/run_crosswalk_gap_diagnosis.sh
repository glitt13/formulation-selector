#!/bin/bash
# run_crosswalk_gap_diagnosis.sh -- root-cause investigation for "missing"
# HUC12s: HUC12s with no entry in the huc12<->divide_id crosswalk, and so no
# regionalized predictions can ever reach them.
#
# Runs three passes (see README.md in this directory for the full strategy
# and what each pass answers):
#   1. CONUS-wide sizing pass (qa_check_missing_huc12_divide_existence.py,
#      no --states): how big is the problem, and how much of it is
#      structural (no real divide there) vs. actionable (a divide exists but
#      wasn't crosswalked)?
#   2. Region-scoped divide-existence check, same script with --states: a
#      closer look at one region (defaults to Florida).
#   3. Region-scoped reassignment diagnostic
#      (qa_check_huc12_divide_reassignment.py): for that region's actionable
#      gaps, where did the overlapping divide actually get crosswalked to
#      instead, and is that assignment defensible?
#
# Usage:
#   ./run_crosswalk_gap_diagnosis.sh /path/to/some_pred_config.yaml [--states XX YY ...] [extra args]
#
# Any extra arguments (e.g. --states, --min_coverage_frac) are passed through
# to passes 2 and 3 only -- pass 1 is always a full, unfiltered CONUS sizing
# pass, since that's the point of running it.
#
# Deliberately does NOT use `set -e`: a QA check finding a real gap is a
# result to report, not a reason to abort whatever called this script (see
# scripts/qa/README.md -- these checks always exit 0).
set -uo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 /path/to/pred_config.yaml [--states XX YY ...] [extra args passed to the region-scoped passes]"
    exit 1
fi

PATH_PRED_CONFIG="$1"
shift || true
EXTRA_ARGS=("$@")
# Expanded below as ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}, not "${EXTRA_ARGS[@]}":
# macOS's system /bin/bash is 3.2 (pre-4.4), which treats an empty array
# expansion under `set -u` as an unbound-variable error. The `+` form avoids
# triggering that check when EXTRA_ARGS is empty.

DIR_QA="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR_REPO="$(cd "${DIR_QA}/../../.." && pwd)"
DIR_FLOW_QA="${DIR_REPO}/pkg/rafts_algo/rafts_algo/flow/qa"

echo "========================================================================"
echo "CROSSWALK GAP DIAGNOSIS for: ${PATH_PRED_CONFIG}"
echo "========================================================================"

echo "--> [1/3] CONUS-wide sizing pass..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_FLOW_QA}/qa_check_missing_huc12_divide_existence.py" \
    --path_pred_config "${PATH_PRED_CONFIG}"

echo ""
echo "--> [2/3] Region-scoped divide-existence check..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_FLOW_QA}/qa_check_missing_huc12_divide_existence.py" \
    --path_pred_config "${PATH_PRED_CONFIG}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

echo ""
echo "--> [3/3] Region-scoped divide-reassignment diagnostic..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_FLOW_QA}/qa_check_huc12_divide_reassignment.py" \
    --path_pred_config "${PATH_PRED_CONFIG}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

echo "========================================================================"
echo "CROSSWALK GAP DIAGNOSIS complete for: ${PATH_PRED_CONFIG}"
echo "========================================================================"
