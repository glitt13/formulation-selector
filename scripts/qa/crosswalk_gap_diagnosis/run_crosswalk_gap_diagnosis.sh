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
# The Florida default for passes 2/3 above is enforced HERE, by this driver,
# not by qa_check_missing_huc12_divide_existence.py's own --states default
# (which is None/CONUS-wide) -- that script is also pass 1, called with no
# --states at all, so its own default must stay CONUS-wide. Before this
# default was added, a bare invocation (e.g. from regn_all_proc.sh, which
# never passes --states) left pass 2 with no region argument either, silently
# falling through to that CONUS-wide default: pass 2 became a byte-for-byte
# duplicate of pass 1, doubling the cost of the most expensive step and
# overwriting pass 1's own CONUS output files with an identical copy.
# qa_check_huc12_divide_reassignment.py (pass 3) already defaulted to Florida
# on its own; this just makes pass 2 consistent with it (and with the
# documentation above, which always described pass 2 this way).
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
# Default passes 2/3 to Florida when the caller supplies no region at all --
# see the note above this script's Usage block for why this can't just be
# qa_check_missing_huc12_divide_existence.py's own --states default instead.
# ${#EXTRA_ARGS[@]} (a length query) is safe on an empty array even under
# `set -u` in bash 3.2; it's only the "expand all elements" form below that
# needs the ${EXTRA_ARGS[@]+...} guard.
if [ ${#EXTRA_ARGS[@]} -eq 0 ]; then
    EXTRA_ARGS=(--states FL)
fi
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
