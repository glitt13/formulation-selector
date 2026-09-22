#!/bin/bash
# run_qa_checks.sh -- run every scripts/qa/ check against one pred_config.
#
# Usage:
#   ./run_qa_checks.sh /path/to/some_pred_config.yaml
#
# Intended to be called as an optional, non-fatal step from a workflow's own
# processing shell script (e.g. regn_all_proc.sh) -- see the "OPTIONAL QA
# CHECKS" block near the end of that script for the calling convention, and
# scripts/qa/README.md for what each check covers and why exit codes here
# are always 0.
#
# Deliberately does NOT use `set -e`: a QA check finding a real gap is a
# result to report, not a reason to abort whatever called this script.
set -uo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 /path/to/pred_config.yaml [extra args passed through to every check]"
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
DIR_REPO="$(cd "${DIR_QA}/../.." && pwd)"
DIR_FLOW_QA="${DIR_REPO}/pkg/rafts_algo/rafts_algo/flow/qa"

echo "========================================================================"
echo "QA CHECKS for: ${PATH_PRED_CONFIG}"
echo "========================================================================"

echo "--> [QA] Attribute-data <-> crosswalk coverage..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_FLOW_QA}/qa_check_attrs_crosswalk_coverage.py" \
    --path_pred_config "${PATH_PRED_CONFIG}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

echo ""
echo "--> [QA] Compiled SQLite <-> crosswalk coverage..."
uv run --project "${DIR_REPO}/pkg" python "${DIR_FLOW_QA}/qa_check_sqlite_crosswalk_coverage.py" \
    --path_pred_config "${PATH_PRED_CONFIG}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

echo "========================================================================"
echo "QA CHECKS complete for: ${PATH_PRED_CONFIG}"
echo "========================================================================"
