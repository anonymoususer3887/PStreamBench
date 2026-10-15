#!/usr/bin/env bash
# Per-strategy plot families and framework-group comparisons, generated with the
# baseline plot functions in 6_make_all_plots_risingwave.sh and 3_plot_pdsp_query_metrics.py.
#
# 6_make_all_plots_risingwave.sh is NOT modified. For every run this script writes a patched
# copy (CSV, OUT_DIR and FRAMEWORKS replaced) into plots_todate/_runner/ and runs that copy.
#
# Output
#   plots_todate/<strategy>/...                      existing plot families, one strategy
#   plots_todate/comparison/<group>/<strategy>/...   framework-group comparison, one strategy
#
# Framework groups are defined once, in 7_plot_pdsp_enumeration_analysis.py (FRAMEWORK_GROUPS).
# Cross-strategy comparisons (Uniform/Random/DS2/Dhalion in one figure) are script 7's job.
#
# Usage
#   ./8_make_strategy_plots.sh list                      show the plan
#   ./8_make_strategy_plots.sh coverage [<strategy>]     coverage report only (no plots)
#   ./8_make_strategy_plots.sh dry-run <target> [ids]    print the commands, run nothing
#   ./8_make_strategy_plots.sh validate                  a few representative plots, then stop
#   ./8_make_strategy_plots.sh strategy [<strategy>]     all menu families (ids 1-17)
#   ./8_make_strategy_plots.sh comparison [<strategy>]   group comparisons, all three groups
#   ./8_make_strategy_plots.sh all                       strategy + comparison (long run)
#   <target> = strategy name, or comparison/<group>/<strategy>
#
# Python: PYTHON=/path/to/venv/bin/python (default: python3; needs pandas + matplotlib + scienceplots)
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"
PY_DIR="$(cd "$(dirname "$(command -v "$PY")")" && pwd)"
export PATH="${PY_DIR}:${PATH}"   # the baseline menu calls `python`

BASELINE="6_make_all_plots_risingwave.sh"
ENUM_SCRIPT="7_plot_pdsp_enumeration_analysis.py"
ROOT="plots_todate"
DATA_DIR="${ROOT}/_data"
RUNNER_DIR="${ROOT}/_runner"
COVER_DIR="${ROOT}/_coverage"

STRATEGIES=(uniform random ds2 dhalion)
GROUP_NAMES=(all_5 flink_spark_storm kafka_risingwave)
# Per-call subprocess counts (query_ids x hardware x rate x framework x metric, as the
# baseline menu actually loops): ids 8 and 12 run ~50,000 Python launches EACH per strategy
# (hours), 9/13/17 run ~1,000-9,000. Excluded from the default batch; run them separately,
# narrowed to a few queries/metrics, once the fast families are reviewed.
ALL_IDS="1 2 3 4 5 6 7 10 11 15 16"
SLOW_IDS="9 13 14 17"
VERY_SLOW_IDS="8 12"
COMPARISON_IDS="2 3 4 5 7 10 11 15 16"
VALIDATE_SINGLE_ID="1"       # intro figure (Flink, Storm; see 9_plot_intro_lmh.py)
VALIDATE_GROUP="flink_spark_storm"
VALIDATE_COMPARISON_ID="7"   # queries compare frameworks

group_frameworks() {
  "$PY" "$ENUM_SCRIPT" --print-group "$1"
}

# ---------------------------------------------------------------------------
# Canonical strategy split: raw -> canonical, printed before anything is generated.
# uniform_1 is excluded (legacy directory, not a strategy). Unknown labels abort.
# ---------------------------------------------------------------------------
split_strategies() {
  mkdir -p "${DATA_DIR}"
  "$PY" - "${DATA_DIR}" <<'PY'
import os, sys
import pandas as pd

out_dir = sys.argv[1]
CANON = {"uniform": "uniform", "random": "random", "ds2": "ds2", "dhalion": "dhalion"}
EXCLUDE = {"uniform_1"}

df = pd.read_csv("query_performance_data.csv", low_memory=False)
raw = df["enumeration_strategy"].astype(str).str.strip()
canon = raw.str.lower().map(lambda s: CANON.get(s, "EXCLUDED" if s in EXCLUDE else None))

print("raw -> canonical strategy counts")
table = pd.crosstab(raw, canon.fillna("UNMAPPED"))
print(table.to_string())
if canon.isna().any():
    unknown = sorted(raw[canon.isna()].unique().tolist())
    sys.exit(f"ERROR: unmapped strategy labels: {unknown}")

for strategy in ["uniform", "random", "ds2", "dhalion"]:
    sub = df[canon == strategy]
    path = os.path.join(out_dir, f"{strategy}.csv")
    sub.to_csv(path, index=False)
    print(f"  {strategy}: {len(sub)} rows -> {path}")
PY
}

# ---------------------------------------------------------------------------
# Coverage before generating: framework x query x hardware x rate for one strategy and group.
# ---------------------------------------------------------------------------
print_coverage() {
  local strategy="$1" group="$2"
  mkdir -p "${COVER_DIR}"
  local fws
  fws="$(group_frameworks "$group")"
  "$PY" - "${DATA_DIR}/${strategy}.csv" "${COVER_DIR}/${group}_${strategy}.csv" ${fws} <<'PY'
import sys
import pandas as pd

csv_in, csv_out, fws = sys.argv[1], sys.argv[2], sys.argv[3:]
df = pd.read_csv(csv_in, low_memory=False)
df = df[df["framework"].isin(fws)]
cov = (df.groupby(["framework", "query", "query_variant", "hardware_type", "producer_rate"], dropna=False)
         .agg(runs=("parallelism", "size"), unique_configs=("parallelism", "nunique"))
         .reset_index())
cov.to_csv(csv_out, index=False)
print(f"  coverage: {len(cov)} (framework, query, variant, hardware, rate) cells -> {csv_out}")
per_fw = cov.groupby("framework").agg(queries=("query", "nunique"), hardware=("hardware_type", "nunique"),
                                      rates=("producer_rate", "nunique"), runs=("runs", "sum"))
print(per_fw.to_string())
PY
}

# ---------------------------------------------------------------------------
# Patched copy of the baseline menu: only CSV, OUT_DIR and FRAMEWORKS are replaced.
# ---------------------------------------------------------------------------
make_runner() {
  local tag="$1" csv="$2" out="$3" fws="$4"
  mkdir -p "${RUNNER_DIR}"
  local runner="${RUNNER_DIR}/run6_${tag}.sh"
  local fw_array=""
  for f in $fws; do fw_array+="\"${f}\" "; done
  sed -e "s|^CSV=.*|CSV=\"${csv}\"|" \
      -e "s|^OUT_DIR=.*|OUT_DIR=\"${out}\"|" \
      -e "s|^FRAMEWORKS=.*|FRAMEWORKS=(${fw_array% })|" "${BASELINE}" > "${runner}"
  chmod +x "${runner}"
  # Confirm the patch took effect before running.
  grep -q "^CSV=\"${csv}\"$" "${runner}" && grep -q "^OUT_DIR=\"${out}\"$" "${runner}" \
    && grep -q "^FRAMEWORKS=(${fw_array% })$" "${runner}" \
    || { echo "ERROR: could not patch ${BASELINE} into ${runner}"; exit 1; }
  echo "${runner}"
}

run_patched() {
  local tag="$1" strategy="$2" out="$3" fws="$4" ids="$5" dry="$6"
  local csv="${DATA_DIR}/${strategy}.csv"
  local runner
  runner="$(make_runner "$tag" "$csv" "$out" "$fws")"
  echo
  echo ">>> ${tag}: strategy=${strategy} frameworks=[${fws}] out=${out} ids=[${ids}]"
  echo "    runner: ${runner}"
  if [[ "$dry" == "1" ]]; then
    echo "    (dry run: not executed)"
    return 0
  fi
  "./${runner}" ${ids}
}

# Output folder names from 6_make_all_plots_risingwave.sh -> numbered families (plot list 1-14).
FAMILY_RENAME=(
  "intro_hw_scaling_multi:13_intro_plots"
  "intro_hw_scaling_multi_fw:13_intro_plots"
  "scaling_single:01_scaling_single_framework"
  "scaling_compare_fw:02_scaling_compare_frameworks"
  "eventrate_single:03_eventrate_single_framework"
  "eventrate_compare_fw:04_eventrate_compare_frameworks"
  "queries_single:05_queries_single_framework"
  "queries_compare_fw:06_queries_compare_frameworks"
  "low_med_high_parallelism_across_hw:07_low_med_high_parallelism"
  "low_med_high_eventrate_across_hw:08_low_med_high_eventrate"
  "dual_axis_tp_latency:09_throughput_latency_dual_axis"
  "app_scaling_all_hw:10_all_hardware_scaling"
  "app_eventrate_all_hw:11_all_hardware_eventrate"
  "efficiency_across_hw:12_cpu_network_efficiency"
  "workload_fw_hw_parallelism:14_workload_framework_hardware"
)

# Moves (copies then removes) the baseline output folders into their numbered names.
number_folders() {
  local root="$1" pair old new
  [[ -d "$root" ]] || return 0
  for pair in "${FAMILY_RENAME[@]}"; do
    old="${pair%%:*}"; new="${pair##*:}"
    if [[ -d "${root}/${old}" ]]; then
      mkdir -p "${root}/${new}"
      cp -R "${root}/${old}/." "${root}/${new}/"
      rm -rf "${root}/${old}"
      echo "    ${root}/${old} -> ${root}/${new}"
    fi
  done
}

single_strategy() {
  local strategy="$1" ids="$2" dry="$3"
  print_coverage "$strategy" "all_5"
  run_patched "${strategy}_single" "$strategy" "${ROOT}/${strategy}" \
    "$(group_frameworks all_5)" "$ids" "$dry"
  [[ "$dry" == "1" ]] || number_folders "${ROOT}/${strategy}"
}

comparison_for() {
  local group="$1" strategy="$2" ids="$3" dry="$4"
  print_coverage "$strategy" "$group"
  run_patched "${group}_${strategy}" "$strategy" "${ROOT}/comparison/${group}/${strategy}" \
    "$(group_frameworks "$group")" "$ids" "$dry"
  [[ "$dry" == "1" ]] || number_folders "${ROOT}/comparison/${group}/${strategy}"
}

list_plan() {
  cat <<EOF
Existing plot families per strategy -> ${ROOT}/<strategy>/
  strategies: ${STRATEGIES[*]}
  menu ids:   ${ALL_IDS}

Framework-group comparisons -> ${ROOT}/comparison/<group>/<strategy>/
  groups: ${GROUP_NAMES[*]}
EOF
  for g in "${GROUP_NAMES[@]}"; do echo "  ${g}: $(group_frameworks "$g")"; done
  cat <<EOF
  menu ids: ${COMPARISON_IDS}

Cross-strategy comparisons (Uniform/Random/DS2/Dhalion in one figure) -> 7_make_enumeration_plots.sh
EOF
}

cmd="${1:-list}"
shift || true

case "$cmd" in
  list) list_plan ;;
  coverage)
    split_strategies
    targets=("${STRATEGIES[@]}"); [[ $# -ge 1 ]] && targets=("$1")
    for s in "${targets[@]}"; do for g in "${GROUP_NAMES[@]}"; do print_coverage "$s" "$g"; done; done ;;
  dry-run)
    split_strategies
    target="${1:?usage: dry-run <strategy | comparison/<group>/<strategy>> [ids]}"; shift || true
    ids="${*:-$ALL_IDS}"
    if [[ "$target" == comparison/* ]]; then
      IFS=/ read -r _ g s <<< "$target"
      comparison_for "$g" "$s" "${*:-$COMPARISON_IDS}" 1
    else
      single_strategy "$target" "$ids" 1
    fi ;;
  validate)
    split_strategies
    echo; echo "=== VALIDATE: representative plots only ==="
    single_strategy uniform "$VALIDATE_SINGLE_ID" 0
    # One comparison figure only: call the plot function directly, constrained to one
    # query, hardware, rate and metric (the baseline menu loops over all of them).
    validate_out="${ROOT}/comparison/${VALIDATE_GROUP}/uniform/06_queries_compare_frameworks/validate"
    mkdir -p "${validate_out}"
    echo; echo ">>> validate comparison: group=${VALIDATE_GROUP} strategy=uniform query=Click Analytics_1 hw=m510 rate=500000 metric=throughput_50"
    "$PY" 3_plot_pdsp_query_metrics.py \
      --csv "${DATA_DIR}/uniform.csv" \
      --plot queries_compare_frameworks \
      --frameworks $(group_frameworks "$VALIDATE_GROUP") \
      --query "Click Analytics_1" \
      --hardware_type m510 \
      --producer_rate 500000 \
      --metric throughput_50 \
      --agg mean \
      --save "${validate_out}/queries_compare_click_time_m510_500k_throughput_50" \
      --all-formats --no-show
    echo; echo "Validation done. Inspect the figures, then run 'strategy' or 'comparison'." ;;
  strategy)
    split_strategies
    targets=("${STRATEGIES[@]}"); [[ $# -ge 1 ]] && targets=("$1")
    for s in "${targets[@]}"; do single_strategy "$s" "$ALL_IDS" 0; done ;;
  comparison)
    split_strategies
    targets=("${STRATEGIES[@]}"); [[ $# -ge 1 ]] && targets=("$1")
    for g in "${GROUP_NAMES[@]}"; do
      for s in "${targets[@]}"; do comparison_for "$g" "$s" "$COMPARISON_IDS" 0; done
    done ;;
  number)
    # Rename existing baseline output folders into the numbered family names.
    for s in "${STRATEGIES[@]}"; do number_folders "${ROOT}/${s}"; done
    for g in "${GROUP_NAMES[@]}"; do
      for s in "${STRATEGIES[@]}"; do number_folders "${ROOT}/comparison/${g}/${s}"; done
    done ;;
  all)
    "$0" strategy
    "$0" comparison ;;
  *) echo "unknown command: $cmd"; list_plan; exit 1 ;;
esac
