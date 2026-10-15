#!/usr/bin/env bash
# Enumeration-strategy plots (validation stage). Wraps 7_plot_pdsp_enumeration_analysis.py.
#
#   ./7_make_enumeration_plots.sh validate        # the six validation figures (A1 A2 B C D1 D2 E F)
#   ./7_make_enumeration_plots.sh coverage        # coverage report only
#   ./7_make_enumeration_plots.sh searchspace     # single mode (filters below)
#   ./7_make_enumeration_plots.sh config | selected | framework | job | regression
#   ./7_make_enumeration_plots.sh all             # DISABLED until the validation figures are approved
#
# Filters (env): QUERY, QUERY_VARIANT, HW, RATE, METRIC, GROUP, SUMMARY, LAYOUT, FIGURE_SIZE, PAPER_MODE=1
#   QUERY="Click Analytics" QUERY_VARIANT=time-based HW=m510 RATE=500000 ./7_make_enumeration_plots.sh searchspace
# Python interpreter: PYTHON=/path/to/venv/bin/python (default: python3; needs pandas + matplotlib)
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"
SCRIPT="7_plot_pdsp_enumeration_analysis.py"

QUERY="${QUERY:-Click Analytics}"
QUERY_VARIANT="${QUERY_VARIANT-time-based}"   # set QUERY_VARIANT="" for queries without variants
HW="${HW:-m510}"
RATE="${RATE:-500000}"
METRIC="${METRIC:-throughput_50}"
GROUP="${GROUP:-all_5}"
STRATEGY="${STRATEGY:-}"   # required for config_perf_single / config_perf_compare
SUMMARY="${SUMMARY:-mean}"
LAYOUT="${LAYOUT:-D1}"
FIGURE_SIZE="${FIGURE_SIZE:-single}"
PAPER_MODE_FLAG=()
if [[ "${PAPER_MODE:-0}" == "1" ]]; then PAPER_MODE_FLAG=(--paper-mode); fi

# Framework groups are defined once, in the Python script.
group_members() { "$PY" "$SCRIPT" --print-group "$1"; }

common_args() {
  printf '%s\n' --query "$QUERY" --query-variant "$QUERY_VARIANT" --hw "$HW" --rate "$RATE" \
                --metric "$METRIC" --figure-size "$FIGURE_SIZE"
}

run_py() {
  echo
  echo "+ $PY $SCRIPT $*"
  "$PY" "$SCRIPT" "$@" ${PAPER_MODE_FLAG[@]+"${PAPER_MODE_FLAG[@]}"}
}

validate() {
  # A1 / A2: search space, Flink, all unique configurations
  run_py --mode searchspace --framework-group flink_spark_storm --parallelism-summary mean \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
  run_py --mode searchspace --framework-group flink_spark_storm --parallelism-summary sum \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
  # B: configuration-label plot (same data)
  run_py --mode config_labels --framework-group flink_spark_storm \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size double
  # C: selected operator allocation + summary table
  run_py --mode selected --framework-group flink_spark_storm \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size double
  # D1 / D2: Flink / Storm / Spark cross-framework comparison
  run_py --mode framework_compare --framework-group flink_spark_storm --framework-layout D1 \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
  run_py --mode framework_compare --framework-group flink_spark_storm --framework-layout D2 \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size double
  # E: Kafka Streams vs RisingWave, job-level, only where semantics are verified (Word Count)
  run_py --mode job_level --framework-group kafka_risingwave \
         --query "Word Count" --query-variant "" --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
  # G: cross-strategy comparison (x = framework, series = strategy), one figure per group
  run_py --mode strategy_framework --framework-group flink_spark_storm \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
  run_py --mode strategy_framework --framework-group kafka_risingwave \
         --query "Word Count" --query-variant "" --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
  run_py --mode strategy_framework --framework-group all_5 \
         --query "Word Count" --query-variant "" --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size double
  # F: Uniform regression against the baseline 3_ helper (Flink, Click Analytics time-based)
  run_py --mode regression --regression-framework flink \
         --query "Click Analytics" --query-variant time-based --hw m510 --rate 500000 --metric throughput_50 \
         --figure-size single
}

mode_run() {
  local mode="$1"
  local extra=()
  case "$mode" in
    searchspace) extra=(--mode searchspace --parallelism-summary "$SUMMARY") ;;
    config) extra=(--mode config_labels) ;;
    selected) extra=(--mode selected) ;;
    framework) extra=(--mode framework_compare --framework-layout "$LAYOUT") ;;
    strategy_framework) extra=(--mode strategy_framework) ;;
    selected_performance) extra=(--mode selected_performance --metric "$METRIC") ;;
    efficiency) extra=(--mode selected_performance --metric throughput_per_sum_inner) ;;
    strategy_rate) extra=(--mode strategy_rate) ;;
    strategy_hardware) extra=(--mode strategy_hardware) ;;
    strategy_workload) extra=(--mode strategy_workload) ;;
    tp_latency) extra=(--mode tp_latency) ;;
    config_perf_single) extra=(--mode config_perf_single --strategy "$STRATEGY") ;;
    config_perf_compare) extra=(--mode config_perf_compare --strategy "$STRATEGY") ;;
    config_eventrate) extra=(--mode config_eventrate --strategy "$STRATEGY") ;;
    operator_own_metrics) extra=(--mode operator_own_metrics --own-metric "${OWN_METRIC:-throughput_per_sec}") ;;
    job) extra=(--mode job_level) ;;
    regression) extra=(--mode regression) ;;
    coverage) extra=(--mode coverage) ;;
  esac
  run_py "${extra[@]}" --framework-group "$GROUP" \
         --query "$QUERY" --query-variant "$QUERY_VARIANT" --hw "$HW" --rate "$RATE" \
         --metric "$METRIC" --figure-size "$FIGURE_SIZE"
}

usage() {
  sed -n '2,12p' "$0"
  echo
  echo "Framework groups (from $SCRIPT):"
  for g in all_5 flink_spark_storm kafka_risingwave; do
    echo "  $g: $(group_members "$g")"
  done
}

# Query/variant pairs present in the CSV for a framework group.
query_variants() {
  "$PY" - "$1" <<'PY'
import sys
import pandas as pd

fws = sys.argv[1].split(",")
df = pd.read_csv("query_performance_data.csv", low_memory=False)
df = df[df["framework"].isin(fws)].copy()
df["query_variant"] = df["query_variant"].fillna("")
for q, v in sorted(df[["query", "query_variant"]].drop_duplicates().itertuples(index=False)):
    print(f"{q}\t{v}")
PY
}

# Full workload x hardware x event-rate x metric matrix (Pipeline 2 + Pipeline 3).
# The loop logic lives in 12_run_full_enumeration_batch.py, not here -- see that file's
# docstring for exactly what each mode loops and why (several modes sweep a dimension
# internally and must NOT also be looped over that same CLI flag).
batch_enum() {
  local dry="$1"
  local extra=()
  [[ "$dry" == "1" ]] && extra=(--dry-run)
  PYTHON="$PY" "$PY" 12_run_full_enumeration_batch.py "${extra[@]}"
}

list_plots() {
  cat <<'EOF'
Plots available from this script (output: plots_enumeration/<family>/<group>/<query>/<variant>/<hw>/<rate>/<metric>/)

  validate  -> the full validated set (A1, A2, B, C, D1, D2, E, F); the commands below
  searchspace   A1  every unique configuration, x = mean inner parallelism   (one panel per framework)
                A2  same, x = sum inner parallelism                          (set SUMMARY=sum)
  config        B   every configuration as a compact label (s10_x16_..._k1), bars coloured by strategy
  selected      C   one selected configuration per strategy, operator allocation + summary CSV
  framework     D1  selected configuration per framework, grouped by strategy  (FRAMEWORK group)
                D2  same data, one panel per framework                          (LAYOUT=D2)
  job           E   Kafka Streams vs RisingWave job-level, only where semantics are verified (Word Count)
  regression    F   Uniform-only check against the baseline 3_ helper (must PASS)
  coverage          coverage report only (no figure)

Not yet implemented (planned): strategy vs event rate, latency/CPU/network efficiency,
per-config operator parallelism beyond the selected plan. The existing dual-axis throughput+latency
function in 3_plot_pdsp_query_metrics.py is the intended source for combined latency plots.

Selection objective: throughput only (validation stage).
EOF
}

cmd="${1:-help}"
case "$cmd" in
  list) list_plots ;;
  validate) validate ;;
  coverage|searchspace|config|selected|framework|strategy_framework|selected_performance|efficiency|strategy_rate|strategy_hardware|strategy_workload|tp_latency|config_perf_single|config_perf_compare|config_eventrate|operator_own_metrics|job|regression) mode_run "$cmd" ;;
  batch-dry) batch_enum 1 ;;
  batch) batch_enum 0 ;;
  all)
    echo "ERROR: 'all' is disabled until the validation figures are reviewed and approved."
    exit 1 ;;
  help|-h|--help) usage ;;
  *) echo "unknown command: $cmd"; usage; exit 1 ;;
esac
