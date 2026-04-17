#!/usr/bin/env -S bash
set -euo pipefail

if (( BASH_VERSINFO[0] < 4 )); then
  echo "ERROR: This script requires Bash >= 4 (associative arrays)."
  echo "Your bash version: ${BASH_VERSION}"
  echo
  echo "macOS fix:"
  echo "  brew install bash"
  echo "  /opt/homebrew/bin/bash $0 $*"
  echo
  exit 1
fi

declare -a PY_LMH_ARGS=()

# -------------------------------------------------------------------
# PlotGeneratorPDSP — interactive, GitHub-friendly plot builder
#
# Usage:
#   ./make_all_plots.sh                 # interactive menu
#   ./make_all_plots.sh all             # run everything
#   ./make_all_plots.sh 1               # run a single plot group (by id)
#   ./make_all_plots.sh 1 3 5           # run multiple plot groups
#   ./make_all_plots.sh 1,3,5           # same as above
#   ./make_all_plots.sh help            # show menu + guide
#
# Optional environment overrides (global):
#   CHART_TYPE=line Y_SCALE=log SHOW_VALUES=0 ./make_all_plots.sh all
#
# Optional style overrides:
#   MPL_STYLE=ieee LATEX_TEXT=1 HW_PALETTE=tableau ./make_all_plots.sh all
#
# Optional environment overrides (axis + density control):
#   YMIN=1 YMAX=1e7 ./make_all_plots.sh all
#   MAX_POINTS=8 POINT_STRATEGY=even ./make_all_plots.sh all
# -------------------------------------------------------------------

APP_NAME="PlotGeneratorPDSP"
BANNER_WIDTH=72

hr() { printf '%*s\n' "$BANNER_WIDTH" '' | tr ' ' '='; }

print_banner() {
  echo
  if command -v figlet >/dev/null 2>&1; then
    figlet -w "$BANNER_WIDTH" "$APP_NAME"
  else
    hr
    echo " ${APP_NAME}"
    hr
  fi
  echo
  hr
  echo " setup ${APP_NAME} started "
  hr
  echo
}

# -----------------------------
# Data + scripts
# -----------------------------
CSV="query_performance_data.csv"
OUT_DIR="plots_pdsp"
PLOT_SCRIPT="3_plot_pdsp_query_metrics.py"

# -----------------------------
# GLOBAL PLOT OPTIONS (env overridable)
# -----------------------------
CHART_TYPE="${CHART_TYPE:-bar}"        # bar | line
Y_SCALE="${Y_SCALE:-linear}"           # linear | log
SHOW_VALUES="${SHOW_VALUES:-1}"        # 1 = show bar labels, 0 = don't

# axis limits (optional)
YMIN="${YMIN:-}"
YMAX="${YMAX:-}"

# density control (optional)
MAX_POINTS="${MAX_POINTS:-}"
POINT_STRATEGY="${POINT_STRATEGY:-even}"  # even | head | tail

# style control (optional)
MPL_STYLE="${MPL_STYLE:-science}"      # science | ieee | nature | default
LATEX_TEXT="${LATEX_TEXT:-0}"          # 1=enable latex, 0=disable
HW_PALETTE="${HW_PALETTE:-modern}"     # modern | tableau | okabeito | muted

# LMH: label verbosity
SHOW_GROUP_VALUES="${SHOW_GROUP_VALUES:-0}"  # 1=show (hw:value) inside xticks

# Build common args for python calls
PY_COMMON_ARGS=( --chart "${CHART_TYPE}" --y-scale "${Y_SCALE}" --mpl-style "${MPL_STYLE}" --hw-palette "${HW_PALETTE}" )

if [[ "${LATEX_TEXT}" -eq 1 ]]; then
  PY_COMMON_ARGS+=( --latex-text )
fi

if [[ "${SHOW_VALUES}" -eq 1 ]]; then
  PY_COMMON_ARGS+=( --show-values )
fi

if [[ -n "${YMIN}" ]]; then
  PY_COMMON_ARGS+=( --ymin "${YMIN}" )
fi

if [[ -n "${YMAX}" ]]; then
  PY_COMMON_ARGS+=( --ymax "${YMAX}" )
fi

if [[ -n "${MAX_POINTS}" ]]; then
  PY_COMMON_ARGS+=( --max-points "${MAX_POINTS}" --point-strategy "${POINT_STRATEGY}" )
fi

# ---- EDIT THESE ARRAYS TO MATCH YOUR SETUP ----
HARDWARES=(
  "c6525-100g"
  "c6525-25g"
  "m510"
)

PRODUCER_RATES=(
  500000
  1000000
)

CORE_METRICS=(
  throughput_50
  throughput_95
  throughput_98
  endtoend_latency
  cpu_50
  cpu_95
  cpu_98
  memory_50
  memory_95
  memory_98
  network_50
  network_95
  network_98
  event_rate_50
  event_rate_95
  event_rate_98
)

EFFICIENCY_METRICS=(
  cpu_efficiency
  network_efficiency
)

FRAMEWORKS=("flink" "storm")

parallelisms_for_hw() {
  local hw="$1"
  case "${hw}" in
    c6525-25g)  echo "2 4 8 16 32 40 48 64 72 80" ;;
    c6525-100g) echo "2 4 8 16 32 40 48 64 72 80 96" ;;
    m510)       echo "2 4 8 16 32 40" ;;
    *)          echo "2 4 8 16 32 40 48 64" ;;
  esac
}

REP_PARALLELISM=32
mkdir -p "${OUT_DIR}"
safe() { echo "$1" | tr ' ' '_'; }

# Intro (Python expands base->all query_ids for intro plots)
INTRO_QUERIES=("Smart Grid" "Click Analytics" "Ad Analytics")
INTRO_RATE=1000000

# LMH mappings
LMH_PAR_MAP_JSON='{"m510":[2,16,40],"c6525-25g":[2,16,40],"c6525-100g":[2,16,40]}'
LMH_RATE_MAP_JSON='{"m510":[100000,500000,1000000],"c6525-25g":[100000,500000,1000000],"c6525-100g":[100000,500000,1000000]}'

if [[ "${SHOW_GROUP_VALUES}" == "1" ]]; then
  PY_LMH_ARGS=( --show-group-values )
else
  PY_LMH_ARGS=()
fi

# -----------------------------
# Menu: one option per plot function
# -----------------------------
# Format: id|key|title|what_it_does|where_outputs_go|typical_runtime
MENU_ITEMS=(
  "1|intro_hw_scaling_multi|Intro HW scaling (Flink)|3 workloads × HW (bars/lines) at fixed rate=1M; shows scaling across HW for Flink only|${OUT_DIR}/intro_hw_scaling_multi/*|fast"
  "2|intro_hw_scaling_multi_fw|Intro HW scaling (Flink+Storm)|Same as (1) but compares Flink vs Storm in a 2×3 figure|${OUT_DIR}/intro_hw_scaling_multi_fw/*|fast"
  "3|batch_low_med_high_parallelism_across_hw|LMH Parallelism across HW|Batch plots at LOW/MED/HIGH parallelism per HW; outputs many metrics in one run|${OUT_DIR}/* (batch outputs)|medium"
  "4|batch_low_med_high_eventrate_across_hw|LMH Event-rate across HW|Batch plots at LOW/MED/HIGH event-rate per HW; outputs many metrics in one run|${OUT_DIR}/* (batch outputs)|medium"
  "5|dual_axis_tp_latency|Dual-axis Throughput+Latency|Per query_id+HW+rate: throughput bars + latency line (dual axis)|${OUT_DIR}/dual_axis_tp_latency/*|slow"
  "6|queries_single_framework|Queries in one plot (single FW)|For each FW+HW+rate: one figure with all query_ids (compare workloads)|${OUT_DIR}/queries_single/*|slow"
  "7|queries_compare_frameworks|Queries in one plot (FW compare)|For each HW+rate: one figure with all query_ids comparing Flink vs Storm|${OUT_DIR}/queries_compare_fw/*|slow"
  "8|scaling_single|Scaling per query_id (single FW)|Per query_id+FW+HW+rate: scaling vs parallelism for each metric|${OUT_DIR}/scaling_single/*|very slow"
  "9|scaling_compare_fw|Scaling per query_id (FW compare)|Per query_id+HW+rate: scaling vs parallelism comparing Flink vs Storm|${OUT_DIR}/scaling_compare_fw/*|very slow"
  "10|batch_compare_eventrates|Compare event-rates (batch)|Per query_id+HW: all rates in one plot for throughput/latency|${OUT_DIR}/* (batch outputs)|medium"
  "11|batch_all_metrics_eventrates|All metrics vs event-rates (batch)|Per query_id+HW: big batch across metrics × all rates|${OUT_DIR}/* (batch outputs)|slow"
  "12|eventrate_single|Event-rate sweep (single FW)|Per query_id+FW+HW+parallelism: one plot across rates for each metric|${OUT_DIR}/eventrate_single/*|very slow"
  "13|eventrate_compare_fw|Event-rate sweep (FW compare)|Per query_id+HW+parallelism: compare Flink vs Storm across rates|${OUT_DIR}/eventrate_compare_fw/*|very slow"
  "14|app_scaling_all_hw|App scaling across all HW|Per query_id+rate: one plot comparing all HW + both FW vs parallelism|${OUT_DIR}/app_scaling_all_hw/*|slow"
  "15|app_eventrate_all_hw|App event-rate across all HW|Per query_id+par: one plot comparing all HW + both FW vs event-rate|${OUT_DIR}/app_eventrate_all_hw/*|slow"
  "16|cpu_efficiency_across_hw|CPU/Network efficiency across HW|Efficiency plots (cpu/network efficiency) across HW at fixed rate/par map|${OUT_DIR}/efficiency_across_hw/*|medium"
  "17|workload_fw_hw_parallelism|Workload×FW×HW×parallelism grid|Per query_id+rate+metric: many figures for full interaction view|${OUT_DIR}/workload_fw_hw_parallelism/*|very slow"
  "18|all|ALL plots|Runs options 1–17 in order|${OUT_DIR}/*|depends (can be very slow)"
)

declare -A ID_TO_KEY=()
ALL_KEYS=()

print_menu() {
  echo "What do you want to do?"
  echo
  printf "%-4s %-38s %-10s\n" "ID" "Plot" "Runtime"
  printf "%-4s %-38s %-10s\n" "--" "----" "-------"
  for item in "${MENU_ITEMS[@]}"; do
    IFS='|' read -r id key title what out where runtime <<<"$item"
    printf "%-4s %-38s %-10s\n" "${id})" "${key}" "${runtime}"
  done
  echo
  echo "Tip: Type 'info <id>' to see what a plot does (e.g., info 9)."
  echo "Enter selection(s): e.g. 1 3 5 | 1,3,5 | all | q"
}

print_info() {
  local id="$1"
  for item in "${MENU_ITEMS[@]}"; do
    IFS='|' read -r mid key title what out runtime <<<"$item"
    if [[ "$mid" == "$id" ]]; then
      echo
      hr
      echo "Option ${mid}: ${title}"
      hr
      echo "Plot key:      ${key}"
      echo "What it does:  ${what}"
      echo "Outputs to:    ${out}"
      echo "Runtime:       ${runtime}"
      echo
      return 0
    fi
  done
  echo "Unknown id: $id"
  return 1
}

init_menu_maps() {
  for item in "${MENU_ITEMS[@]}"; do
    IFS='|' read -r id key title what out runtime <<<"$item"
    ID_TO_KEY["$id"]="$key"
    if [[ "$key" != "all" ]]; then
      ALL_KEYS+=("$key")
    fi
  done
}

# -----------------------------
# Sanity checks
# -----------------------------
require_file() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing file: $f"
    exit 1
  fi
}

# -----------------------------
# Discover QUERY_IDS from CSV
# -----------------------------
discover_query_ids() {
  echo ">>> Discovering query_ids from CSV (${CSV}) ..."
  QUERY_IDS=()
  while IFS= read -r qid; do
    [[ -z "${qid}" ]] && continue
    QUERY_IDS+=("${qid}")
  done < <(python - <<PY
import pandas as pd
import sys
csv = "${CSV}"
df = pd.read_csv(csv)
if "query" not in df.columns:
    sys.exit(0)
df["query"] = df["query"].astype(str)
if "job_query_number" in df.columns:
    df["job_query_number"] = pd.to_numeric(df["job_query_number"], errors="coerce").fillna(1).astype(int)
    df["query_id"] = df["query"] + "_" + df["job_query_number"].astype(str)
else:
    df["query_id"] = df["query"]
try:
    for qid in sorted(df["query_id"].dropna().unique().tolist()):
        print(qid)
except BrokenPipeError:
    pass
PY
  )

  if [[ ${#QUERY_IDS[@]} -eq 0 ]]; then
    echo "ERROR: No query_ids found in CSV (missing 'query' column?)."
    exit 1
  fi

  echo ">>> Found ${#QUERY_IDS[@]} query_ids."
}

# ===================================================================
# Plot functions (one per plot key)
# ===================================================================

run_intro_hw_scaling_multi() {
  echo ">>> [intro_hw_scaling_multi] Flink-only intro figure (3 workloads × HW @ rate=1M) ..."
  for METRIC in throughput_50 endtoend_latency; do
    SAVE_BASE="${OUT_DIR}/intro_hw_scaling_multi/intro_${METRIC}"
    python "${PLOT_SCRIPT}" \
      --csv "${CSV}" \
      --plot intro_hw_scaling_multi \
      --framework flink \
      --queries "${INTRO_QUERIES[@]}" \
      --producer_rate "${INTRO_RATE}" \
      --metric "${METRIC}" \
      --agg mean \
      --save "${SAVE_BASE}" \
      --all-formats \
      --no-show \
      "${PY_COMMON_ARGS[@]}" || true
  done
}

run_intro_hw_scaling_multi_fw() {
  echo ">>> [intro_hw_scaling_multi_fw] Flink vs Storm intro figure (2×3) @ rate=1M ..."
  for METRIC in throughput_50 endtoend_latency; do
    SAVE_BASE="${OUT_DIR}/intro_hw_scaling_multi_fw/intro_${METRIC}"
    python "${PLOT_SCRIPT}" \
      --csv "${CSV}" \
      --plot intro_hw_scaling_multi_fw \
      --frameworks "${FRAMEWORKS[@]}" \
      --queries "${INTRO_QUERIES[@]}" \
      --producer_rate "${INTRO_RATE}" \
      --metric "${METRIC}" \
      --agg mean \
      --save "${SAVE_BASE}" \
      --all-formats \
      --no-show \
      "${PY_COMMON_ARGS[@]}" || true
  done
}

run_batch_low_med_high_parallelism_across_hw() {
  echo ">>> [batch_low_med_high_parallelism_across_hw] LOW/MED/HIGH parallelism across hardware ..."
  python "${PLOT_SCRIPT}" \
    --csv "${CSV}" \
    --plot batch_low_med_high_parallelism_across_hw \
    --frameworks "${FRAMEWORKS[@]}" \
    --hardware-types "${HARDWARES[@]}" \
    --par-map-json "${LMH_PAR_MAP_JSON}" \
    --metrics "${CORE_METRICS[@]}" \
    --agg mean \
    --out_dir "${OUT_DIR}" \
    --all-formats \
    --no-show \
    "${PY_COMMON_ARGS[@]}" \
    ${PY_LMH_ARGS[@]+"${PY_LMH_ARGS[@]}"} || true
}

run_batch_low_med_high_eventrate_across_hw() {
  echo ">>> [batch_low_med_high_eventrate_across_hw] LOW/MED/HIGH event-rate across hardware ..."
  python "${PLOT_SCRIPT}" \
    --csv "${CSV}" \
    --plot batch_low_med_high_eventrate_across_hw \
    --frameworks "${FRAMEWORKS[@]}" \
    --hardware-types "${HARDWARES[@]}" \
    --rate-map-json "${LMH_RATE_MAP_JSON}" \
    --metrics "${CORE_METRICS[@]}" \
    --agg mean \
    --out_dir "${OUT_DIR}" \
    --all-formats \
    --no-show \
    "${PY_COMMON_ARGS[@]}" \
    ${PY_LMH_ARGS[@]+"${PY_LMH_ARGS[@]}"} || true
}

run_dual_axis_tp_latency() {
  echo ">>> [dual_axis_tp_latency] Dual-axis throughput+latency (per query_id+hw+rate) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for HW in "${HARDWARES[@]}"; do
      for RATE in "${PRODUCER_RATES[@]}"; do
        SAVE_BASE="${OUT_DIR}/dual_axis_tp_latency/${Q_SAFE}/${HW}/rate${RATE}"
        python "${PLOT_SCRIPT}" \
          --csv "${CSV}" \
          --plot dual_axis_tp_latency \
          --frameworks "${FRAMEWORKS[@]}" \
          --query "${QID}" \
          --hardware_type "${HW}" \
          --producer_rate "${RATE}" \
          --x-dim parallelism_avg \
          --tp-metric throughput_50 \
          --lat-metric endtoend_latency \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show \
          "${PY_COMMON_ARGS[@]}" || true
      done
    done
  done
}

run_queries_single_framework() {
  echo ">>> [queries_single_framework] All query_ids in one plot (single framework) ..."
  for HW in "${HARDWARES[@]}"; do
    for RATE in "${PRODUCER_RATES[@]}"; do
      for FW in "${FRAMEWORKS[@]}"; do
        for METRIC in "${CORE_METRICS[@]}"; do
          SAVE_BASE="${OUT_DIR}/queries_single/${FW}/${HW}/${RATE}_$(safe "${METRIC}")"
          python "${PLOT_SCRIPT}" \
            --csv "${CSV}" \
            --plot queries_single_framework \
            --framework "${FW}" \
            --hardware_type "${HW}" \
            --producer_rate "${RATE}" \
            --metric "${METRIC}" \
            --agg mean \
            --save "${SAVE_BASE}" \
            --all-formats \
            --no-show \
            "${PY_COMMON_ARGS[@]}" || true
        done
      done
    done
  done
}

run_queries_compare_frameworks() {
  echo ">>> [queries_compare_frameworks] All query_ids in one plot (Flink vs Storm) ..."
  for HW in "${HARDWARES[@]}"; do
    for RATE in "${PRODUCER_RATES[@]}"; do
      for METRIC in "${CORE_METRICS[@]}"; do
        SAVE_BASE="${OUT_DIR}/queries_compare_fw/${HW}/${RATE}_$(safe "${METRIC}")"
        python "${PLOT_SCRIPT}" \
          --csv "${CSV}" \
          --plot queries_compare_frameworks \
          --frameworks "${FRAMEWORKS[@]}" \
          --hardware_type "${HW}" \
          --producer_rate "${RATE}" \
          --metric "${METRIC}" \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show \
          "${PY_COMMON_ARGS[@]}" || true
      done
    done
  done
}

run_scaling_single() {
  echo ">>> [scaling_single] Internal scaling per query_id (single framework) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for HW in "${HARDWARES[@]}"; do
      for RATE in "${PRODUCER_RATES[@]}"; do
        for FW in "${FRAMEWORKS[@]}"; do
          for METRIC in "${CORE_METRICS[@]}"; do
            SAVE_BASE="${OUT_DIR}/scaling_single/${Q_SAFE}/${FW}/${HW}/${RATE}_$(safe "${METRIC}")"
            python "${PLOT_SCRIPT}" \
              --csv "${CSV}" \
              --plot scaling_single \
              --framework "${FW}" \
              --query "${QID}" \
              --hardware_type "${HW}" \
              --producer_rate "${RATE}" \
              --metric "${METRIC}" \
              --agg mean \
              --save "${SAVE_BASE}" \
              --all-formats \
              --no-show \
              "${PY_COMMON_ARGS[@]}" || true
          done
        done
      done
    done
  done
}

run_scaling_compare_fw() {
  echo ">>> [scaling_compare_fw] Scaling per query_id (Flink vs Storm) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for HW in "${HARDWARES[@]}"; do
      for RATE in "${PRODUCER_RATES[@]}"; do
        for METRIC in "${CORE_METRICS[@]}"; do
          SAVE_BASE="${OUT_DIR}/scaling_compare_fw/${Q_SAFE}/${HW}/${RATE}_$(safe "${METRIC}")"
          python "${PLOT_SCRIPT}" \
            --csv "${CSV}" \
            --plot scaling_compare_fw \
            --frameworks "${FRAMEWORKS[@]}" \
            --query "${QID}" \
            --hardware_type "${HW}" \
            --producer_rate "${RATE}" \
            --metric "${METRIC}" \
            --agg mean \
            --save "${SAVE_BASE}" \
            --all-formats \
            --no-show \
            "${PY_COMMON_ARGS[@]}" || true
        done
      done
    done
  done
}

run_batch_compare_eventrates() {
  echo ">>> [batch_compare_eventrates] Compare event-rates (throughput/latency) per query_id+HW ..."
  local BATCH_METRICS=(throughput_50 endtoend_latency)
  for QID in "${QUERY_IDS[@]}"; do
    for HW in "${HARDWARES[@]}"; do
      for METRIC in "${BATCH_METRICS[@]}"; do
        python "${PLOT_SCRIPT}" \
          --csv "${CSV}" \
          --plot batch_compare_eventrates \
          --frameworks "${FRAMEWORKS[@]}" \
          --query "${QID}" \
          --hardware_type "${HW}" \
          --metric "${METRIC}" \
          --agg mean \
          --out_dir "${OUT_DIR}" \
          --all-formats \
          --no-show \
          "${PY_COMMON_ARGS[@]}" || true
      done
    done
  done
}

run_batch_all_metrics_eventrates() {
  echo ">>> [batch_all_metrics_eventrates] BIG batch: all metrics × all rates per query_id+HW ..."
  for QID in "${QUERY_IDS[@]}"; do
    for HW in "${HARDWARES[@]}"; do
      python "${PLOT_SCRIPT}" \
        --csv "${CSV}" \
        --plot batch_all_metrics_eventrates \
        --frameworks "${FRAMEWORKS[@]}" \
        --query "${QID}" \
        --hardware_type "${HW}" \
        --metric throughput_50 \
        --agg mean \
        --out_dir "${OUT_DIR}" \
        --all-formats \
        --no-show \
        "${PY_COMMON_ARGS[@]}" || true
    done
  done
}

run_eventrate_single() {
  echo ">>> [eventrate_single] Event-rate sweep per query_id+HW+parallelism (single framework) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for HW in "${HARDWARES[@]}"; do
      local PARS
      PARS=$(parallelisms_for_hw "${HW}")
      for PAR in ${PARS}; do
        for FW in "${FRAMEWORKS[@]}"; do
          for METRIC in "${CORE_METRICS[@]}"; do
            SAVE_BASE="${OUT_DIR}/eventrate_single/${Q_SAFE}/${FW}/${HW}/par${PAR}_$(safe "${METRIC}")"
            python "${PLOT_SCRIPT}" \
              --csv "${CSV}" \
              --plot eventrate_single \
              --framework "${FW}" \
              --query "${QID}" \
              --hardware_type "${HW}" \
              --parallelism_avg "${PAR}" \
              --metric "${METRIC}" \
              --agg mean \
              --save "${SAVE_BASE}" \
              --all-formats \
              --no-show \
              "${PY_COMMON_ARGS[@]}" || true
          done
        done
      done
    done
  done
}

run_eventrate_compare_fw() {
  echo ">>> [eventrate_compare_fw] Event-rate sweep per query_id+HW+parallelism (Flink vs Storm) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for HW in "${HARDWARES[@]}"; do
      local PARS
      PARS=$(parallelisms_for_hw "${HW}")
      for PAR in ${PARS}; do
        for METRIC in "${CORE_METRICS[@]}"; do
          SAVE_BASE="${OUT_DIR}/eventrate_compare_fw/${Q_SAFE}/${HW}/par${PAR}_$(safe "${METRIC}")"
          python "${PLOT_SCRIPT}" \
            --csv "${CSV}" \
            --plot eventrate_compare_fw \
            --frameworks "${FRAMEWORKS[@]}" \
            --query "${QID}" \
            --hardware_type "${HW}" \
            --parallelism_avg "${PAR}" \
            --metric "${METRIC}" \
            --agg mean \
            --save "${SAVE_BASE}" \
            --all-formats \
            --no-show \
            "${PY_COMMON_ARGS[@]}" || true
        done
      done
    done
  done
}

run_app_scaling_all_hw() {
  echo ">>> [app_scaling_all_hw] All hardware + both frameworks in one plot (x=parallelism) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for RATE in "${PRODUCER_RATES[@]}"; do
      for METRIC in "${CORE_METRICS[@]}"; do
        SAVE_BASE="${OUT_DIR}/app_scaling_all_hw/${Q_SAFE}/rate${RATE}_$(safe "${METRIC}")"
        python "${PLOT_SCRIPT}" \
          --csv "${CSV}" \
          --plot app_scaling_all_hw \
          --frameworks "${FRAMEWORKS[@]}" \
          --query "${QID}" \
          --producer_rate "${RATE}" \
          --metric "${METRIC}" \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show \
          "${PY_COMMON_ARGS[@]}" || true
      done
    done
  done
}

run_app_eventrate_all_hw() {
  echo ">>> [app_eventrate_all_hw] All hardware + both frameworks in one plot (x=event-rate, fixed par) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for METRIC in "${CORE_METRICS[@]}"; do
      SAVE_BASE="${OUT_DIR}/app_eventrate_all_hw/${Q_SAFE}/par${REP_PARALLELISM}_$(safe "${METRIC}")"
      python "${PLOT_SCRIPT}" \
        --csv "${CSV}" \
        --plot app_eventrate_all_hw \
        --frameworks "${FRAMEWORKS[@]}" \
        --query "${QID}" \
        --parallelism_avg "${REP_PARALLELISM}" \
        --metric "${METRIC}" \
        --agg mean \
        --save "${SAVE_BASE}" \
        --all-formats \
        --no-show \
        "${PY_COMMON_ARGS[@]}" || true
    done
  done
}

run_cpu_efficiency_across_hw() {
  echo ">>> [cpu_efficiency_across_hw] Efficiency across hardware (cpu/network efficiency) ..."
  local EFF_BASES=("Smart Grid" "Click Analytics" "Ad Analytics")
  local EFF_RATE=1000000
  local PAR_MAP_JSON='{"m510":40,"c6525-25g":80,"c6525-100g":96}'

  for BASE in "${EFF_BASES[@]}"; do
    for QID in "${QUERY_IDS[@]}"; do
      [[ "${QID}" == "${BASE}_"* ]] || continue
      local Q_SAFE
      Q_SAFE=$(safe "${QID}")
      for METRIC in "${EFFICIENCY_METRICS[@]}"; do
        SAVE_BASE="${OUT_DIR}/efficiency_across_hw/${Q_SAFE}/rate${EFF_RATE}_$(safe "${METRIC}")"
        python "${PLOT_SCRIPT}" \
          --csv "${CSV}" \
          --plot cpu_efficiency_across_hw \
          --frameworks "${FRAMEWORKS[@]}" \
          --query "${QID}" \
          --producer_rate "${EFF_RATE}" \
          --metric "${METRIC}" \
          --par_map "${PAR_MAP_JSON}" \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show \
          "${PY_COMMON_ARGS[@]}" || true
      done
    done
  done
}

run_workload_fw_hw_parallelism() {
  echo ">>> [workload_fw_hw_parallelism] Full interaction grid (workload×FW×HW×par, many plots) ..."
  for QID in "${QUERY_IDS[@]}"; do
    local Q_SAFE
    Q_SAFE=$(safe "${QID}")
    for RATE in "${PRODUCER_RATES[@]}"; do
      for METRIC in "${CORE_METRICS[@]}"; do
        SAVE_BASE="${OUT_DIR}/workload_fw_hw_parallelism/${Q_SAFE}/rate${RATE}_$(safe "${METRIC}")"
        python "${PLOT_SCRIPT}" \
          --csv "${CSV}" \
          --plot workload_fw_hw_parallelism \
          --frameworks "${FRAMEWORKS[@]}" \
          --query "${QID}" \
          --producer_rate "${RATE}" \
          --metric "${METRIC}" \
          --agg mean \
          --save "${SAVE_BASE}" \
          --all-formats \
          --no-show \
          "${PY_COMMON_ARGS[@]}" || true
      done
    done
  done
}

# -----------------------------
# Dispatcher
# -----------------------------
dispatch_plot() {
  local key="$1"
  case "$key" in
    intro_hw_scaling_multi) run_intro_hw_scaling_multi ;;
    intro_hw_scaling_multi_fw) run_intro_hw_scaling_multi_fw ;;
    batch_low_med_high_parallelism_across_hw) run_batch_low_med_high_parallelism_across_hw ;;
    batch_low_med_high_eventrate_across_hw) run_batch_low_med_high_eventrate_across_hw ;;
    dual_axis_tp_latency) run_dual_axis_tp_latency ;;
    queries_single_framework) run_queries_single_framework ;;
    queries_compare_frameworks) run_queries_compare_frameworks ;;
    scaling_single) run_scaling_single ;;
    scaling_compare_fw) run_scaling_compare_fw ;;
    batch_compare_eventrates) run_batch_compare_eventrates ;;
    batch_all_metrics_eventrates) run_batch_all_metrics_eventrates ;;
    eventrate_single) run_eventrate_single ;;
    eventrate_compare_fw) run_eventrate_compare_fw ;;
    app_scaling_all_hw) run_app_scaling_all_hw ;;
    app_eventrate_all_hw) run_app_eventrate_all_hw ;;
    cpu_efficiency_across_hw) run_cpu_efficiency_across_hw ;;
    workload_fw_hw_parallelism) run_workload_fw_hw_parallelism ;;
    *)
      echo "Unknown plot key: $key"
      echo "Run './make_all_plots.sh help' or run without args to see the menu."
      exit 1
      ;;
  esac
}

# ===================================================================
# Main
# ===================================================================
init_menu_maps
print_banner

echo ">>> Global plotting options:"
echo "    chart=${CHART_TYPE}, y-scale=${Y_SCALE}, show-values=${SHOW_VALUES}"
echo "    mpl-style=${MPL_STYLE}, latex-text=${LATEX_TEXT}, hw-palette=${HW_PALETTE}"
echo "    ymin=${YMIN:-<auto/none>}, ymax=${YMAX:-<auto/none>}"
echo "    max-points=${MAX_POINTS:-<none>}, point-strategy=${POINT_STRATEGY}"
echo "    show-group-values=${SHOW_GROUP_VALUES}"
echo "    LMH_PAR_MAP_JSON=${LMH_PAR_MAP_JSON}"
echo "    LMH_RATE_MAP_JSON=${LMH_RATE_MAP_JSON}"
echo

# Help mode
if [[ "${1:-}" == "help" || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  print_menu
  echo
  echo "To get details about one option:"
  echo "  ./make_all_plots.sh info 9"
  exit 0
fi

# Info mode: ./make_all_plots.sh info <id>
if [[ "${1:-}" == "info" ]]; then
  if [[ $# -lt 2 ]]; then
    echo "Usage: ./make_all_plots.sh info <id>"
    exit 1
  fi
  print_info "$2"
  exit 0
fi

require_file "${CSV}"
require_file "${PLOT_SCRIPT}"

# Discover query_ids once for all plots (needed by most)
discover_query_ids

# Selection parsing (interactive if no args)
SELECTIONS=""
if [[ $# -eq 0 ]]; then
  print_menu
  echo
  read -r -p "#? " SELECTIONS
else
  SELECTIONS="$*"
fi

SELECTIONS="${SELECTIONS:-all}"
SELECTIONS="${SELECTIONS//,/ }"

PLOTS_TO_RUN=()
for tok in $SELECTIONS; do
  case "$tok" in
    q|quit|exit) exit 0 ;;
    all) PLOTS_TO_RUN=("all") ;;
    *)
      if [[ "$tok" =~ ^[0-9]+$ ]]; then
        key="${ID_TO_KEY[$tok]:-}"
        [[ -z "$key" ]] && { echo "Unknown option: $tok"; exit 1; }
        PLOTS_TO_RUN+=("$key")
      else
        # allow direct plot keys too
        PLOTS_TO_RUN+=("$tok")
      fi
      ;;
  esac
done

# Expand "all"
for x in "${PLOTS_TO_RUN[@]}"; do
  if [[ "$x" == "all" ]]; then
    PLOTS_TO_RUN=("${ALL_KEYS[@]}")
    break
  fi
done

echo
echo ">>> Will run ${#PLOTS_TO_RUN[@]} plot function(s):"
printf '    - %s\n' "${PLOTS_TO_RUN[@]}"
echo

for key in "${PLOTS_TO_RUN[@]}"; do
  dispatch_plot "$key"
done

echo
echo ">>> Done. Plots written under '${OUT_DIR}'"
