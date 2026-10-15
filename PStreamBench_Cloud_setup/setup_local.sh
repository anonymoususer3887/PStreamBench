#!/bin/bash
# Local (single-machine, no CloudLab) setup for RisingWave benchmark testing.
#
# Mirrors what pre_conditioning.sh / install_risingwave_only.sh /
# download_datasets.sh do for a real CloudLab node, but targets a local dev
# machine (macOS/Homebrew) instead: no sudo, no /home/playground (which
# requires root to create and, on CloudLab, turned out to be an ephemeral
# mount that's wiped on every node reboot -- see PSTREAMBENCH_DATA_DIR
# below for how the producer script avoids depending on that path at all).
#
# Idempotent: safe to re-run. Each step checks whether it's already done
# before doing it again, so re-running after an update just fills in
# whatever changed.
#
# IMPORTANT (learned the hard way, 2026-07-27): keep the machine awake for
# the duration of testing. A laptop sleep of even a few minutes causes
# Kafka's and RisingWave's internal heartbeat/worker-liveness timers to
# expire on wake, cascading into "AllBrokersDown" / "Deleted expired
# worker" failures that kill the whole local stack. This script starts
# `caffeinate` for you; if you're driving things manually instead, run
# `caffeinate -d -i -s &` yourself first.
#
# Usage:
#   ./setup_local.sh              # full setup + start everything
#   ./setup_local.sh --data-only  # just (re)download datasets, skip services
#   ./setup_local.sh --stop       # stop everything (Kafka/RisingWave/Prometheus/node_exporter/caffeinate)

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${PSTREAMBENCH_DATA_DIR:-$HOME/pstreambench_data}"
KAFKA_LOG="/tmp/pstreambench_local_kafka.log"
RISINGWAVE_LOG="/tmp/pstreambench_local_risingwave.log"
PROMETHEUS_LOG="/tmp/pstreambench_local_prometheus.log"
NODE_EXPORTER_LOG="/tmp/pstreambench_local_node_exporter.log"
PROMETHEUS_CONFIG="/tmp/pstreambench_local_prometheus.yml"
PROMETHEUS_DATA_DIR="/tmp/pstreambench_local_prom_data"

log() { echo "[setup_local] $*"; }

if [ "$1" = "--stop" ]; then
  log "Stopping local RisingWave, Kafka, Prometheus, node_exporter, caffeinate..."
  pkill -f "risingwave single-node" 2>/dev/null || true
  pkill -f "kafka.Kafka" 2>/dev/null || true
  pkill -f "prometheus --config.file=$PROMETHEUS_CONFIG" 2>/dev/null || true
  pkill -f "node_exporter" 2>/dev/null || true
  pkill -f "caffeinate -d -i -s" 2>/dev/null || true
  log "Done."
  exit 0
fi

if ! pgrep -f "caffeinate -d -i -s" >/dev/null 2>&1; then
  log "Starting caffeinate to prevent sleep during testing (see note above)..."
  caffeinate -d -i -s &
  disown
fi

# --- 1. Homebrew dependencies -----------------------------------------
log "Checking Homebrew dependencies (risingwave, kafka, libpq, kaggle)..."

if ! command -v risingwave >/dev/null 2>&1; then
  log "Installing RisingWave..."
  brew tap risingwavelabs/risingwave
  brew install risingwave
else
  log "RisingWave already installed ($(risingwave --version))."
fi

if ! command -v kafka-server-start >/dev/null 2>&1; then
  log "Installing Kafka..."
  brew install kafka
else
  log "Kafka already installed."
fi

if ! command -v psql >/dev/null 2>&1; then
  log "Installing libpq (for psql)..."
  brew install libpq
  brew link --force libpq
fi
export PATH="/opt/homebrew/opt/libpq/bin:$PATH"

if ! command -v prometheus >/dev/null 2>&1 && [ ! -x /opt/homebrew/opt/prometheus/bin/prometheus ]; then
  log "Installing Prometheus..."
  brew install prometheus
fi

if [ ! -x /opt/homebrew/opt/node_exporter/bin/node_exporter ]; then
  log "Installing node_exporter..."
  brew install node_exporter
fi

# Always installed into a dedicated venv, never system/global pip -- see
# feedback_use_venv_for_python project memory.
PY_VENV="${PSTREAMBENCH_PY_VENV:-$HOME/.pstreambench_venv}"
if [ ! -x "$PY_VENV/bin/kaggle" ]; then
  log "Creating venv at $PY_VENV and installing kaggle CLI/package into it..."
  python3 -m venv "$PY_VENV"
  "$PY_VENV/bin/pip" install --quiet --upgrade pip
  "$PY_VENV/bin/pip" install --quiet kaggle
fi
KAGGLE_BIN="$PY_VENV/bin/kaggle"

# --- 2. Kaggle credentials ----------------------------------------------
mkdir -p ~/.kaggle
if [ ! -f ~/.kaggle/kaggle.json ]; then
  log "Copying kaggle.json credentials..."
  cp "$SCRIPT_DIR/kaggle.json" ~/.kaggle/kaggle.json
fi
chmod 600 ~/.kaggle/kaggle.json

# --- 3. Data directory + datasets ---------------------------------------
mkdir -p "$DATA_DIR"
log "Data directory: $DATA_DIR"

# dataset_name -> expected final filename in DATA_DIR (see
# download_datasets.sh for the authoritative kaggle-slug/archive-name list;
# kept in sync here since local testing needs the same files the CloudLab
# producer scripts expect).
declare -a DATASETS=(
  "wordcount:word-count.tar.xz:word-count.txt"
  "sentimentanalysis:sentimentAnalysis_tweetstream.tar.xz:sentimentAnalysis_tweetstream.json"
  "spikedetection:spikeDetection_sensors.tar.xz:spikeDetection_sensors.dat"
  "bargainindex:stocks.tar.xz:stocks.csv"
  "machineoutlier:outlier.tar.xz:outlier.csv"
  "trafficmonitoring:taxi-traces.tar.xz:taxi-traces.csv"
  "clickstream:click-stream.tar.xz:click-stream.json"
  "smartgrid:smart-grid.tar.xz:smart-grid.csv"
  "tpchdataset:tpc_h.tar.xz:tpc_h.csv"
  "adanalytics:adAnalytics.tar.xz:adAnalytics.dat"
  "googlecloudmonitoring:googleCloudMonitoring.tar.xz:googleCloudMonitoring.csv"
  "httplogprocessing:http.tar.xz:http.log"
  "linearroad:lrb.tar.xz:lrb.csv"
  "trendingtopics:ttopic.tar.xz:ttopic.csv"
)

ZIP_DIR="$DATA_DIR/.zip"
mkdir -p "$ZIP_DIR"

for entry in "${DATASETS[@]}"; do
  slug="${entry%%:*}"
  rest="${entry#*:}"
  archive="${rest%%:*}"
  final="${rest#*:}"

  if [ -f "$DATA_DIR/$final" ]; then
    log "  [ok] $final already present, skipping download."
    continue
  fi

  log "  Downloading $slug ..."
  (
    cd "$ZIP_DIR"
    "$KAGGLE_BIN" datasets download -d "shubhamsumalya/$slug" -o
    unzip -o -q "$slug.zip"
    tar -xf "$archive" -C "$DATA_DIR"
  )
  if [ -f "$DATA_DIR/$final" ]; then
    log "  [done] $final"
  else
    log "  [WARN] expected $final not found after extracting $slug -- check archive contents manually."
  fi
done

# Spike Detection uses PStreamBench_controller/data.txt (the raw Intel
# Berkeley Lab dataset, 8 columns incl. an epoch counter) as its
# authoritative source, per explicit direction -- NOT the Kaggle
# "spikedetection" dataset (a different, already-preprocessed 7-column
# export with no epoch counter, confirmed 2026-07-27 to have a different
# schema entirely). Always (re)derive spikeDetection_sensors.dat from
# data.txt here, overwriting whatever the Kaggle download step above
# produced for it.
DATA_TXT="$SCRIPT_DIR/../PStreamBench_controller/data.txt"
if [ -f "$DATA_TXT" ]; then
  log "Deriving spikeDetection_sensors.dat from data.txt (8-column format, per explicit direction)..."
  awk 'NF==8' "$DATA_TXT" > "$DATA_DIR/spikeDetection_sensors.dat"
else
  log "[WARN] $DATA_TXT not found -- spikeDetection_sensors.dat left as whatever the Kaggle download produced (wrong schema for spike_detection.sql's field offsets)."
fi

if [ "$1" = "--data-only" ]; then
  log "Data-only mode requested, skipping service startup."
  exit 0
fi

# --- 4. Start Kafka -------------------------------------------------------
if pgrep -f "kafka.Kafka" >/dev/null 2>&1; then
  log "Kafka already running."
else
  log "Starting local Kafka (KRaft mode)..."
  nohup /opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties \
    > "$KAFKA_LOG" 2>&1 &
  disown
  sleep 5
  if pgrep -f "kafka.Kafka" >/dev/null 2>&1; then
    log "Kafka started (log: $KAFKA_LOG)."
  else
    log "[WARN] Kafka did not start -- check $KAFKA_LOG"
  fi
fi

# --- 5. Start RisingWave (single-node) ------------------------------------
if pgrep -f "risingwave single-node" >/dev/null 2>&1; then
  log "RisingWave already running."
else
  log "Starting local RisingWave (single-node mode)..."
  nohup risingwave single-node > "$RISINGWAVE_LOG" 2>&1 &
  disown
  sleep 5
  if psql -h 0.0.0.0 -p 4566 -d dev -U root -c "SELECT 1;" >/dev/null 2>&1; then
    log "RisingWave started and responding (log: $RISINGWAVE_LOG)."
  else
    log "[WARN] RisingWave not responding yet -- check $RISINGWAVE_LOG"
  fi
fi

# --- 6. Start node_exporter + Prometheus (scraping RisingWave + node_exporter) ---
# RisingWave's single-node mode fuses meta/frontend/compute/compactor
# metrics onto ONE port -- confirmed live (2026-07-26) to be 1260 (the
# individual default ports of 1250/2222/1222 all conflict when running as
# one process, so RisingWave falls back to the first free port and logs a
# WARN about it -- check $RISINGWAVE_LOG for "Currently listening on" if
# this ever changes).
RW_METRICS_PORT=1260

if pgrep -f "node_exporter" >/dev/null 2>&1; then
  log "node_exporter already running."
else
  log "Starting node_exporter..."
  nohup /opt/homebrew/opt/node_exporter/bin/node_exporter > "$NODE_EXPORTER_LOG" 2>&1 &
  disown
  sleep 2
fi

cat > "$PROMETHEUS_CONFIG" <<EOF
global:
  scrape_interval: 5s
  evaluation_interval: 5s
scrape_configs:
  - job_name: "risingwave"
    static_configs:
      - targets: ["localhost:${RW_METRICS_PORT}"]
  - job_name: "node_exporter"
    static_configs:
      - targets: ["localhost:9100"]
EOF

if pgrep -f "prometheus --config.file=$PROMETHEUS_CONFIG" >/dev/null 2>&1; then
  log "Prometheus already running."
else
  log "Starting Prometheus..."
  mkdir -p "$PROMETHEUS_DATA_DIR"
  nohup /opt/homebrew/opt/prometheus/bin/prometheus \
    --config.file="$PROMETHEUS_CONFIG" \
    --storage.tsdb.path="$PROMETHEUS_DATA_DIR" \
    --web.listen-address=:9090 \
    > "$PROMETHEUS_LOG" 2>&1 &
  disown
  sleep 5
fi

log ""
log "=== Ready ==="
log "Kafka:       localhost:9092"
log "RisingWave:  psql -h 0.0.0.0 -p 4566 -d dev -U root"
log "Prometheus:  http://localhost:9090  (scraping RisingWave:$RW_METRICS_PORT + node_exporter:9100)"
log "Data dir:    $DATA_DIR  (export PSTREAMBENCH_DATA_DIR=$DATA_DIR for custom_producer.py)"
log ""
log "KNOWN GAPS vs. the real CloudLab setup (not bugs, just not wired up locally):"
log "  - Kafka JMX exporter isn't attached to this local broker, so"
log "    kafka_event_rate/kafka_throughput metrics will be null. The remote"
log "    setup scripts already configure this correctly (jmx_prometheus_javaagent)."
log "  - node_exporter's memory/network metric NAMES differ on macOS vs."
log "    Linux (e.g. node_memory_free_bytes vs. node_memory_MemFree_bytes,"
log "    en0 vs. eno1) -- getprom_risingwave.py's hardware queries are"
log "    written for the real (Linux) CloudLab nodes, so node_memory/"
log "    node_network_out will show null locally even though node_exporter"
log "    itself is running -- this is expected, not a bug to fix here."
log "  - RisingWave's own actorMetrics (barrier latency) and operatorMetrics"
log "    (per-fragment throughput/selectivity) ARE fully validated locally"
log "    with zero nulls -- these were the metrics actually breaking on the"
log "    remote cluster this session, and this local setup is sufficient to"
log "    catch regressions in them."
log ""
log "To stop:    $0 --stop"
