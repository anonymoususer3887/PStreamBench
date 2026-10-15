#!/bin/bash
# Local (single-machine, no CloudLab) setup for Flink benchmark testing.
# Counterpart of setup_local.sh (RisingWave) for Apache Flink -- same
# idempotent-step philosophy, reuses the SAME local Kafka + datasets +
# caffeinate handling, but starts one local JobManager + one local
# TaskManager instead of a RisingWave single-node process.
#
# Unlike RisingWave, no workload porting is needed at all here -- the 14
# DSPBench workloads are already compiled into
# PStreamBench_Cloud_setup/dsp_jobs-1.0-SNAPSHOT.jar (the same jar the real
# CloudLab deployment uses), so this script is pure infrastructure: get a
# local Flink JobManager+TaskManager+Kafka+Prometheus stack running and
# able to submit that same jar.
#
# Key differences from the real CloudLab setup (utils/playbook/tasks/flink):
#   - Flink 1.16.2 requires Java 8/11 (NOT 17 -- Flink added only
#     experimental Java 17 support in 1.18+); this script installs and uses
#     openjdk@11 specifically for Flink's own JVMs via JAVA_HOME, without
#     touching the system default `java` (which Kafka's brew formula is
#     fine running under a newer JDK).
#   - jobmanager.memory.process.size/taskmanager.memory.process.size are
#     64gb in the CloudLab sample conf (sized for real CloudLab nodes) --
#     dropped to 2gb here for a laptop.
#   - JobManager and TaskManager are two separate local processes but share
#     one flink-conf.yaml; both would try to bind the Prometheus reporter
#     to the same port (9250) if given a fixed port, so this uses a small
#     port RANGE (9250-9260) instead -- Flink's PrometheusReporter grabs
#     the first free port in the range per-process, exactly the local
#     multi-process-one-host case this exists for.
#
# Usage:
#   ./setup_local_flink.sh          # full setup + start JobManager/TaskManager
#   ./setup_local_flink.sh --stop   # stop Flink processes started by this script

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_DIR="${PSTREAMBENCH_FLINK_DIR:-$HOME/pstreambench_data/.flink}"
FLINK_VERSION="1.16.2"
FLINK_HOME="$FLINK_DIR/flink-$FLINK_VERSION"
JAVA11_HOME="$(brew --prefix openjdk@11 2>/dev/null || echo /opt/homebrew/opt/openjdk@11)"
JM_LOG="/tmp/pstreambench_local_flink_jm.log"
TM_LOG="/tmp/pstreambench_local_flink_tm.log"

log() { echo "[setup_local_flink] $*"; }

if [ "$1" = "--stop" ]; then
  log "Stopping local Flink JobManager/TaskManager..."
  JAVA_HOME="$JAVA11_HOME" "$FLINK_HOME/bin/taskmanager.sh" stop-all 2>/dev/null || true
  JAVA_HOME="$JAVA11_HOME" "$FLINK_HOME/bin/jobmanager.sh" stop-all 2>/dev/null || true
  pkill -f "flink-$FLINK_VERSION.*TaskManagerRunner" 2>/dev/null || true
  pkill -f "flink-$FLINK_VERSION.*StandaloneSessionClusterEntrypoint" 2>/dev/null || true
  log "Done."
  exit 0
fi

# --- 0. Java 11 for Flink's own JVMs (kept separate from system default) ---
if [ ! -x "$JAVA11_HOME/bin/java" ]; then
  log "Installing openjdk@11 (required by Flink $FLINK_VERSION -- Flink 1.16 doesn't support Java 17)..."
  brew install openjdk@11
fi
log "Flink will run under: $("$JAVA11_HOME/bin/java" -version 2>&1 | head -1)"

# --- 1. Download + lay out Flink (idempotent) ------------------------------
mkdir -p "$FLINK_DIR"
if [ ! -d "$FLINK_HOME" ]; then
  log "Downloading Flink $FLINK_VERSION..."
  curl -sL "https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz" \
    -o "$FLINK_DIR/flink.tgz"
  tar zxf "$FLINK_DIR/flink.tgz" -C "$FLINK_DIR"
else
  log "Flink $FLINK_VERSION already present at $FLINK_HOME."
fi

log "Copying dsp_jobs jar + native-metrics dependency jars..."
cp "$SCRIPT_DIR/dsp_jobs-1.0-SNAPSHOT.jar" "$FLINK_HOME/bin/"
cp "$SCRIPT_DIR/../PStreamBench_controller/dsp_be/infra/flink_files/jna-5.10.0.jar" "$FLINK_HOME/lib/"
cp "$SCRIPT_DIR/../PStreamBench_controller/dsp_be/infra/flink_files/jna-platform-5.10.0.jar" "$FLINK_HOME/lib/"
cp "$SCRIPT_DIR/../PStreamBench_controller/dsp_be/infra/flink_files/oshi-core-6.1.5.jar" "$FLINK_HOME/lib/"
cp "$SCRIPT_DIR/roads.geojson" "$FLINK_HOME/bin/" 2>/dev/null || true

# Flink's Prometheus reporter is bundled as an optional plugin, not in lib/
# by default -- confirmed needed live (2026-07-27): without this, both
# JobManager and TaskManager metrics silently have no Prometheus endpoint
# at all (no error, no port opened) since metrics.reporter.prom.factory.class
# can't be found on the classpath.
mkdir -p "$FLINK_HOME/plugins/metrics-prometheus"
if [ ! -f "$FLINK_HOME/plugins/metrics-prometheus/flink-metrics-prometheus-$FLINK_VERSION.jar" ]; then
  cp "$FLINK_HOME/opt/flink-metrics-prometheus-$FLINK_VERSION.jar" "$FLINK_HOME/plugins/metrics-prometheus/"
fi

# --- 2. Local flink-conf.yaml (small memory sizes, port range for reporter) ---
cat > "$FLINK_HOME/conf/flink-conf.yaml" << EOF
jobmanager.rpc.address: localhost
jobmanager.rpc.port: 6123
jobmanager.bind-host: 0.0.0.0
jobmanager.memory.process.size: 2gb
taskmanager.bind-host: 0.0.0.0
taskmanager.host: localhost
taskmanager.memory.process.size: 2gb
taskmanager.numberOfTaskSlots: 4
parallelism.default: 1
jobmanager.execution.failover-strategy: region
rest.port: 8086
rest.address: localhost
rest.bind-address: 0.0.0.0
blob.server.port: 6124
query.server.port: 6125
metrics.system-resource: true
metrics.system-resource-probing-interval: 500
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9250-9260
state.backend: filesystem
state.checkpoints.dir: file:///tmp/flink-checkpoints-directory
state.savepoints.dir: file:///tmp/flink-savepoints-directory
EOF

# --- 3. Start JobManager + TaskManager (idempotent) ------------------------
if pgrep -f "flink-$FLINK_VERSION.*StandaloneSessionClusterEntrypoint" >/dev/null 2>&1; then
  log "JobManager already running."
else
  log "Starting local JobManager..."
  JAVA_HOME="$JAVA11_HOME" nohup "$FLINK_HOME/bin/jobmanager.sh" start-foreground > "$JM_LOG" 2>&1 &
  disown
  sleep 5
fi

if pgrep -f "flink-$FLINK_VERSION.*TaskManagerRunner" >/dev/null 2>&1; then
  log "TaskManager already running."
else
  log "Starting local TaskManager..."
  JAVA_HOME="$JAVA11_HOME" nohup "$FLINK_HOME/bin/taskmanager.sh" start-foreground > "$TM_LOG" 2>&1 &
  disown
  sleep 5
fi

if curl -sf "http://localhost:8086/overview" >/dev/null 2>&1; then
  log "Flink cluster responding on REST port 8086."
else
  log "[WARN] Flink REST API not responding yet -- check $JM_LOG"
fi

log ""
log "=== Ready ==="
log "Flink REST/Web UI: http://localhost:8086"
log "Flink dashboard job list: http://localhost:8086/overview"
log "Prometheus targets (add to your Prometheus config): localhost:9250, localhost:9251"
log "  (JobManager and TaskManager pick different ports from the 9250-9260"
log "  range -- check $JM_LOG and $TM_LOG for 'Started PrometheusReporter HTTP server on port NNNN' if unsure)."
log ""
log "Submit a job the same way the real cluster does, e.g. Word Count:"
log "  JAVA_HOME=$JAVA11_HOME $FLINK_HOME/bin/flink run \\"
log "    -c com.kom.dsp.wordcount.WordCountJob $FLINK_HOME/bin/dsp_jobs-1.0-SNAPSHOT.jar \\"
log "    -parallelism 1 -mode Kafka -input WordCountIn -output WordCountOut \\"
log "    -query 1 -kafka-server localhost:9092 -size 0 -slide 0 \\"
log "    -waitTimeToCancel 1 -lateness 0 -popularityThreshold 0"
log ""
log "Requires local Kafka running first -- see setup_local.sh (RisingWave's"
log "local setup script also starts a plain local Kafka broker you can reuse)."
log ""
log "To stop: $0 --stop"
