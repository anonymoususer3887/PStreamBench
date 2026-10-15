#!/bin/bash
# Local (single-machine, no CloudLab) setup for Apache Spark (Structured
# Streaming, Standalone mode) benchmark testing. Counterpart of
# setup_local.sh / setup_local_flink.sh / setup_local_storm.sh for Spark.
#
# Like Storm's dsp_storm_jobs, Spark's job jar is NOT pre-baked into this
# repo -- it's a Maven project at
# PStreamBench_workload_generator/spark/dsp_spark_jobs with a Makefile
# (`make build`) that runs `mvn package` and copies the result to
# PStreamBench_controller/dsp_be/infra/spark_files/. This script builds it
# if missing (a `target/dsp_spark_jobs-1.0-SNAPSHOT.jar` was actually
# already present from a prior build when this script was written).
#
# Spark's own PrometheusServlet (native, Spark 3.x) is used for metrics --
# no bolt-on exporter needed (unlike Storm's storm_exporter.py). Confirmed
# reachable at :4040/metrics/prometheus/ (driver/application) and
# :8087/metrics/master/prometheus/ (master) once a job is running.
#
# Usage:
#   ./setup_local_spark.sh          # full setup + start master/worker
#   ./setup_local_spark.sh --stop   # stop everything this script started

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROLLER_DIR="$SCRIPT_DIR/../PStreamBench_controller/dsp_be"
SPARK_JOBS_DIR="$SCRIPT_DIR/../PStreamBench_workload_generator/spark/dsp_spark_jobs"
SPARK_JAR="$CONTROLLER_DIR/infra/spark_files/dsp_spark_jobs-1.0-SNAPSHOT.jar"
SPARK_DIR="${PSTREAMBENCH_SPARK_DIR:-$HOME/pstreambench_data/.spark}"
SPARK_VERSION="3.5.1"
SPARK_HOME="$SPARK_DIR/spark-$SPARK_VERSION-bin-hadoop3"
JAVA11_HOME="$(brew --prefix openjdk@11 2>/dev/null || echo /opt/homebrew/opt/openjdk@11)"
MASTER_LOG="/tmp/pstreambench_local_spark_master.log"
WORKER_LOG="/tmp/pstreambench_local_spark_worker.log"

log() { echo "[setup_local_spark] $*"; }

if [ "$1" = "--stop" ]; then
  log "Stopping local Spark master/worker..."
  JAVA_HOME="$JAVA11_HOME" "$SPARK_HOME/sbin/stop-worker.sh" 2>/dev/null || true
  JAVA_HOME="$JAVA11_HOME" "$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || true
  log "Done."
  exit 0
fi

# --- 0. Java 11 (matches dsp_spark_jobs' pom.xml target.java.version) ------
if [ ! -x "$JAVA11_HOME/bin/java" ]; then
  log "Installing openjdk@11..."
  brew install openjdk@11
fi

# --- 1. Build the job jar if missing (Maven, not pip -- no venv needed) ----
if [ ! -f "$SPARK_JAR" ]; then
  log "dsp_spark_jobs jar not found -- building it (mvn package)..."
  (cd "$SPARK_JOBS_DIR" && JAVA_HOME="$JAVA11_HOME" mvn -q package)
  cp "$SPARK_JOBS_DIR/target/dsp_spark_jobs-1.0-SNAPSHOT.jar" "$SPARK_JAR"
else
  log "dsp_spark_jobs jar already built at $SPARK_JAR."
fi

# --- 2. Download + lay out Spark (idempotent) ------------------------------
mkdir -p "$SPARK_DIR"
if [ ! -d "$SPARK_HOME" ]; then
  log "Downloading Spark $SPARK_VERSION..."
  curl -sL "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz" \
    -o "$SPARK_DIR/spark.tgz"
  tar zxf "$SPARK_DIR/spark.tgz" -C "$SPARK_DIR"
else
  log "Spark $SPARK_VERSION already present at $SPARK_HOME."
fi

cp "$SPARK_JAR" "$SPARK_HOME/bin/"
cp "$SCRIPT_DIR/spark-metrics.properties" "$SPARK_HOME/conf/metrics.properties"

cat > "$SPARK_HOME/conf/spark-env.sh" << EOF
export SPARK_MASTER_WEBUI_PORT=8087
export SPARK_MASTER_HOST=localhost
export SPARK_WORKER_CORES=4
export SPARK_LOCAL_IP=localhost
EOF
chmod +x "$SPARK_HOME/conf/spark-env.sh"

cat > "$SPARK_HOME/conf/spark-defaults.conf" << EOF
spark.master                     spark://localhost:7077
spark.ui.prometheus.enabled                              true
spark.metrics.conf.*.sink.prometheusServlet.class         org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheusServlet.path          /metrics/prometheus
spark.metrics.conf.master.sink.prometheusServlet.path      /metrics/master/prometheus
spark.metrics.conf.applications.sink.prometheusServlet.path /metrics/applications/prometheus
spark.sql.shuffle.partitions     4
EOF

# --- 3. Start master + worker (idempotent) ---------------------------------
if pgrep -f "org.apache.spark.deploy.master.Master" >/dev/null 2>&1; then
  log "Spark master already running."
else
  log "Starting Spark master..."
  JAVA_HOME="$JAVA11_HOME" "$SPARK_HOME/sbin/start-master.sh" > "$MASTER_LOG" 2>&1
  sleep 5
fi

if pgrep -f "org.apache.spark.deploy.worker.Worker" >/dev/null 2>&1; then
  log "Spark worker already running."
else
  log "Starting Spark worker..."
  JAVA_HOME="$JAVA11_HOME" "$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077 > "$WORKER_LOG" 2>&1
  sleep 5
fi

if curl -sf "http://localhost:8087/json/" >/dev/null 2>&1; then
  log "Spark master responding on :8087."
else
  log "[WARN] Spark master not responding yet -- check $MASTER_LOG"
fi

log ""
log "=== Ready ==="
log "Spark master UI: http://localhost:8087"
log "Spark app UI (once a job is running): http://localhost:4040"
log "Prometheus scrape targets: localhost:8087/metrics/master/prometheus/, localhost:4040/metrics/prometheus/"
log ""
log "Submit a job the same way the real cluster does, e.g. Word Count:"
log "  JAVA_HOME=$JAVA11_HOME $SPARK_HOME/bin/spark-submit --master spark://localhost:7077 \\"
log "    --name WordCount --total-executor-cores 2 --executor-memory 2g \\"
log "    --conf spark.sql.streaming.metricsEnabled=true \\"
log "    --conf spark.metrics.conf=$SPARK_HOME/conf/metrics.properties \\"
log "    --conf spark.ui.port=4040 \\"
log "    --class <fully.qualified.WordCountJobClassName> \\"
log "    $SPARK_HOME/bin/dsp_spark_jobs-1.0-SNAPSHOT.jar \\"
log "    -parallelism 2 -mode Kafka -input WordCountIn -output WordCountOut \\"
log "    -kafka-server localhost:9092 -waitTimeToCancel 60 -size 0 -slide 0 \\"
log "    -query 1 -popularityThreshold 0"
log ""
log "Requires local Kafka running first -- see setup_local.sh."
log ""
log "To stop: $0 --stop"
