#!/bin/bash
# Local (single-machine, no CloudLab) setup for Apache Storm benchmark
# testing. Counterpart of setup_local.sh (RisingWave) / setup_local_flink.sh
# for Storm.
#
# Unlike Flink's dsp_jobs jar (bundled directly in this repo), Storm's job
# jar is NOT pre-built -- it's a Gradle project at
# PStreamBench_workload_generator/storm/dsp_storm_jobs with a Makefile
# (`make build`) that runs `./gradlew shadowJar` and copies the result to
# PStreamBench_controller/dsp_be/infra/storm_files/. This script builds it
# if missing, the same way the Makefile does.
#
# The sample Storm config (infra/storm_files/sample-storm.yaml) already
# hardcodes "localhost" for zookeeper/nimbus -- no changes needed for local
# single-machine testing, unlike Flink's memory-size overrides.
#
# Storm's Nimbus requires a real Zookeeper ensemble (unlike this project's
# local Kafka, which runs in KRaft mode with no Zookeeper at all) --
# confirmed live (2026-07-27): Nimbus fails with
# "KeeperException$ConnectionLossException ... for /storm" if nothing is
# listening on port 2181. This script installs/starts a standalone
# Homebrew `zookeeper` instance dedicated to Storm (unrelated to Kafka).
#
# Usage:
#   ./setup_local_storm.sh          # full setup + start ZK/Nimbus/UI/Supervisor/exporter
#   ./setup_local_storm.sh --stop   # stop everything this script started

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROLLER_DIR="$SCRIPT_DIR/../PStreamBench_controller/dsp_be"
STORM_JOBS_DIR="$SCRIPT_DIR/../PStreamBench_workload_generator/storm/dsp_storm_jobs"
STORM_JAR="$CONTROLLER_DIR/infra/storm_files/dsp_storm_jobs-1.0-SNAPSHOT-all.jar"
STORM_DIR="${PSTREAMBENCH_STORM_DIR:-$HOME/pstreambench_data/.storm}"
STORM_VERSION="2.6.2"
STORM_HOME="$STORM_DIR/apache-storm-$STORM_VERSION"
JAVA11_HOME="$(brew --prefix openjdk@11 2>/dev/null || echo /opt/homebrew/opt/openjdk@11)"
PY_VENV="${PSTREAMBENCH_PY_VENV:-$HOME/.pstreambench_venv}"
NIMBUS_LOG="/tmp/pstreambench_local_storm_nimbus.log"
UI_LOG="/tmp/pstreambench_local_storm_ui.log"
SUPERVISOR_LOG="/tmp/pstreambench_local_storm_supervisor.log"
EXPORTER_LOG="/tmp/pstreambench_local_storm_exporter.log"

log() { echo "[setup_local_storm] $*"; }

if [ "$1" = "--stop" ]; then
  log "Stopping local Storm Nimbus/UI/Supervisor/exporter/Zookeeper..."
  pkill -f "apache-storm-$STORM_VERSION.*nimbus" 2>/dev/null || true
  pkill -f "apache-storm-$STORM_VERSION.*core.daemon.ui" 2>/dev/null || true
  pkill -f "apache-storm-$STORM_VERSION.*supervisor" 2>/dev/null || true
  pkill -f "storm_exporter" 2>/dev/null || true
  pkill -f "zookeeper.*QuorumPeerMain" 2>/dev/null || true
  log "Done."
  exit 0
fi

# --- 0. Java 11 (Storm 2.6.2 targets Java 8/11, not 17) --------------------
if [ ! -x "$JAVA11_HOME/bin/java" ]; then
  log "Installing openjdk@11 (Storm $STORM_VERSION doesn't support Java 17)..."
  brew install openjdk@11
fi

ZK_PREFIX="$(brew --prefix zookeeper 2>/dev/null || echo /opt/homebrew/opt/zookeeper)"
if [ ! -x "$ZK_PREFIX/bin/zkServer" ]; then
  log "Installing Zookeeper (required by Storm's Nimbus -- this project's local Kafka runs KRaft mode with no ZK of its own)..."
  brew install zookeeper
fi
if "$ZK_PREFIX/bin/zkServer" status >/dev/null 2>&1; then
  log "Zookeeper already running."
else
  # zkServer's own daemon-mode `start` does a readiness check that's
  # flaky/reports FAILED TO START even when the server comes up fine a
  # moment later (confirmed live 2026-07-27) -- start-foreground + nohup
  # instead, same pattern as every other service this script manages.
  #
  # zookeeper.admin.enableServer=false -- ZK's built-in Jetty admin server
  # defaults to port 8080, which collides with Storm UI's port 8080 on the
  # same host (confirmed live 2026-07-27: ZK exits with "Address already in
  # use" / "Exiting JVM with code 4" otherwise). Not needed for Storm's use
  # of ZK, just disabled.
  log "Starting Zookeeper on :2181 (dedicated to Storm)..."
  SERVER_JVMFLAGS="-Dapple.awt.UIElement=true -Dzookeeper.admin.enableServer=false" nohup "$ZK_PREFIX/bin/zkServer" start-foreground > /tmp/pstreambench_local_zookeeper.log 2>&1 &
  disown
  sleep 5
fi

# --- 1. Build the job jar if missing (venv not needed -- this is Gradle/Java) ---
if [ ! -f "$STORM_JAR" ]; then
  log "dsp_storm_jobs jar not found -- building it (./gradlew shadowJar)..."
  (cd "$STORM_JOBS_DIR" && JAVA_HOME="$JAVA11_HOME" ./gradlew shadowJar)
  cp "$STORM_JOBS_DIR/build/libs/dsp_storm_jobs-1.0-SNAPSHOT-all.jar" "$STORM_JAR"
else
  log "dsp_storm_jobs jar already built at $STORM_JAR."
fi

# --- 2. storm_exporter's venv (prometheus_client) -- see feedback_use_venv_for_python ---
if [ ! -x "$PY_VENV/bin/python3" ] || ! "$PY_VENV/bin/python3" -c "import prometheus_client" >/dev/null 2>&1; then
  log "Creating/updating venv at $PY_VENV for storm_exporter.py's prometheus_client dependency..."
  python3 -m venv "$PY_VENV"
  "$PY_VENV/bin/pip" install --quiet --upgrade pip
  "$PY_VENV/bin/pip" install --quiet prometheus_client requests
fi

# --- 3. Download + lay out Storm (idempotent) ------------------------------
mkdir -p "$STORM_DIR"
if [ ! -d "$STORM_HOME" ]; then
  log "Downloading Storm $STORM_VERSION..."
  curl -sL "https://archive.apache.org/dist/storm/apache-storm-$STORM_VERSION/apache-storm-$STORM_VERSION.tar.gz" \
    -o "$STORM_DIR/storm.tar.gz"
  tar zxf "$STORM_DIR/storm.tar.gz" -C "$STORM_DIR"
else
  log "Storm $STORM_VERSION already present at $STORM_HOME."
fi

cp "$STORM_JAR" "$STORM_HOME/bin/"
cp "$SCRIPT_DIR/storm_exporter.py" "$STORM_HOME/bin/"
cp "$CONTROLLER_DIR/infra/storm_files/sample-storm.yaml" "$STORM_HOME/conf/storm.yaml"

# --- 4. Start Nimbus + UI + Supervisor + Prometheus exporter (idempotent) ---
if pgrep -f "apache-storm-$STORM_VERSION.*nimbus" >/dev/null 2>&1; then
  log "Nimbus already running."
else
  log "Starting Nimbus..."
  JAVA_HOME="$JAVA11_HOME" nohup "$STORM_HOME/bin/storm" nimbus > "$NIMBUS_LOG" 2>&1 &
  disown
  sleep 5
fi

if pgrep -f "apache-storm-$STORM_VERSION.*core.daemon.ui" >/dev/null 2>&1; then
  log "Storm UI already running."
else
  log "Starting Storm UI..."
  JAVA_HOME="$JAVA11_HOME" nohup "$STORM_HOME/bin/storm" ui > "$UI_LOG" 2>&1 &
  disown
  sleep 5
fi

if pgrep -f "apache-storm-$STORM_VERSION.*supervisor" >/dev/null 2>&1; then
  log "Supervisor already running."
else
  log "Starting Supervisor (worker JVM host)..."
  JAVA_HOME="$JAVA11_HOME" nohup "$STORM_HOME/bin/storm" supervisor > "$SUPERVISOR_LOG" 2>&1 &
  disown
  sleep 5
fi

# Port 9094, not the Ansible playbook's usual 9093 -- confirmed live
# (2026-07-27) that 9093 collides with this project's local Kafka broker's
# own KRaft controller listener port on this machine. Unrelated processes,
# just an unlucky default-port clash local-only.
STORM_EXPORTER_PORT=9094
if pgrep -f "storm_exporter" >/dev/null 2>&1; then
  log "storm_exporter already running."
else
  log "Starting Prometheus storm_exporter (polls Storm UI REST API on :8080, exposes :$STORM_EXPORTER_PORT)..."
  nohup bash -c "while true; do '$PY_VENV/bin/python3' '$STORM_HOME/bin/storm_exporter.py' localhost:8080 $STORM_EXPORTER_PORT 1; sleep 5; done" > "$EXPORTER_LOG" 2>&1 &
  disown
  sleep 3
fi

if curl -sf "http://localhost:8080/api/v1/cluster/summary" >/dev/null 2>&1; then
  log "Storm UI responding on :8080."
else
  log "[WARN] Storm UI not responding yet -- check $UI_LOG and $NIMBUS_LOG"
fi

log ""
log "=== Ready ==="
log "Storm UI:        http://localhost:8080"
log "Prometheus scrape target (storm_exporter): localhost:$STORM_EXPORTER_PORT"
log ""
log "Submit a job the same way the real cluster does, e.g. Word Count:"
log "  JAVA_HOME=$JAVA11_HOME $STORM_HOME/bin/storm jar $STORM_HOME/bin/dsp_storm_jobs-1.0-SNAPSHOT-all.jar \\"
log "    com.kom.dsp.wordcount.WordCountTopology -name WordCount -parallelism 1 \\"
log "    -mode Kafka -input WordCountIn -output WordCountOut -query 1 \\"
log "    -kafka-server localhost:9092 -main-ip localhost -size 0 -slide 0 \\"
log "    -waitTimeToCancel 60 -lateness 0 -popularityThreshold 0 -numWorkers 1"
log ""
log "Requires local Kafka running first -- see setup_local.sh."
log ""
log "To stop: $0 --stop"
