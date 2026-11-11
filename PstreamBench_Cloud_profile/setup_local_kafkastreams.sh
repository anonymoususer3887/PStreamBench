#!/bin/bash
# Local (single-machine, no CloudLab) setup for Apache Kafka Streams
# benchmark testing. Counterpart of setup_local.sh / setup_local_flink.sh /
# setup_local_storm.sh / setup_local_spark.sh for Kafka Streams.
#
# Architecturally the simplest of the five engines (see
# KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.1): no cluster daemon at all --
# Kafka Streams is a library, not a JobManager/Nimbus/Master. This script
# only needs to: build the job jar if missing, and confirm local Kafka is
# running. Submitting a job is just `java -cp jar class -args...` (a
# per-run JVM, not a persistent service this script manages the lifecycle
# of), so unlike the other setup_local_*.sh scripts there is no
# start/--stop service pair here.
#
# Usage:
#   ./setup_local_kafkastreams.sh   # build the job jar if missing, print submit command

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROLLER_DIR="$SCRIPT_DIR/../PStreamBench_controller/dsp_be"
KS_JOBS_DIR="$SCRIPT_DIR/../PStreamBench_workload_generator/kafkastreams/dsp_kafkastreams_jobs"
KS_JAR="$CONTROLLER_DIR/infra/kafkastreams_files/dsp_kafkastreams_jobs-1.0-SNAPSHOT.jar"
JAVA11_HOME="$(brew --prefix openjdk@11 2>/dev/null || echo /opt/homebrew/opt/openjdk@11)"

log() { echo "[setup_local_kafkastreams] $*"; }

if [ ! -x "$JAVA11_HOME/bin/java" ]; then
  log "Installing openjdk@11..."
  brew install openjdk@11
fi

if [ ! -f "$KS_JAR" ]; then
  log "dsp_kafkastreams_jobs jar not found -- building it (mvn package)..."
  (cd "$KS_JOBS_DIR" && JAVA_HOME="$JAVA11_HOME" mvn -q package)
  mkdir -p "$CONTROLLER_DIR/infra/kafkastreams_files"
  cp "$KS_JOBS_DIR/target/dsp_kafkastreams_jobs-1.0-SNAPSHOT.jar" "$KS_JAR"
else
  log "dsp_kafkastreams_jobs jar already built at $KS_JAR."
fi

if ! pgrep -f "kafka.Kafka" >/dev/null 2>&1; then
  log "[WARN] Local Kafka doesn't appear to be running -- see setup_local.sh."
fi

JMX_JAR="$SCRIPT_DIR/jmx_prometheus_javaagent.jar"
JMX_CONF="$SCRIPT_DIR/jmx_exporter_kafkastreams_app.yml"

log ""
log "=== Ready ==="
log "No persistent service to start -- each job run is its own JVM. Submit"
log "the same way the real cluster does, e.g. Word Count (application-id"
log "must be unique per run -- it doubles as the consumer group id, and a"
log "stale one from a prior run will make the new instance pick up wherever"
log "the old one left off instead of starting fresh):"
log ""
log "  JAVA_HOME=$JAVA11_HOME $JAVA11_HOME/bin/java \\"
log "    -javaagent:$JMX_JAR=7076:$JMX_CONF \\"
log "    -cp $KS_JAR com.kom.dsp.wordcount.WordCountTopology \\"
log "    -parallelism 1 -mode Kafka -input WordCountIn -output WordCountOut \\"
log "    -kafka-server localhost:9092 -waitTimeToCancel 60 -size 0 -slide 0 \\"
log "    -query 1 -popularityThreshold 0 -application-id WordCount-\$(date +%s)"
log ""
log "Prometheus scrape target (this instance's own JMX exporter): localhost:7076"
log ""
log "Requires local Kafka running first -- see setup_local.sh."
