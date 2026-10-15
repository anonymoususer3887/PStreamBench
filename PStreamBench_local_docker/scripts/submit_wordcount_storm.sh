#!/usr/bin/env bash
# Submits the Word Count Storm topology to the local Nimbus via `storm jar`
# (run inside storm-nimbus, which has the storm CLI + is already configured
# to talk to itself). Only parallelism_degree[0..3] are meaningful here:
# spout, tokenizer, counter, sink -- same 4-slot shape as Flink's WordCountJob.
#
# Topology names must be unique while active on this Nimbus -- timestamped
# per run, same reasoning as Kafka Streams' application-id.
#
# IMPORTANT (found testing this): AbstractTopology.java's distributed-mode
# path logs "Killing query: <name>" after waitTimeToCancel but never
# actually calls a kill API -- the topology stays ACTIVE forever. In the
# real deployment this is masked because the Django controller kills jobs
# itself via a separate Ansible playbook (cluster-simple-storm-job-kill.yaml)
# after the job's own run -- it's not a bug so much as "distributed Storm
# topologies are killed externally, not self-killed," unlike Flink/Kafka
# Streams which do self-terminate in-process. There's no controller here,
# so this script does that kill itself; skipping it leaked 3 topologies
# that quietly exhausted every worker slot and made every later submission
# sit forever "ACTIVE" with 0 workers and no error anywhere.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2}"
WAIT_SECONDS="${2:-40}"
TOPOLOGY_NAME="wordcount-local-$(date +%s)"

echo "+ submitting Storm Word Count (parallelism=$PARALLELISM, topology=$TOPOLOGY_NAME, runs for ${WAIT_SECONDS}s)"
docker compose exec -T storm-nimbus storm jar \
  /opt/storm-jobs/dsp_storm_jobs-1.0-SNAPSHOT-all.jar \
  com.kom.dsp.wordcount.WordCountTopology \
  -name "$TOPOLOGY_NAME" \
  -parallelism "$PARALLELISM" \
  -query 1 \
  -mode kafka \
  -input WordCountIn \
  -output WordCountOut \
  -kafka-server kafka:9092 \
  -main-ip storm-nimbus \
  -waitTimeToCancel "$WAIT_SECONDS" \
  -size 0 -slide 0 -lateness 0 -popularityThreshold 0 -numWorkers 2

echo "+ killing topology $TOPOLOGY_NAME (the job itself never does this in distributed mode -- see comment above)"
docker compose exec -T storm-nimbus storm kill "$TOPOLOGY_NAME" -w 0 >/dev/null 2>&1 || true
