#!/usr/bin/env bash
# Submits the Word Count Flink job to the local docker-compose cluster.
# Parallelism vector = (source, tokenizer, counter, sink) -- 4 slots, per
# WordCountJob.java. The job itself self-cancels after --waitTimeToCancel
# seconds (it's built into the job's own main(), not a Flink CLI flag), so
# this script blocks for roughly that long -- that's expected, not a hang.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2}"
WAIT_SECONDS="${2:-40}"

echo "+ submitting Word Count (parallelism=$PARALLELISM, runs for ${WAIT_SECONDS}s)"
docker compose exec -T flink-jobmanager flink run \
  -c com.kom.dsp.wordcount.WordCountJob \
  /opt/flink-jobs/dsp_jobs-1.0-SNAPSHOT.jar \
  --parallelism "$PARALLELISM" \
  --query 1 \
  --mode kafka \
  --input WordCountIn \
  --output WordCountOut \
  --kafka-server kafka:9092 \
  --waitTimeToCancel "$WAIT_SECONDS" \
  --size 5000 --slide 1000 --lateness 1000 --popularityThreshold 5
