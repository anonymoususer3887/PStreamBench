#!/usr/bin/env bash
# Submits the Word Count Kafka Streams app to the local docker-compose
# cluster. Unlike Flink, there's no "cluster" to submit to -- this runs the
# app jar directly as a plain `java` process inside kafkastreams-app, which
# connects straight to kafka:9092.
#
# Only parallelism_degree[1] (num.stream.threads) is meaningful here -- see
# WordCountTopology.java's class comment for why (Kafka Streams has no
# per-operator parallelism, unlike Flink/Storm/Spark).
#
# application-id MUST be unique per run (it doubles as the consumer group id
# and the RocksDB state-store directory name) -- reusing one across runs
# either resumes stale state or collides; this script timestamps it for you.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2}"
WAIT_SECONDS="${2:-40}"
APP_ID="wordcount-local-$(date +%s)"

echo "+ submitting Kafka Streams Word Count (parallelism=$PARALLELISM, application-id=$APP_ID, runs for ${WAIT_SECONDS}s)"
docker compose exec -T kafkastreams-app java \
  -cp /opt/kafkastreams-jobs/dsp_kafkastreams_jobs-1.0-SNAPSHOT.jar \
  com.kom.dsp.wordcount.WordCountTopology \
  -parallelism "$PARALLELISM" \
  -mode kafka \
  -input WordCountIn \
  -output WordCountOut \
  -kafka-server kafka:9092 \
  -application-id "$APP_ID" \
  -waitTimeToCancel "$WAIT_SECONDS"
