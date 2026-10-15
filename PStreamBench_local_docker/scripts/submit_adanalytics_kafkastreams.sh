#!/usr/bin/env bash
# Submits the Ad Analytics (with_join, query=1) Kafka Streams app. Same
# input/output topics as the Flink version (AdAnalyticsIn/Out) -- Kafka
# Streams uses its own application-id as the consumer group, so it can read
# the same topic Flink is also consuming without interfering.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2}"
WAIT_SECONDS="${2:-40}"
APP_ID="adanalytics-local-$(date +%s)"

echo "+ submitting Kafka Streams Ad Analytics / with_join (parallelism=$PARALLELISM, application-id=$APP_ID, runs for ${WAIT_SECONDS}s)"
docker compose exec -T kafkastreams-app java \
  -cp /opt/kafkastreams-jobs/dsp_kafkastreams_jobs-1.0-SNAPSHOT.jar \
  com.kom.dsp.AdsAnalytics.AdsAnalyticsTopology \
  -parallelism "$PARALLELISM" \
  -mode kafka \
  -input AdAnalyticsIn \
  -output AdAnalyticsOut \
  -kafka-server kafka:9092 \
  -application-id "$APP_ID" \
  -waitTimeToCancel "$WAIT_SECONDS" \
  -query 1 -size 10000 -slide 5000
