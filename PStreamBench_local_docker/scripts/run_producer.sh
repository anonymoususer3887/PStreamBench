#!/usr/bin/env bash
# Feeds real data (downloaded from Kaggle by setup_datasets.sh) into the
# local Kafka topic for a workload. Runs on the HOST, against Kafka's
# host-facing listener (localhost:9092) -- see docker-compose.yml's comment
# on why that port number specifically matters.
#
# Usage: ./scripts/run_producer.sh "Word_Count" WordCountIn [events_per_second] [duration_seconds]
#        ./scripts/run_producer.sh "Ad_Analytics" AdAnalyticsIn [events_per_second] [duration_seconds]
set -euo pipefail
cd "$(dirname "$0")/.."

APP_TYPE="${1:?app_type required, e.g. Word_Count or Ad_Analytics -- underscored, matches custom_producer.py own constants}"
TOPIC="${2:?input topic required, e.g. WordCountIn or AdAnalyticsIn}"
RATE="${3:-1000}"
DURATION="${4:-20}"

echo "+ producing ${RATE} events/s of ${APP_TYPE} onto ${TOPIC} for ${DURATION}s"
# No GNU `timeout` on macOS by default -- background the producer (it loops
# indefinitely over the dataset otherwise) and kill it after DURATION.
./venv/bin/python scripts/custom_producer.py \
  --event_per_second "$RATE" \
  --app_type "$APP_TYPE" \
  --kafka_bootstrap_server localhost:9092 \
  --kafka_input_topic "$TOPIC" &
PRODUCER_PID=$!
sleep "$DURATION"
kill "$PRODUCER_PID" 2>/dev/null || true
wait "$PRODUCER_PID" 2>/dev/null || true
echo "+ producer stopped (duration elapsed)"
