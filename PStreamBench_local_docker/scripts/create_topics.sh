#!/usr/bin/env bash
# Creates the Kafka input/output topics for Word Count and Ad Analytics.
# Topic names match dsp_be/utils/constants_flink.py exactly, so artifacts
# produced here are directly comparable to the real cluster's naming.
set -euo pipefail
cd "$(dirname "$0")/.."

TOPICS=(WordCountIn WordCountOut AdAnalyticsIn AdAnalyticsOut)

for t in "${TOPICS[@]}"; do
  echo "+ creating topic: $t"
  docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$t" --partitions 4 --replication-factor 1
done

echo
echo "topics now present:"
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list
