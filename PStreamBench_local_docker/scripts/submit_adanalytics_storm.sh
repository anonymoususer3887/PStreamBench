#!/usr/bin/env bash
# Submits the Ad Analytics (with_join, query=1) Storm topology. 7-slot
# parallelism vector: spout,clicks-parser,impressions-parser,clicks-counter,
# impressions-counter,join+rollingCTR,sink -- matches operator_templates.csv.
# See submit_wordcount_storm.sh for why this script kills the topology
# itself afterward (the job never does, in distributed mode).
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2,2,2,2}"
WAIT_SECONDS="${2:-40}"
TOPOLOGY_NAME="adanalytics-local-$(date +%s)"

echo "+ submitting Storm Ad Analytics / with_join (parallelism=$PARALLELISM, topology=$TOPOLOGY_NAME, runs for ${WAIT_SECONDS}s)"
docker compose exec -T storm-nimbus storm jar \
  /opt/storm-jobs/dsp_storm_jobs-1.0-SNAPSHOT-all.jar \
  com.kom.dsp.adAnalytics.AdAnalyticsTopology \
  -name "$TOPOLOGY_NAME" \
  -parallelism "$PARALLELISM" \
  -query 1 \
  -mode kafka \
  -input AdAnalyticsIn \
  -output AdAnalyticsOut \
  -kafka-server kafka:9092 \
  -main-ip storm-nimbus \
  -waitTimeToCancel "$WAIT_SECONDS" \
  -size 10000 -slide 5000 -lateness 1000 -popularityThreshold 5 -numWorkers 2

echo "+ killing topology $TOPOLOGY_NAME"
docker compose exec -T storm-nimbus storm kill "$TOPOLOGY_NAME" -w 0 >/dev/null 2>&1 || true
