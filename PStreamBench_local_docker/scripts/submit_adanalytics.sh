#!/usr/bin/env bash
# Submits the Ad Analytics (with_join, query=1) Flink job to the local
# docker-compose cluster. Parallelism vector = (source, click-parser,
# impression-parser, clicks-counter, impressions-counter, join+rollingCTR,
# sink) -- 7 slots, per AdsAnalytics.java / operator_templates.csv. Unlike
# Word Count, this job correctly reads --kafka-server already (no fix needed).
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2,2,2,2}"
WAIT_SECONDS="${2:-40}"

echo "+ submitting Ad Analytics / with_join (parallelism=$PARALLELISM, runs for ${WAIT_SECONDS}s)"
docker compose exec -T flink-jobmanager flink run \
  -c com.kom.dsp.AdsAnalytics.AdsAnalytics \
  /opt/flink-jobs/dsp_jobs-1.0-SNAPSHOT.jar \
  --parallelism "$PARALLELISM" \
  --query 1 \
  --mode kafka \
  --input AdAnalyticsIn \
  --output AdAnalyticsOut \
  --kafka-server kafka:9092 \
  --waitTimeToCancel "$WAIT_SECONDS" \
  --size 10000 --slide 5000 --lateness 1000 --popularityThreshold 5
