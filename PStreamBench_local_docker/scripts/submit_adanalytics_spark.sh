#!/usr/bin/env bash
# Submits the Ad Analytics (with_join, query=1) Spark Structured Streaming
# job. Same self-terminating behavior as Word Count -- no extra kill step.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2,2,2,2}"
WAIT_SECONDS="${2:-40}"
JOB_NAME="adanalytics-local-$(date +%s)"

echo "+ submitting Spark Ad Analytics / with_join (parallelism=$PARALLELISM, job=$JOB_NAME, runs for ${WAIT_SECONDS}s)"
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name "$JOB_NAME" \
  --total-executor-cores 2 \
  --class com.kom.dsp.AdsAnalytics.AdsAnalytics \
  /opt/spark-jobs/dsp_spark_jobs-1.0-SNAPSHOT.jar \
  -parallelism "$PARALLELISM" \
  -query 1 \
  -mode kafka \
  -input AdAnalyticsIn \
  -output AdAnalyticsOut \
  -kafka-server kafka:9092 \
  -waitTimeToCancel "$WAIT_SECONDS" \
  -size 10000 -slide 5000
