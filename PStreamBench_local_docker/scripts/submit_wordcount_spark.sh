#!/usr/bin/env bash
# Submits the Word Count Spark Structured Streaming job via spark-submit,
# run inside spark-master (which has spark-submit on its PATH and is
# already configured to talk to itself). Spark has no per-operator
# parallelism; parallelism_degree[0..3] map to repartition() calls /
# spark.sql.shuffle.partitions -- see WordCountJob.java's own class comment.
#
# Unlike Storm, this job genuinely self-terminates (query.stop() is called
# explicitly after awaitTermination(), not just logged) -- no extra kill
# step needed here.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2}"
WAIT_SECONDS="${2:-40}"
JOB_NAME="wordcount-local-$(date +%s)"

echo "+ submitting Spark Word Count (parallelism=$PARALLELISM, job=$JOB_NAME, runs for ${WAIT_SECONDS}s)"
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name "$JOB_NAME" \
  --total-executor-cores 2 \
  --class com.kom.dsp.wordcount.WordCountJob \
  /opt/spark-jobs/dsp_spark_jobs-1.0-SNAPSHOT.jar \
  -parallelism "$PARALLELISM" \
  -mode kafka \
  -input WordCountIn \
  -output WordCountOut \
  -kafka-server kafka:9092 \
  -waitTimeToCancel "$WAIT_SECONDS"
