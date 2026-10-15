#!/usr/bin/env bash
# Thin wrapper so RisingWave fits the same (parallelism, duration) calling
# convention as the Flink/Kafka Streams submit scripts, for the wizard and
# for consistency. RisingWave jobs do NOT self-terminate like Flink/Kafka
# Streams do -- $2 (duration) is accepted but ignored; the source/MV/sink
# stay running until you drop them explicitly:
#   ./venv/bin/python scripts/submit_risingwave.py word_count --drop
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2}"
# RisingWave only has one swept value (the aggregation-stage
# streaming_parallelism); reuse position [1] of the vector, matching the
# same convention Kafka Streams' submit scripts use for num.stream.threads.
RW_PARALLELISM="$(echo "$PARALLELISM" | cut -d',' -f2)"

./venv/bin/python scripts/submit_risingwave.py word_count --parallelism "$RW_PARALLELISM"
