#!/usr/bin/env bash
# See submit_wordcount_risingwave.sh for why this wrapper exists and why
# duration ($2) is accepted but ignored.
set -euo pipefail
cd "$(dirname "$0")/.."

PARALLELISM="${1:-2,2,2,2,2,2,2}"
RW_PARALLELISM="$(echo "$PARALLELISM" | cut -d',' -f2)"

./venv/bin/python scripts/submit_risingwave.py ad_analytics --parallelism "$RW_PARALLELISM"
