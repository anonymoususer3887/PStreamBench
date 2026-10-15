#!/usr/bin/env bash
# Proof-of-life check: reads a handful of messages from an output topic and
# counts them. Non-zero output = the job actually processed real records and
# wrote real results, not just "the container started".
set -euo pipefail
cd "$(dirname "$0")/.."

TOPIC="${1:?output topic required, e.g. WordCountOut or AdAnalyticsOut}"
MAX_MESSAGES="${2:-10}"

echo "+ reading up to ${MAX_MESSAGES} messages from ${TOPIC} (10s timeout)..."
COUNT=$(docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages "$MAX_MESSAGES" \
  --timeout-ms 10000 2>/dev/null | tee /tmp/pstreambench_verify_sample.txt | wc -l | tr -d ' ')

echo
if [ "$COUNT" -gt 0 ]; then
  echo "PASS: ${COUNT} message(s) found on ${TOPIC}. Sample:"
  head -5 /tmp/pstreambench_verify_sample.txt
else
  echo "FAIL: no messages found on ${TOPIC} -- job did not produce output (or producer hasn't sent enough data yet)"
  exit 1
fi
