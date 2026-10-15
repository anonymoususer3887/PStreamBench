"""One-off validation script (not part of the main strategy sweep): runs
Ad Analytics Q1 at ~115ms window, 1,000,000 events/sec, thread=40 (the
known-hardest combo this session -- 2.5x core oversubscription on this
cluster's 16 real cores) across a matrix of grace_period_ms values, to
measure the real effect of the grace-period fix (see WindowConfig.java)
before trusting it for the full sweep.

For each grace value, records:
  - expired_warn_count: "Skipping record for expired window" occurrences
    in the job's own application log (direct evidence of the bug this
    fix targets)
  - total_log_lines: sanity denominator
  - queryMetrics.kafka_throughput.throughput: the Prometheus-derived
    metric the main sweep actually gates on
  - real Kafka output-topic message count, read directly via
    kafka-get-offsets.sh WHILE the job is still running (bypasses the
    Prometheus/JMX metrics path entirely -- ground truth, same technique
    used to root-cause the original bug live on 2026-09-25)

Run directly: ./.sweep_venv/bin/python3 grace_comparison.py
"""
import json
import subprocess
import sys
import time

sys.path.insert(0, ".")
import run_strategy_sweep_kafkastreams as sw

JOB_CLASS = "Ad Analytics"
QUERY_NUMBER = "1"
CONFIG_FILE = "adanalytics_q1"
FULL_PDS = [2, 40, 1]
EVENT_RATE = 1_000_000
WINDOW_SIZE_MS = 115
WINDOW_SLIDE_MS = 10
GRACE_VALUES_MS = [0, 115, 500, 1000]

MAIN_NODE = "ms1113.utah.cloudlab.us"
SSH_USER = "pagnih03"
OUTPUT_TOPIC = "AdAnalyticsOut"
REMOTE_LOG = "/tmp/kafkastreams-AdAnalytics-ms1113.utah.cloudlab.us.log"


def ssh(cmd, timeout=20):
    full = ["ssh", "-o", "ConnectTimeout=8", f"{SSH_USER}@{MAIN_NODE}", cmd]
    try:
        r = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return r.stdout
    except subprocess.TimeoutExpired:
        return ""


def wait_for_topic_and_sample_offsets(topic, settle_seconds=45, timeout_seconds=180):
    """Polls until `topic` exists, waits settle_seconds for real traffic,
    then returns the summed end-offset across all partitions -- ground
    truth for "how much real output has this job produced so far",
    independent of the Prometheus/JMX metrics path this whole
    investigation is about.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        out = ssh(f"cd /home/playground/kafka_2.12 && bin/kafka-topics.sh "
                  f"--bootstrap-server localhost:9092 --list 2>/dev/null | grep -q '^{topic}$' && echo FOUND")
        if "FOUND" in out:
            break
        time.sleep(10)
    else:
        return None  # topic never appeared within timeout

    time.sleep(settle_seconds)
    out = ssh(f"cd /home/playground/kafka_2.12 && bin/kafka-get-offsets.sh "
              f"--broker-list localhost:9092 --topic {topic} --time -1 2>/dev/null")
    total = 0
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic:
            try:
                total += int(parts[2])
            except ValueError:
                pass
    return total


def run_one_grace_test(grace_ms):
    print(f"\n=== grace_period_ms={grace_ms} ===", flush=True)

    # Launch the job in the background (submit_job blocks for the whole
    # job lifecycle) so we can sample live offsets concurrently.
    import threading
    result_holder = {}

    def _submit():
        result_holder["result"] = sw.submit_job(
            JOB_CLASS, QUERY_NUMBER, CONFIG_FILE, FULL_PDS, EVENT_RATE,
            window_size_override=WINDOW_SIZE_MS, window_slide_override=WINDOW_SLIDE_MS,
            grace_period_ms_override=grace_ms)

    t = threading.Thread(target=_submit)
    t.start()

    live_offset_total = wait_for_topic_and_sample_offsets(OUTPUT_TOPIC)
    print(f"  live output-topic offset sample (mid-run, ground truth): {live_offset_total}", flush=True)

    t.join()
    result = result_holder["result"]
    print(f"  submit_job result: {result}", flush=True)

    # Application log analysis (job has fully closed by now -- runner.py's
    # get_logs() already ran before cleanup, so the log file, unlike the
    # topic, is not deleted).
    expired_count = ssh(f"grep -c 'Skipping record for expired window' {REMOTE_LOG} 2>/dev/null").strip()
    total_lines = ssh(f"wc -l < {REMOTE_LOG} 2>/dev/null").strip()

    throughput = None
    if result.get("status") == "ok":
        eval_path = result.get("file")
        if eval_path:
            try:
                with open(eval_path) as f:
                    d = json.load(f)
                throughput = d.get("queryMetrics", {}).get("kafka_throughput", {}).get("throughput")
            except Exception as e:
                print(f"  (could not read {eval_path}: {e!r})")
    elif result.get("status") == "null_core_metrics":
        throughput = "null (discarded)"

    return {
        "grace_ms": grace_ms,
        "status": result.get("status"),
        "kafka_throughput": throughput,
        "expired_warn_count": expired_count,
        "total_log_lines": total_lines,
        "live_output_offset_sample": live_offset_total,
    }


def main():
    results = []
    for grace_ms in GRACE_VALUES_MS:
        r = run_one_grace_test(grace_ms)
        results.append(r)
        print(f"  >>> {r}", flush=True)
        print("  sleeping 60s gap before next grace value...", flush=True)
        time.sleep(60)

    print("\n\n=== SUMMARY ===")
    print(f"{'grace_ms':>10} {'status':>18} {'throughput':>15} {'expired_warns':>15} {'log_lines':>12} {'live_offset':>12}")
    for r in results:
        print(f"{r['grace_ms']:>10} {str(r['status']):>18} {str(r['kafka_throughput']):>15} "
              f"{r['expired_warn_count']:>15} {r['total_log_lines']:>12} {str(r['live_output_offset_sample']):>12}")


if __name__ == "__main__":
    main()
