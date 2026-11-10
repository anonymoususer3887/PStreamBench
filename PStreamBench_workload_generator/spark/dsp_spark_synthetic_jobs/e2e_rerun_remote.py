"""Reruns the ds2/dhalion synthetic Spark sweep with the fixed jar (adds
true end-to-end latency via SyntheticTopology.java's sink() instrumentation)
using the EXACT same per-shape/per-rate parallelism assignments as the
original 2026-10-17 runs (read from ASSIGNMENTS below, extracted from the
existing metaInfo-*.json files). Writes into NEW ds2_e2e/dhalion_e2e
folders -- never touches the existing ds2/dhalion data.

Runs ON the cluster's master node (ms1034 / node0 / 10.10.1.1), since
Prometheus and the Spark driver's metrics endpoint are only bound to the
internal 10.10.1.1 address.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

MASTER_URL = "spark://128.110.217.33:7077"
# This cluster's Spark master/webUI (8087) and Prometheus (9090) are only
# bound to the public IP, unlike the old dead cluster where 10.10.1.1
# (internal LAN) worked for both -- confirmed live this cluster binds
# [::ffff:128.110.217.33] only for 7077/8087; 9090 happens to answer on
# both but keeping everything on one consistent address avoids relying on
# that coincidence persisting.
MAIN_NODE_IP = "128.110.217.33"
JAR = "/home/playground/dsp_spark_synthetic_jobs-1.0-SNAPSHOT-e2e-fixed.jar"
CHECKPOINT_ROOT = "/proj/maki-test-PG0/spark-checkpoints"
GETPROM = "/home/playground/getprom_spark.py"
VENV_PY = "/home/playground/e2e_venv/bin/python3"
DATA_ROOT = Path("/home/playground/e2e_output")
MANIFEST = Path("/home/playground/e2e_rerun_manifest.jsonl")

JOB_RUN_TIME_MIN = 5
# Raised from 300s: the 17 null/zero-output combos (wide joins at DoP=2)
# were never actually hanging -- a single skewed join/window batch takes
# 5-10+ min at these rates, which exceeded Spark's default
# spark.network.timeout (120s) and got the executor declared dead mid-task,
# triggering an expensive stage-retry storm that ate the whole budget. Fixed
# below via --conf spark.network.timeout/heartbeatInterval; this longer
# budget just gives a real (slow but alive) batch enough wall time to finish.
WAIT_TIME_TO_CANCEL_S = 1800
# Raised from 20s: a retry pass at 1200s/20s cooldown still produced
# null_output on a cleanly-executing attempt (zero heartbeat timeouts, zero
# OOM) for ds2_e2e|ThreeWayJoin|1000000 -- give genuinely slow batches more
# wall-clock margin and let worker JVMs from a prior failed attempt fully
# release memory before the next attempt starts.
SUBMIT_REGISTER_TIMEOUT_S = 60
COOLDOWN_S = 60

# Default executor memory in standalone mode is 1g for a whole 16-core
# executor -- too tight for join state under these event rates. Bumped on
# every attempt (not just retries), since this changes nothing about the
# experimental variable (DoP/parallelism), only the resource ceiling, and
# the cluster has 59GB free per node to spare. Escalated further on retry
# for combos that still come back null/zero, up to MAX_ATTEMPTS.
MEMORY_TIERS = [("4g", "8g"), ("6g", "16g"), ("8g", "24g")]
MAX_ATTEMPTS = len(MEMORY_TIERS)

CLASS_MAP = {
    "LinearQuery": ("com.kom.dsp.synthetic.LinearQueryTopology", 1),
    "TwoFilters": ("com.kom.dsp.synthetic.ChainedFiltersTopology", 2),
    "ThreeFilters": ("com.kom.dsp.synthetic.ChainedFiltersTopology", 3),
    "FourFilters": ("com.kom.dsp.synthetic.ChainedFiltersTopology", 4),
    "TwoWayJoin": ("com.kom.dsp.synthetic.JoinTopology", 2),
    "ThreeWayJoin": ("com.kom.dsp.synthetic.JoinTopology", 3),
    "FourWayJoin": ("com.kom.dsp.synthetic.JoinTopology", 4),
    "FiveWayJoin": ("com.kom.dsp.synthetic.JoinTopology", 5),
    "SixWayJoin": ("com.kom.dsp.synthetic.JoinTopology", 6),
}

# Extracted verbatim from the existing metaInfo-*.json files for the
# original 2026-10-17 ds2/dhalion runs -- (shape, rate) -> pds list.
ASSIGNMENTS = {
    "ds2": {
        ("LinearQuery", 100000): "2,2,2,2,1", ("LinearQuery", 500000): "2,3,3,3,1", ("LinearQuery", 1000000): "2,2,2,2,1",
        ("TwoFilters", 100000): "2,2,2,2,1", ("TwoFilters", 500000): "2,3,3,3,1", ("TwoFilters", 1000000): "2,2,2,2,1",
        ("ThreeFilters", 100000): "2,2,2,2,2,1", ("ThreeFilters", 500000): "2,3,3,3,3,1", ("ThreeFilters", 1000000): "2,2,2,2,2,1",
        ("FourFilters", 100000): "2,2,2,2,2,2,1", ("FourFilters", 500000): "2,3,3,3,3,3,1", ("FourFilters", 1000000): "2,2,2,2,2,2,1",
        ("TwoWayJoin", 100000): "2,2,3,3,26,1", ("TwoWayJoin", 500000): "2,2,3,3,2,1", ("TwoWayJoin", 1000000): "2,2,3,3,2,1",
        ("ThreeWayJoin", 100000): "2,2,2,2,2,2,4,1", ("ThreeWayJoin", 500000): "2,2,2,3,3,3,2,1", ("ThreeWayJoin", 1000000): "2,2,2,3,3,3,2,1",
        ("FourWayJoin", 100000): "2,2,2,2,2,1", ("FourWayJoin", 500000): "2,2,2,2,2,1", ("FourWayJoin", 1000000): "2,2,2,2,2,1",
        ("FiveWayJoin", 100000): "2,2,2,2,2,2,1", ("FiveWayJoin", 500000): "2,2,2,2,2,2,1", ("FiveWayJoin", 1000000): "2,2,2,2,2,2,1",
        ("SixWayJoin", 100000): "2,2,2,2,2,2,2,1", ("SixWayJoin", 500000): "2,2,2,2,2,2,2,1", ("SixWayJoin", 1000000): "2,2,2,2,2,2,2,1",
    },
    "dhalion": {
        ("LinearQuery", 100000): "2,3,3,3,1", ("LinearQuery", 500000): "2,2,2,2,1", ("LinearQuery", 1000000): "2,3,3,3,1",
        ("TwoFilters", 100000): "2,3,3,3,1", ("TwoFilters", 500000): "2,2,2,2,1", ("TwoFilters", 1000000): "2,3,3,3,1",
        ("ThreeFilters", 100000): "2,3,3,3,3,1", ("ThreeFilters", 500000): "2,3,3,3,3,1", ("ThreeFilters", 1000000): "2,3,3,3,3,1",
        ("FourFilters", 100000): "2,3,3,3,3,3,1", ("FourFilters", 500000): "2,2,2,2,2,2,1", ("FourFilters", 1000000): "2,3,3,3,3,3,1",
        ("TwoWayJoin", 100000): "2,2,2,2,2,1", ("TwoWayJoin", 500000): "2,2,5,5,5,1", ("TwoWayJoin", 1000000): "2,2,8,8,8,1",
        ("ThreeWayJoin", 100000): "2,2,2,3,3,3,3,1", ("ThreeWayJoin", 500000): "2,2,2,7,7,7,7,1", ("ThreeWayJoin", 1000000): "2,2,2,12,12,12,12,1",
        ("FourWayJoin", 100000): "2,2,2,2,7,1", ("FourWayJoin", 500000): "2,2,2,2,2,1", ("FourWayJoin", 1000000): "2,2,2,2,2,1",
        ("FiveWayJoin", 100000): "2,2,2,2,2,2,1", ("FiveWayJoin", 500000): "2,2,2,2,2,2,1", ("FiveWayJoin", 1000000): "2,2,2,2,2,2,1",
        ("SixWayJoin", 100000): "2,2,2,2,2,2,2,1", ("SixWayJoin", 500000): "2,2,2,2,2,2,2,1", ("SixWayJoin", 1000000): "2,2,2,2,2,2,2,1",
    },
}

RATES = [100000, 500000, 1000000]
SHAPES = list(CLASS_MAP.keys())


def log(msg):
    print(f"[e2e_rerun] {msg}", flush=True)


def log_manifest(key, **fields):
    entry = {"key": key, "ts": time.time(), **fields}
    with open(MANIFEST, "a") as f:
        f.write(json.dumps(entry) + "\n")


def already_done(key):
    if not MANIFEST.exists():
        return False
    for line in MANIFEST.read_text().splitlines():
        try:
            e = json.loads(line)
        except Exception:
            continue
        if e.get("key") == key and e.get("status") == "ok":
            return True
    return False


def run_one(strategy, shape, rate):
    key = f"{strategy}_e2e|{shape}|{rate}"
    if already_done(key):
        log(f"SKIP (already ok): {key}")
        return

    last_status = None
    for attempt, (driver_mem, executor_mem) in enumerate(MEMORY_TIERS, start=1):
        last_status = _attempt_one(strategy, shape, rate, key, attempt, driver_mem, executor_mem)
        if last_status == "ok":
            return
        log(f"  attempt {attempt}/{MAX_ATTEMPTS} for {key} came back '{last_status}'"
            + ("" if attempt == MAX_ATTEMPTS else f", retrying with more memory ({MEMORY_TIERS[attempt][0]} driver / {MEMORY_TIERS[attempt][1]} executor)..."))
        time.sleep(COOLDOWN_S)

    log(f"  GIVING UP on {key} after {MAX_ATTEMPTS} attempts, last status={last_status}")
    log_manifest(key, status=f"failed_after_retries:{last_status}")


def _attempt_one(strategy, shape, rate, key, attempt, driver_mem, executor_mem):
    pds = ASSIGNMENTS[strategy][(shape, rate)]
    class_name, query = CLASS_MAP[shape]
    # cwd for getprom_spark.py, which creates its own <shape>/ subfolder
    # under this (full_folder_path = os.getcwd()/job_name) -- matches the
    # original single-nesting convention: .../nudge/<Shape>/evaluationMetrics*.
    out_dir = DATA_ROOT / shape / "spark" / "distributed" / f"{strategy}_e2e" / str(rate) / "nudge"
    out_dir.mkdir(parents=True, exist_ok=True)
    submit_log = out_dir / f"submit_attempt{attempt}.log"

    cmd = [
        "/home/playground/spark-3.5.1-bin-hadoop3/bin/spark-submit",
        "--master", MASTER_URL,
        "--deploy-mode", "client",
        "--name", shape,
        "--class", class_name,
        "--conf", f"spark.driver.memory={driver_mem}",
        "--conf", f"spark.executor.memory={executor_mem}",
        # 600s was marginal, not generous: real single-task durations for
        # these wide-join/DoP=2 batches clustered at 620-650s across 3
        # separate observed runs (ThreeWayJoin and FourWayJoin alike),
        # repeatedly tripping a 600s timeout regardless of memory tier.
        "--conf", "spark.network.timeout=1800s",
        "--conf", "spark.executor.heartbeatInterval=30s",
        # Without this, Spark never registers a streaming metrics source at
        # all (confirmed live: zero spark_streaming* series in Prometheus
        # for any app run on this cluster, while other Spark metrics flow
        # fine) -- getSparkStreamingMetrics' processing_rate query always
        # comes back None regardless of job outcome. The old dead cluster
        # must have had this set some other way (a one-off spark-defaults.conf
        # edit that died with the node) since it never lived in this script;
        # setting it here instead makes it survive a cluster rebuild.
        "--conf", "spark.sql.streaming.metricsEnabled=true",
        JAR,
        "--name", shape,
        "--parallelism", pds,
        "--query", str(query),
        "--eventRate", str(rate),
        "--size", "1000",
        "--slide", "1000",
        "--waitTimeToCancel", str(WAIT_TIME_TO_CANCEL_S),
        "--checkpointRoot", CHECKPOINT_ROOT,
    ]
    log(f"SUBMIT {key} pds={pds} query={query}")
    with open(submit_log, "w") as f:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)

    # Poll the master's JSON status API for our app, matched by name.
    app_id = None
    deadline = time.time() + SUBMIT_REGISTER_TIMEOUT_S
    while time.time() < deadline:
        try:
            import requests
            r = requests.get(f"http://{MAIN_NODE_IP}:8087/json/", timeout=5)
            for app in r.json().get("activeapps", []):
                if app["name"] == shape:
                    app_id = app["id"]
                    break
        except Exception as e:
            log(f"  (master query failed: {e})")
        if app_id:
            break
        time.sleep(3)

    if not app_id:
        proc.wait(timeout=WAIT_TIME_TO_CANCEL_S + 60)
        log(f"  NEVER REGISTERED with master: {key}")
        log_manifest(key, status="never_registered", attempt=attempt)
        return "never_registered"

    log(f"  app_id={app_id}, waiting for completion...")
    try:
        proc.wait(timeout=WAIT_TIME_TO_CANCEL_S + 300)
    except subprocess.TimeoutExpired:
        log(f"  teardown exceeded margin, killing driver: {key}")
        proc.kill()
        proc.wait(timeout=60)
        log_manifest(key, status="wait_timeout", app_id=app_id, attempt=attempt)
        return "wait_timeout"

    getprom_cmd = [
        VENV_PY, GETPROM,
        "--main_node_ip", MAIN_NODE_IP,
        "--job_id", app_id,
        "--job_class", shape,
        "--job_run_time", str(JOB_RUN_TIME_MIN),
        "--job_parallelization", pds,
        "--job_query_number", str(query),
        "--job_window_size", "1000",
        "--job_window_slide_size", "1000",
        "--job_sample", "0",
        "--job_iteration", "0",
        "--producer_event_per_second", str(rate),
        "--hardware_type", "m510",
        "--num_nodes", "6",
        "--num_slots", "16",
        "--enumeration_strategy", strategy,
    ]
    subprocess.run(getprom_cmd, cwd=str(out_dir), check=False)

    eval_files = sorted(out_dir.glob(f"{shape}/evaluationMetrics-*.json"), key=lambda p: p.stat().st_mtime)
    if not eval_files:
        log(f"  NO METRICS FILE WRITTEN: {key}")
        log_manifest(key, status="no_metrics_file", app_id=app_id, attempt=attempt)
        return "no_metrics_file"

    latest = eval_files[-1]
    metrics = json.loads(latest.read_text())
    e2e = metrics.get("endToEndLatency", {}).get("end_to_end_latency_seconds_50th")
    throughput = metrics.get("sparkStreamingMetrics", {}).get("processing_rate")
    status = "ok" if (e2e is not None and e2e > 0 and throughput is not None and float(throughput) > 0) else "null_output"
    log(f"  DONE {key} (attempt {attempt}): status={status} e2e_latency_50={e2e} processing_rate={throughput}")
    log_manifest(key, status=status, app_id=app_id, attempt=attempt, file=str(latest), e2e_latency_50=e2e, processing_rate=throughput)
    return status


if __name__ == "__main__":
    strategies = sys.argv[1:] or ["ds2", "dhalion"]
    for strategy in strategies:
        for shape in SHAPES:
            for rate in RATES:
                try:
                    run_one(strategy, shape, rate)
                except Exception as e:
                    log(f"TOP-LEVEL ERROR on {strategy}/{shape}/{rate}: {e!r}")
                    log_manifest(f"{strategy}_e2e|{shape}|{rate}", status=f"error:{e!r}")
                time.sleep(COOLDOWN_S)
    log("E2E_RERUN_ALL_DONE")
