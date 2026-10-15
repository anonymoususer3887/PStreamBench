"""
Non-uniform per-operator parallelism benchmarking for Spark -- Random, DS2,
and Dhalion, against the real CloudLab cluster, mirroring
run_strategy_sweep_risingwave.py's shape but adapted to Spark's job-creation
API instead of raw SQL submission.

Key difference from RisingWave: unlike RisingWave (at most 3 of 21
workloads had more than one controllable stage; the rest had exactly one),
every Spark job here has multiple genuinely independently-controllable
operators (each its own .repartition(n) call or, for a shuffle stage,
spark.sql.shuffle.partitions -- see each job's own pd[] indexing in
PStreamBench_workload_generator/spark/dsp_spark_jobs/.../*.java). Source
(first slot) and sink (last slot) stay pinned at 2/1 -- same convention
run_sweep.py's build_pd_array() and the RisingWave strategy sweep both
already use -- only interior slots are strategy-controlled.

Submission mechanism: unlike RisingWave (where this project had to
orchestrate SQL-submit -> wait -> collect-metrics -> drop as separate
steps), a single POST /infra/jobcreate/<cluster_id> call with
job_enumeration_strategy="Custom" already does all of that server-side --
submit, sleep job_run_time minutes, collect metrics via getprom_spark.py,
check termination, kill if needed (see infra/runner.py's JobRunner.run()/
get_logs()/is_terminated()/terminate() chain, and job_automate.py for the
proven usage pattern this reuses). This script's job is just to build the
right payload per combo (using parallelism_strategies.py the same way the
RisingWave sweep does) and relocate the resulting files.

WORKLOADS below was built by reading every job's actual pd[] indexing in
its .java source directly (not inferred from job_pds length alone -- two
real discrepancies were caught doing this: Trending Topics has an extra
"topicextractor" metrics-only observe() checkpoint with no corresponding
pd[] slot at all, and Machine Outlier's pd[3] is parsed into no variable
anywhere, i.e. a dead slot the job never reads). interior_stage_names uses
a "_UNOBSERVED" placeholder for any interior slot with no real
operatorMetrics entry -- load_stage_metrics_spark() already handles a
missing stage with neutral defaults (DS2/Dhalion leave that slot
unchanged from baseline rather than guessing), so no special-casing is
needed at the call site for those slots.

Linear Road Q3/Q4 are excluded entirely: their pd[] array is exactly
[source, mapper] (2 slots) -- both pinned by the source=2/sink=1
convention, leaving zero interior positions for any strategy to control at
all, matching run_sweep.py's own documented treatment of these two ("no
parallelism dimension... run once per event rate instead of once per
(event rate, parallelism) pair").

Scope confirmed explicitly (2026-08-25): the 20 workload/query combos that
already have real Spark job code, not all 21 RisingWave combos -- Smart
Grid Q2, Log Processing Q2, TPCH Q2, and Google Cloud Monitoring Q2 have no
Spark implementation at all (checked directly: no query-number branching
in SmartGridJob.java/LogsAnalyzer.java/TPCH.java/GoogleCloudMonitoring.java),
not just a missing config file.

Usage:
    python3 run_strategy_sweep_spark.py run --dry-run
    python3 run_strategy_sweep_spark.py run --strategy random
    python3 run_strategy_sweep_spark.py run --strategy ds2
    python3 run_strategy_sweep_spark.py run --strategy dhalion
    python3 run_strategy_sweep_spark.py run --event-rate 1000000  # all three strategies
    python3 run_strategy_sweep_spark.py status
"""
import glob
import json
import os
import time
from pathlib import Path

import click
import requests

import parallelism_strategies as strat

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = SCRIPT_DIR / "configs"
CONTROLLER_DIR = SCRIPT_DIR.parent
MANIFEST_PATH = SCRIPT_DIR / "manifest_strategy_spark.json"

CONTROLLER_URL = "http://localhost:8000"
CLUSTER_ID = "1d4d002d-3104-403d-a986-46552c460356"  # spark-strategy-sweep, ms0932 master

RANDOM_LEVELS = [2, 4, 8, 16, 32, 40]
RANDOM_SAMPLES = 10
RANDOM_SEED = 42
BASELINE_EVENT_RATE = 100_000
JOB_RUN_TIME_MINUTES = 5  # matches run_sweep.py's own RUN_TIME_MINUTES convention
GAP_SECONDS = 60
JOB_CREATE_TIMEOUT_SECONDS = 900  # run_time (5min) + producer/ansible/metrics/kill overhead
ENGINE_NAME = "spark"

UNOBSERVED = "_UNOBSERVED"  # placeholder stage name with no operatorMetrics entry
                             # (load_stage_metrics_spark returns neutral defaults for
                             # any name missing from operatorMetrics -- DS2/Dhalion
                             # leave that slot unchanged from baseline, same fallback
                             # the RisingWave adapter already established)

# workload_key (used for the dsp_be/<workload_key>/ output folder -- matches
# job_class exactly except where two query variants share one job_class, in
# which case the query number is appended to keep them apart, same reason
# RisingWave distinguished "Ad Analytics" from "Ad Analytics Q1")
#   -> (job_class, job_query_name, config_file, interior_stage_names)
#
# interior_stage_names is verified against each job's real pd[] indexing
# (see module docstring) -- position 0 (source) and position -1 (sink) are
# never in this list, they're always pinned at 2/1.
WORKLOADS = {
    "Word Count":                 ("Word Count", "1", "wordcount_q1",        ["tokenizer", "counter"]),
    "Ad Analytics Q1":            ("Ad Analytics", "1", "adanalytics_q1",    ["clicksparser", "impressionsparser", "clickstag" + UNOBSERVED, "impressionstag" + UNOBSERVED, "window"]),
    "Ad Analytics Q2":            ("Ad Analytics", "2", "adanalytics_q2",    ["parser", "counter", "rollingCTR"]),
    "Bargain Index":              ("Bargain Index", "1", "bargainindex_q1", ["parser", "vwap", "bargain"]),
    "Click Analytics Q1":         ("Click Analytics", "1", "clickanalytics_q1", ["parser", "repeatvisit", "reduce"]),
    "Click Analytics Q2":         ("Click Analytics", "2", "clickanalytics_q2", ["parser", "geo"]),
    "Click Analytics Q3":         ("Click Analytics", "3", "clickanalytics_q3", ["parser", "repeatvisit", "reduce"]),
    "Google cloud Monitoring":    ("Google cloud Monitoring", "1", "gcm_q1", ["parser", "window"]),
    "Linear Road Q1":             ("Linear Road", "1", "linearroad_q1",      ["mapper"]),
    "Linear Road Q2":             ("Linear Road", "2", "linearroad_q2",      ["mapper"]),
    # Linear Road Q3/Q4 deliberately excluded -- see module docstring.
    "Log Processing":             ("Log Processing", "1", "logprocessing_q1", ["parser", "counter"]),
    "Machine Outlier":            ("Machine Outlier", "1", "machineoutlier_q1", ["parser", "outlier", "unused_slot3" + UNOBSERVED]),
    "Sentiment Analysis":         ("Sentiment Analysis", "1", "sentiment_q1", ["parser", "analyser"]),
    "Smart Grid":                 ("Smart Grid", "1", "smartgrid_q1",        ["parser", "window"]),
    "Spike Detection":            ("Spike Detection", "1", "spikedetection_q1", ["parser", "average", "spikedetector"]),
    "TPCH":                       ("TPCH", "1", "tpch_q1",                   ["parser", "filter", "flatmapper", "counter"]),
    "Traffic Monitoring":         ("Traffic Monitoring", "1", "trafficmonitoring_q1", ["parser", "roadmatcher", "avgspeed"]),
    "Trending Topics":            ("Trending Topics", "1", "trendingtopics_q1", ["parser", "counter", "filter"]),
}


def log(msg):
    print(f"[run_strategy_sweep_spark] {msg}", flush=True)


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def strategy_key(workload_key, strategy_name, tag, event_rate):
    return f"{strategy_name}|{workload_key}|{tag}|{event_rate}"


def strategy_folder(job_class, strategy_name):
    # <job_class>/spark/<strategy>/ -- keeps this isolated from the existing
    # flat uniform-sweep data (run_sweep.py never used a strategy/engine
    # subfolder) and from any future Flink/Storm/Kafka Streams strategy runs
    # landing in the same <job_class>/ folder later.
    folder = CONTROLLER_DIR / job_class / ENGINE_NAME / strategy_name
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _check_core_metrics_present(metrics_file):
    """Same "file exists is not the same as file is usable" gate the
    RisingWave sweep added (bug -- see BUGS_AND_FIXES.md's null-metrics
    validation entries) -- adapted to Spark's own evaluationMetrics shape.
    """
    with open(metrics_file) as fh:
        metrics = json.load(fh)
    checks = {
        "queryMetrics.kafka_throughput.throughput": metrics.get("queryMetrics", {}).get("kafka_throughput", {}).get("throughput"),
        "sparkStreamingMetrics.processing_rate": metrics.get("sparkStreamingMetrics", {}).get("processing_rate"),
        "hardwareMetrics.node_memory_98": metrics.get("hardwareMetrics", {}).get("node_memory_98"),
    }
    for path, value in checks.items():
        if value is None:
            return path
    return None


def move_new_metrics_files(job_class, strategy_name, before_eval, before_meta):
    """Same before/after-snapshot relocation pattern as the RisingWave
    strategy sweep. Validates core fields before moving (see
    _check_core_metrics_present) -- a bad pair is deleted rather than
    relocated, matching run_sweep_risingwave.py's null_core_metrics gate.
    """
    flat_folder = CONTROLLER_DIR / job_class
    after_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    after_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))
    new_eval = sorted(after_eval - before_eval, key=os.path.getmtime)
    new_meta = sorted(after_meta - before_meta, key=os.path.getmtime)

    if not new_eval:
        return new_eval, None, None

    latest_eval = new_eval[-1]
    bad_field = _check_core_metrics_present(latest_eval)
    if bad_field:
        log(f"  METRICS FILE HAS NULL CORE FIELD ({bad_field}), discarding: {latest_eval}")
        for f in new_eval + new_meta:
            os.remove(f)
        return new_eval, "null_core_metrics", None

    dest = strategy_folder(job_class, strategy_name)
    moved_file = None
    for f in new_eval + new_meta:
        target = dest / Path(f).name
        os.rename(f, target)
        if f == latest_eval:
            moved_file = str(target)
    return new_eval, None, moved_file


def load_config_template(config_file):
    with open(CONFIGS_DIR / f"{config_file}.json") as f:
        return json.load(f)


def submit_job(job_class, job_query_name, config_file, full_pds, event_rate, dry_run=False):
    """One submit-wait-collect-kill cycle via POST /infra/jobcreate/<cluster_id>
    with job_enumeration_strategy="Custom" and a single-line job_custom_strategy
    -- the whole lifecycle (submit, sleep job_run_time, collect metrics via
    getprom_spark.py, check termination, kill) happens server-side in this one
    blocking call (see infra/runner.py's JobRunner + module docstring above).
    """
    template = load_config_template(config_file)
    payload = dict(template)
    payload["job_enumeration_strategy"] = "Custom"
    payload["job_custom_strategy"] = ",".join(str(p) for p in full_pds)
    payload["job_pds"] = []
    payload["job_class"] = job_class
    payload["job_query_name"] = job_query_name
    payload["job_class_input_type"] = "Kafka"
    payload["job_class_input"] = event_rate
    payload["job_class_input_min"] = event_rate
    payload["job_class_input_max"] = event_rate
    payload["job_run_time"] = JOB_RUN_TIME_MINUTES
    payload["job_iterations"] = 1
    payload["num_of_times_job_run"] = 1
    payload["job_iteration_strategy"] = "From Top"

    if dry_run:
        log(f"  DRY RUN would POST: job_class={job_class} query={job_query_name} "
            f"pds={full_pds} rate={event_rate}")
        return {"status": "dry_run"}

    resp = requests.post(
        f"{CONTROLLER_URL}/infra/jobcreate/{CLUSTER_ID}",
        json=payload, timeout=JOB_CREATE_TIMEOUT_SECONDS,
    )
    if resp.status_code != 200:
        return {"status": f"http_{resp.status_code}: {resp.text[:300]}"}
    return {"status": "ok"}


def run_one(job_class, query_number, config_file, full_pds, event_rate, strategy_name, dry_run=False):
    flat_folder = CONTROLLER_DIR / job_class
    before_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    before_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))

    result = submit_job(job_class, query_number, config_file, full_pds, event_rate, dry_run=dry_run)
    if dry_run or result.get("status") != "ok":
        return result

    new_eval, bad_status, moved_file = move_new_metrics_files(job_class, strategy_name, before_eval, before_meta)
    if bad_status:
        result["status"] = bad_status
        return result
    if not new_eval:
        log(f"  NO NEW METRICS FILE WRITTEN after submission")
        result["status"] = "no_metrics_file"
        return result

    result["file"] = moved_file
    log(f"  saved -> {moved_file}")
    return result


def run_random(manifest, event_rate, dry_run=False):
    for workload_key, (job_class, query_number, config_file, stage_names) in WORKLOADS.items():
        num_stages = len(stage_names)
        assignments = strat.random_strategy(RANDOM_LEVELS, num_stages, RANDOM_SAMPLES, seed=RANDOM_SEED)
        for i, assignment in enumerate(assignments):
            key = strategy_key(workload_key, "random", f"s{i}", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            full_pds = [2] + assignment + [1]
            log(f"[random] {workload_key} sample {i}/{len(assignments)} rate={event_rate} -> {full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, full_pds, event_rate, "random", dry_run=dry_run)
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[key] = result.get("status", "unknown")
            save_manifest(manifest)
            if not dry_run:
                log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
                time.sleep(GAP_SECONDS)


def run_baseline_then_nudge(manifest, event_rate, dry_run=False):
    """Shared baseline-then-nudge flow for DS2 and Dhalion -- one baseline
    deploy per workload covers both strategies, same accounting the
    RisingWave strategy sweep uses (1 baseline + 2 proposed runs).
    """
    for workload_key, (job_class, query_number, config_file, stage_names) in WORKLOADS.items():
        num_stages = len(stage_names)
        baseline_assignment = [strat.MIN_PARALLELISM] * num_stages
        baseline_full_pds = [2] + baseline_assignment + [1]
        baseline_key = strategy_key(workload_key, "uniform", "baseline", event_rate)
        baseline_file = None

        if manifest.get(baseline_key) == "ok":
            log(f"SKIP (already done): {baseline_key}")
            baseline_file = manifest.get(baseline_key + "|file")
        else:
            log(f"[baseline] {workload_key} rate={event_rate} -> {baseline_full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, baseline_full_pds, event_rate, "uniform", dry_run=dry_run)
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[baseline_key] = result.get("status", "unknown")
            if result.get("status") == "ok":
                baseline_file = result["file"]
                manifest[baseline_key + "|file"] = baseline_file
            save_manifest(manifest)
            if not dry_run:
                log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
                time.sleep(GAP_SECONDS)

        if dry_run or not baseline_file:
            log(f"  no baseline file available for {workload_key} -- skipping DS2/Dhalion nudge")
            continue

        stage_metrics = strat.load_stage_metrics_spark(baseline_file, stage_names)

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (event_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            full_pds = [2] + list(assignment) + [1]
            log(f"[{strategy_name}] {workload_key} rate={event_rate} -> {full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, full_pds, event_rate, strategy_name, dry_run=dry_run)
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[key] = result.get("status", "unknown")
            save_manifest(manifest)
            if not dry_run:
                log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
                time.sleep(GAP_SECONDS)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--strategy", type=click.Choice(["random", "ds2", "dhalion", "all"]), default="all")
@click.option("--event-rate", type=int, default=BASELINE_EVENT_RATE)
@click.option("--dry-run", is_flag=True, default=False)
def run(strategy, event_rate, dry_run):
    manifest = load_manifest()
    if strategy in ("random", "all"):
        run_random(manifest, event_rate, dry_run=dry_run)
    if strategy in ("ds2", "dhalion", "all"):
        run_baseline_then_nudge(manifest, event_rate, dry_run=dry_run)
    log("\n=== DONE ===")


@cli.command()
def status():
    manifest = load_manifest()
    ok = sum(1 for v in manifest.values() if v == "ok")
    log(f"{ok} entries ok, {len(manifest) - ok} attempted-but-not-ok")
    for k, v in sorted(manifest.items()):
        log(f"  {k}: {v}")


if __name__ == "__main__":
    cli()
