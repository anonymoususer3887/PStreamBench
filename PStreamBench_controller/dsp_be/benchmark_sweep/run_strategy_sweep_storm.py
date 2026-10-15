"""
Non-uniform per-operator parallelism benchmarking for Storm -- Random, DS2,
and Dhalion, against the real CloudLab cluster, mirroring
run_strategy_sweep_spark.py's shape but adapted to Storm's own topology
model and job-creation payload.

Submission mechanism: same as Spark/RisingWave -- a single POST
/infra/jobcreate/<cluster_id> call with job_enumeration_strategy="Custom"
does submit -> sleep job_run_time minutes -> collect metrics via
getprom_storm.py -> check termination -> kill, all server-side (see
infra/runner.py's JobRunner._run_storm_job()/get_logs()/is_terminated()/
terminate()). This script's job is just to build the right payload per
combo and relocate the resulting files, exactly like the Spark script.

SOURCE/SINK PINNING: unlike RisingWave/Spark (source=2, sink=1), Storm's
own cluster uses source=10, sink=1 -- confirmed empirically (2026-09-02)
by inspecting every workload's real job_parallelization string in this
project's *existing* Storm artifacts (e.g. word-count/m510/storm shows
"10,72,72,1", ad-analytics shows "10,40,40,40,40,40,1", every single one
of 12 checked workloads uses source=10) -- this is a genuinely different
convention from the other 4 engines, not a mistake to "fix" to match them.
Only interior slots are strategy-controlled, same as every other engine.

Config templates: this script reuses the SAME configs/*.json files
run_sweep.py/run_strategy_sweep_spark.py already use (job_window_size,
job_window_slide_size, job_threshold, job_class, job_query_name are
workload-intrinsic values, not engine-specific -- confirmed by inspecting
those files, none of them mention Spark or any engine at all). The one
field NOT reused from those templates is job_pds's *length*: Linear Road
Q1/Q2 have only 3 pd slots in Spark's config (Structured Streaming fuses
the mapper+formatter stages into one .map() call) but Storm keeps them as
2 genuinely separate bolts (toll-notification-mapper AND
toll-notification-formatter, confirmed directly against
LinearRoadTopology.java) -- 5 real pd slots, not 3. submit_job() always
clears payload["job_pds"] and uses job_custom_strategy instead, so the
template's own job_pds length never actually matters at submission time;
WORKLOADS below carries Storm's own verified stage list independent of
whatever the Spark config's job_pds happens to contain.

WORKLOADS was built by reading every workload's actual Storm Topology.java
source directly (LinearRoadTopology.java, ClickAnalyticsTopology.java,
etc under PStreamBench_workload_generator/storm/dsp_storm_jobs/src/main/
java/com/kom/dsp/), not inferred from getprom_storm.py's own bolt-name
dispatch lists (which turned out to be harmless supersets in most cases,
but were missing Linear Road Q2's real bolt names entirely -- fixed in
getprom_storm.py directly, 2026-09-02) or from Spark's job_pds length
(see above). Two shared-index cases found, both handled by picking ONE
representative bolt name for that pd slot (the other bolt sharing the
same index always gets the identical parallelism value, so a second
metrics-adapter entry would be redundant, not wrong):
  - Ad Analytics Q1: "join" and "rolling-ctr" both read
    getParallelismDegree(5) -- "join" is used as the interior stage name
    for that slot.
  - Trending Topics: "topic-extractor-bolt" and
    "rolling-counter-sliding-window-bolt" both read
    getParallelismDegree(2) -- "topic-extractor-bolt" is used.
Machine Outlier's pd[3] is a dead slot never read by the job (same
finding as Spark's WORKLOADS) -- placeholder name with the _UNOBSERVED
suffix, same convention as run_strategy_sweep_spark.py.

Storm genuinely has per-bolt busy_ratio/backpressure_ratio (unlike Spark's
job-wide proxy) -- see load_stage_metrics_storm()'s docstring in
parallelism_strategies.py, and getprom_storm.py's getBoltMetrics() for how
they're derived from Storm's native "capacity" metric. That metric's
Prometheus exporter (storm_exporter.py) had "capacity" and
"processLatency" swapped at the source -- found and fixed 2026-09-02,
*before* any of this script's DS2/Dhalion runs, otherwise every nudge
computed from real cluster data would have silently been using process
latency samples labeled as capacity.

Scope: 18 workload/query combos -- Linear Road Q3/Q4 excluded (2-slot pd
arrays even in Storm's own topology -- daily-expenditure-bolt/logger-bolt
have no distinct sink stage in the same way Q1/Q2 do), matching the
project-wide convention already established for RisingWave/Spark's own
strategy sweeps (not just an artifact of RisingWave's fragment fusion).
Click Analytics Q2 (geo-based) is included despite this project's
existing Storm uniform-sweep artifacts never having run it (only
count-based/time-based exist under click-analytics/m510/storm/) -- this
strategy sweep is independent of that old baseline data (DS2/Dhalion only
need their own internal baseline run), and RisingWave/Spark's strategy
sweeps both cover all 3 Click Analytics variants, so Storm does too here
for consistency.

Usage:
    python3 run_strategy_sweep_storm.py run --dry-run
    python3 run_strategy_sweep_storm.py run --strategy random
    python3 run_strategy_sweep_storm.py run --strategy ds2
    python3 run_strategy_sweep_storm.py run --strategy dhalion
    python3 run_strategy_sweep_storm.py run --event-rate 1000000  # all three strategies
    python3 run_strategy_sweep_storm.py status
"""
import glob
import json
import os
import subprocess
import time
from pathlib import Path

import click
import requests

import parallelism_strategies as strat

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = SCRIPT_DIR / "configs"
CONTROLLER_DIR = SCRIPT_DIR.parent
MANIFEST_PATH = SCRIPT_DIR / "manifest_strategy_storm.json"

CONTROLLER_URL = "http://localhost:8000"
CLUSTER_ID = "c76ffb60-13a7-42ba-a3fa-6aee1896d12b"  # storm-strategy-sweep, ms0932 master

SOURCE_PARALLELISM = 10  # Storm's own convention -- see module docstring
SINK_PARALLELISM = 1

MASTER_HOST = "ms0932.utah.cloudlab.us"
MASTER_SSH_USER = "pagnih03"
EXPORTER_METRICS_URL = f"http://{MASTER_HOST}:9093/metrics"
EXPORTER_RESTART_CMD = (
    "sudo pkill -9 -f storm_exporter; sleep 2; "
    "cd /home/playground/apache-storm-2.6.2/bin && "
    "setsid nohup ./storm_exporter.sh > /tmp/storm_exporter.log 2>&1 < /dev/null &"
)

RANDOM_LEVELS = [2, 4, 8, 16, 32, 40]
RANDOM_SAMPLES = 10
RANDOM_SEED = 42
BASELINE_EVENT_RATE = 100_000
JOB_RUN_TIME_MINUTES = 5
GAP_SECONDS = 60
JOB_CREATE_TIMEOUT_SECONDS = 900
ENGINE_NAME = "storm"

UNOBSERVED = "_UNOBSERVED"  # placeholder stage name with no operatorMetrics entry
                             # (load_stage_metrics_storm returns neutral defaults for
                             # any name missing from operatorMetrics -- DS2/Dhalion
                             # leave that slot unchanged from baseline)

# workload_key -> (job_class, job_query_name, config_file, interior_bolt_names)
# interior_bolt_names is verified against each job's real Storm
# Topology.java getParallelismDegree() indexing (see module docstring) --
# position 0 (source) and the last position (sink) are never in this list,
# they're always pinned at SOURCE_PARALLELISM/SINK_PARALLELISM.
WORKLOADS = {
    "Linear Query":               ("Linear Query", "1", "linearquery_q1",    ["filter1", "window-agg", "filter2"]),
    "Word Count":                 ("Word Count", "1", "wordcount_q1",        ["tokenizer", "counter"]),
    "Ad Analytics Q1":            ("Ad Analytics", "1", "adanalytics_q1",    ["clicks-parser", "impressions-parser", "clicks-counter", "impressions-counter", "join"]),
    "Ad Analytics Q2":            ("Ad Analytics", "2", "adanalytics_q2",    ["parser", "counter", "rolling-ctr"]),
    "Bargain Index":              ("Bargain Index", "1", "bargainindex_q1", ["parser-bolt", "vwap-calculator-bolt", "bargain-index-bolt"]),
    "Click Analytics Q1":         ("Click Analytics", "1", "clickanalytics_q1", ["parse-click-log-bolt", "repeat-visit-operator-bolt", "sum-reducer-bolt"]),
    "Click Analytics Q2":         ("Click Analytics", "2", "clickanalytics_q2", ["parse-click-log-bolt", "geography-operator-bolt"]),
    "Click Analytics Q3":         ("Click Analytics", "3", "clickanalytics_q3", ["parse-click-log-bolt", "repeat-visit-operator-bolt", "sum-reducer-bolt"]),
    "Google cloud Monitoring":    ("Google cloud Monitoring", "1", "gcm_q1", ["task-event-parser-bolt", "cpu-per-category-bolt"]),
    "Linear Road Q1":             ("Linear Road", "1", "linearroad_q1",      ["vehicleEvent-parser-bolt", "toll-notification-mapper", "toll-notification-formatter"]),
    "Linear Road Q2":             ("Linear Road", "2", "linearroad_q2",      ["vehicleEvent-parser-bolt", "accident-notification", "accident-notification-formatter"]),
    # Linear Road Q3/Q4 deliberately excluded -- see module docstring.
    "Log Processing":             ("Log Processing", "1", "logprocessing_q1", ["logParserBolt", "volumeCounterBolt"]),
    "Machine Outlier":            ("Machine Outlier", "1", "machineoutlier_q1", ["machine-usage-parser-bolt", "machine-outlier-bolt", "unused_slot3" + UNOBSERVED]),
    "Sentiment Analysis":         ("Sentiment Analysis", "1", "sentiment_q1", ["parse-tweet-bolt", "analyze-sentiment-bolt"]),
    "Smart Grid":                 ("Smart Grid", "1", "smartgrid_q1",        ["house-event-parser-bolt", "global-median-bolt"]),
    "Spike Detection":            ("Spike Detection", "1", "spikedetection_q1", ["parser-bolt", "average-calculator-bolt", "spike-detector-bolt"]),
    "TPCH":                       ("TPCH", "1", "tpch_q1",                   ["tpchEventParserBolt", "filterCalculatorBolt", "priorityMapperBolt", "sumBolt"]),
    "Traffic Monitoring":         ("Traffic Monitoring", "1", "trafficmonitoring_q1", ["ParserBolt", "MapMatcherBolt", "AvgSpeedCalculatorBolt"]),
    "Trending Topics":            ("Trending Topics", "1", "trendingtopics_q1", ["twitter-parser-bolt", "topic-extractor-bolt", "popularity-detector-bolt"]),
}


# Job classes whose tuples are generated in-process by the topology
# (no Kafka producer). Everything else reads from Kafka as before.
SYNTHETIC_JOB_CLASSES = {"Linear Query"}


def log(msg):
    print(f"[run_strategy_sweep_storm] {msg}", flush=True)


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def strategy_key(workload_key, strategy_name, tag, event_rate):
    return f"{strategy_name}|{workload_key}|{tag}|{event_rate}"


def strategy_folder(job_class, strategy_name):
    # <job_class>/storm/<strategy>/ -- keeps this isolated from the existing
    # flat uniform-sweep data and from the Spark/RisingWave strategy runs
    # landing in the same <job_class>/ folder under a different engine name.
    folder = CONTROLLER_DIR / job_class / ENGINE_NAME / strategy_name
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _check_core_metrics_present(metrics_file):
    """Same "file exists is not the same as file is usable" gate the
    RisingWave/Spark sweeps use -- adapted to getprom_storm.py's own
    evaluationMetrics shape (job_name/operatorMetrics/queryMetrics/
    hardwareMetrics, see getTopologyMetrics()).
    """
    with open(metrics_file) as fh:
        metrics = json.load(fh)
    checks = {
        "queryMetrics.throughput_avg": metrics.get("queryMetrics", {}).get("throughput_avg"),
        "hardwareMetrics.node_memory_98": metrics.get("hardwareMetrics", {}).get("node_memory_98"),
    }
    for path, value in checks.items():
        if value is None:
            return path
    return None


def topology_folder_name(job_class):
    """getprom_storm.py writes into a folder named after the LIVE Storm
    topology name (job_class.replace(" ", ""), e.g. "WordCount"), not the
    human-readable job_class with spaces -- unlike getprom_spark.py/
    getprom_risingwave.py, which take an explicit --job_class CLI arg and
    use it verbatim as the folder name. This is because Storm/Flink's
    reporting script queries the topology's own live "name" field off the
    cluster's REST API to build the folder path, while Spark/Kafka
    Streams/RisingWave get job_class passed down explicitly (see
    infra/runner.py's get_logs(): --job_class is only added to
    script_arguments for those 3 engines, never for Storm/Flink). Confirmed
    live (2026-09-02): every "no_metrics_file" result from this script's
    first smoke-test attempts was actually a false negative -- the real
    evaluationMetrics-*.json files were sitting in dsp_be/WordCount/ the
    whole time, just not where this function was looking.
    """
    return job_class.replace(" ", "")


def move_new_metrics_files(job_class, strategy_name, before_eval, before_meta):
    """Same before/after-snapshot relocation pattern as the RisingWave/Spark
    strategy sweeps. Validates core fields before moving -- a bad pair is
    deleted rather than relocated.
    """
    flat_folder = CONTROLLER_DIR / topology_folder_name(job_class)
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


def ensure_exporter_healthy():
    """storm_exporter.py's Prometheus client Gauges are labeled by
    TopologyId, which is DIFFERENT on every single submission (Storm's own
    "<name>-<counter>-<timestamp>" convention, see this module's docstring
    on filename uniqueness) -- every gauge's label set only ever grows,
    never shrinks, across the lifetime of the exporter process. Confirmed
    live (2026-09-04): after ~220 real submissions since the exporter was
    last started, its /metrics HTTP endpoint hung completely (15s+ timeout,
    zero response) while the underlying polling loop against Storm's REST
    API kept running fine in the background ("caught metrics" still
    printing every second) -- classic prometheus_client HTTP server
    thread deadlock under an ever-growing response body, not a crash of
    the process itself, so a plain "is it still running" ps check would
    have missed it entirely. This caused 6 consecutive workloads' baseline
    runs (and their dependent DS2/Dhalion nudges) to silently fail with
    no_metrics_file right at the end of the 100k sweep pass.

    Called before every single submission (cheap -- a few hundred ms) so
    the sweep self-heals instead of silently degrading for the remaining
    hours/days of the 500k/1M passes (~234 more unique topology
    submissions each, so this WILL recur without a periodic reset).
    """
    try:
        resp = requests.get(EXPORTER_METRICS_URL, timeout=8)
        if resp.status_code == 200:
            return
    except requests.exceptions.RequestException:
        pass
    log("  storm_exporter /metrics unresponsive -- restarting on the master...")
    subprocess.run(
        ["ssh", "-o", "ConnectTimeout=8", f"{MASTER_SSH_USER}@{MASTER_HOST}", EXPORTER_RESTART_CMD],
        timeout=30,
    )
    time.sleep(8)
    try:
        resp = requests.get(EXPORTER_METRICS_URL, timeout=8)
        log(f"  storm_exporter restart {'succeeded' if resp.status_code == 200 else 'still unhealthy: ' + str(resp.status_code)}")
    except requests.exceptions.RequestException as e:
        log(f"  storm_exporter restart verification failed: {e!r}")


def load_config_template(config_file):
    with open(CONFIGS_DIR / f"{config_file}.json") as f:
        return json.load(f)


def submit_job(job_class, job_query_name, config_file, full_pds, event_rate, dry_run=False):
    """One submit-wait-collect-kill cycle via POST /infra/jobcreate/<cluster_id>
    with job_enumeration_strategy="Custom" -- the whole lifecycle happens
    server-side in this one blocking call (see infra/runner.py's
    JobRunner._run_storm_job()).
    """
    template = load_config_template(config_file)
    payload = dict(template)
    payload["job_enumeration_strategy"] = "Custom"
    payload["job_custom_strategy"] = ",".join(str(p) for p in full_pds)
    payload["job_pds"] = []
    payload["job_class"] = job_class
    payload["job_query_name"] = job_query_name
    payload["job_class_input_type"] = "Synthetic" if job_class in SYNTHETIC_JOB_CLASSES else "Kafka"
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

    ensure_exporter_healthy()

    resp = requests.post(
        f"{CONTROLLER_URL}/infra/jobcreate/{CLUSTER_ID}",
        json=payload, timeout=JOB_CREATE_TIMEOUT_SECONDS,
    )
    if resp.status_code != 200:
        return {"status": f"http_{resp.status_code}: {resp.text[:300]}"}
    return {"status": "ok"}


def run_one(job_class, query_number, config_file, full_pds, event_rate, strategy_name, dry_run=False):
    flat_folder = CONTROLLER_DIR / topology_folder_name(job_class)
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
            full_pds = [SOURCE_PARALLELISM] + assignment + [SINK_PARALLELISM]
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
    deploy per workload covers both strategies.
    """
    for workload_key, (job_class, query_number, config_file, stage_names) in WORKLOADS.items():
        num_stages = len(stage_names)
        baseline_assignment = [strat.MIN_PARALLELISM] * num_stages
        baseline_full_pds = [SOURCE_PARALLELISM] + baseline_assignment + [SINK_PARALLELISM]
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

        stage_metrics = strat.load_stage_metrics_storm(baseline_file, stage_names)

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (event_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            full_pds = [SOURCE_PARALLELISM] + list(assignment) + [SINK_PARALLELISM]
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
