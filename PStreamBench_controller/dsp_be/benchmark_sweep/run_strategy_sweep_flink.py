"""
Non-uniform per-operator parallelism benchmarking for Flink -- Random, DS2,
and Dhalion, against the real CloudLab cluster, mirroring
run_strategy_sweep_storm.py's shape but adapted to Flink's own job model,
folder-naming convention, and metrics-file shape.

Submission mechanism: same as every other engine -- a single POST
/infra/jobcreate/<cluster_id> call with job_enumeration_strategy="Custom"
does submit -> wait -> collect metrics via getprom.py -> terminate, all
server-side (see infra/runner.py's JobRunner._run_flink_job()). That
function underwent a real structural-race fix (2026-09-13, see its own
docstring): Flink's client process self-cancels the job after job_run_time
independently of the blocking Ansible producer play, so job-id discovery
now polls concurrently via a background thread started *before* the
blocking playbook call, not sequentially after.

SOURCE/SINK PINNING: matches Storm's convention exactly -- source=10,
sink=1 -- confirmed empirically (2026-09-13) against this project's own
pre-existing Flink uniform-sweep artifacts (PStreamBench_benchmark_data/
artifacts/{linear-road,ad-analytics}/m510/flink/*/metaInfo-*.json all show
job_parallelization starting with "10," and ending with ",1" with 100%
consistency across dozens of samples; word-count's own history is mixed
(some batches used source=40), but 10/1 is still the dominant, and only
cross-workload-consistent, value -- adopted project-wide for this sweep).

LINEAR ROAD'S SINK IS NOT INDEPENDENTLY CONTROLLABLE -- a genuine
structural difference from every other engine, found by reading
LinearRoad.java directly (2026-09-13): the Kafka sink's `.sinkTo(sink)`
call has NO `.setParallelism()` at all, so it silently inherits
`env.setParallelism(pd[0])` (the SOURCE value), not SINK_PARALLELISM=1.
Also, `VehicleEventParser` (the first real operator) is explicitly pinned
to pd[0] too, i.e. source and the first interior stage share one pd slot.
The job's `main()` only ever reads parallelism_degree[0..2] -- there is no
3rd interior slot and no independent sink slot at all. This means Linear
Road Q1/Q2's full_pds has exactly 3 elements here (source + 2 interior),
NOT source+2interior+sink like every other workload -- appending a 4th
"sink" value would be silently ignored by the Java side, so it's simply
omitted rather than left in as harmless-but-misleading padding.

WORKLOADS was built by reading every workload's actual Flink job source
directly (LinearRoad.java, AdsAnalytics.java, etc under
PStreamBench_workload_generator/flink/dsp_flink_jobs/src/main/java/com/kom/
dsp/), matching the meticulous approach used for Storm, NOT inferred from
Storm/Spark's own WORKLOADS (Flink's stage granularity sometimes differs --
e.g. Traffic Monitoring's TrafficEventParser is its own explicit stage in
Flink, unlike Linear Road where the parser fuses into the source slot).
Notable per-workload findings:
  - Ad Analytics Q1: "join" (.join(...)) and "rollingCTR" (the chained
    .filter(...) computing the ratio) are BOTH pinned to the same pd[5] --
    one representative stage name is used for that slot, matching Storm's
    handling of its own equivalent shared-index case.
  - Ad Analytics Q2: NONE of its 3 interior operators (parser/counter/
    rollingCTR) have a matching operatorMetrics entry in getprom.py today
    (getAdanalytics() only recognizes Q1's click/impression/join naming --
    Q2's lookup silently fails and is swallowed by a bare `except Exception:
    pass`). DS2/Dhalion nudges for Q2 therefore always see neutral
    (unobserved) metrics for every stage and degrade to a no-op nudge
    (assignment == baseline) -- Random still works normally since it
    doesn't need real metrics. Documented here rather than silently
    producing misleading "DS2 chose no change" data.
  - Trending Topics: "topic-extractor" and "rolling-counter" share pd[2]
    (both literally lowercase-hyphenated operatorMetrics keys, unlike
    every other workload's PascalCase convention -- confirmed directly
    against getprom.py's own dict literal, not assumed).
  - Machine Outlier: pd[3] is dead (only reachable via a commented-out
    BFPRTAlgorithm code path) -- same finding as Storm/Spark, same
    _UNOBSERVED placeholder-name convention used here.
  - Smart Grid: pd[1] (HouseEventParser) runs on every request but has NO
    operatorMetrics entry in getprom.py at all -- given the _UNOBSERVED
    suffix so DS2/Dhalion leave it unchanged rather than mis-nudging on a
    default-zero reading. Q2 ADDITIONALLY has a genuine dead pd[3] (jumps
    pd[2]->pd[4] in the Java source) -- Q1 uses that same index as its live
    sink pin, so Q2 needs its own _UNOBSERVED placeholder there to keep the
    array long enough to reach the real pd[4] sink.
  - Click Analytics Q2 has one fewer interior stage than Q1/Q3 (no
    "reduce" stage -- GeographyOperator emits directly), so its sink lives
    at pd[3] instead of pd[4]. This isn't a dead-slot case like Smart Grid
    Q2 -- it's simply a shorter real array (4 elements, not 5) -- so no
    placeholder is needed, full_pds is just shorter for this one query.

getprom.py's Flink metrics shape differs from Storm's in one field name:
queryMetrics has no "throughput_avg" key (Flink's own getQueryMetrics()
never computes one) -- the closest analogous field is "throughput_50"
(median records/sec into the Kafka sink) -- confirmed directly against a
real evaluationMetrics-*.json from the concurrent-polling fix's first
successful test run (2026-09-13). hardwareMetrics.node_memory_98 exists
identically to every other engine, reused as-is.

Scope: 18 workload/query combos, Linear Road Q3/Q4 excluded -- same
project-wide convention as every other engine's strategy sweep (see
run_strategy_sweep_storm.py's docstring for why).

Usage:
    python3 run_strategy_sweep_flink.py run --dry-run
    python3 run_strategy_sweep_flink.py run --strategy random
    python3 run_strategy_sweep_flink.py run --strategy ds2
    python3 run_strategy_sweep_flink.py run --strategy dhalion
    python3 run_strategy_sweep_flink.py run --event-rate 1000000  # all three strategies
    python3 run_strategy_sweep_flink.py status
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
MANIFEST_PATH = SCRIPT_DIR / "manifest_strategy_flink.json"

CONTROLLER_URL = "http://localhost:8000"
CLUSTER_ID = "d91a0561-fbe2-4a08-b0e7-70b67a73ac81"  # flink-strategy-sweep, ms1113 master

SOURCE_PARALLELISM = 10  # see module docstring -- matches Storm's own convention
SINK_PARALLELISM = 1

RANDOM_LEVELS = [2, 4, 8, 16, 32, 40]
RANDOM_SAMPLES = 10
RANDOM_SEED = 42
BASELINE_EVENT_RATE = 100_000
JOB_RUN_TIME_MINUTES = 5
GAP_SECONDS = 60
# JOB_RUN_TIME_MINUTES*60 (producer + Flink client both block for this)
# plus a generous buffer for Ansible/JAR-copy/topic-create/metrics-query
# overhead -- 900s measured too tight live (2026-09-13): a real Word Count
# smoke test's full submit-wait-collect-terminate cycle exceeded it even
# though the job itself completed and terminated cleanly server-side, so
# the client saw a spurious ReadTimeout on an otherwise-successful run
# (the resulting evaluationMetrics-*.json was still written server-side,
# just never relocated into its strategy folder since the client-side
# exception short-circuited run_one() before move_new_metrics_files()).
JOB_CREATE_TIMEOUT_SECONDS = JOB_RUN_TIME_MINUTES * 60 + 900
ENGINE_NAME = "flink"

UNOBSERVED = "_UNOBSERVED"  # placeholder stage name with no operatorMetrics entry
                             # (load_stage_metrics_flink returns neutral defaults for
                             # any name missing from operatorMetrics -- DS2/Dhalion
                             # leave that slot unchanged from baseline)

# workload_key -> (job_class, job_query_name, config_file, flink_job_name,
#                   interior_stage_names, has_independent_sink)
# interior_stage_names is verified against each job's real Flink *.java
# main()'s parallelism_degree[N] indexing (see module docstring) -- pd[0]
# (source) is always pinned to SOURCE_PARALLELISM and never in this list.
# has_independent_sink=False means the job has no separate sink
# .setParallelism() call at all (only Linear Road today) -- full_pds omits
# a trailing sink value entirely for those, rather than pinning one that
# would silently be ignored by the Java side.
WORKLOADS = {
    "Word Count":                 ("Word Count", "1", "wordcount_q1", "Flink word count job",
                                    ["Tokenizer", "Counter"], True),
    "Ad Analytics Q1":            ("Ad Analytics", "1", "adanalytics_q1", "Ads Analytics",
                                    ["ClickParser", "ImpressionParser", "ClicksCounter", "ImpressionsCounter", "Join"], True),
    "Ad Analytics Q2":            ("Ad Analytics", "2", "adanalytics_q2", "Ads Analytics",
                                    ["parser" + UNOBSERVED, "counter" + UNOBSERVED, "rollingCTR" + UNOBSERVED], True),
    "Bargain Index":              ("Bargain Index", "1", "bargainindex_q1", "Bargain Index",
                                    ["BargainIndexParser", "BargainIndexVWAP", "BargainIndexCalc"], True),
    "Click Analytics Q1":         ("Click Analytics", "1", "clickanalytics_q1", "Click Analytics",
                                    ["ClickAnalyticsParser", "ClickAnalyticsRepeat", "ClickAnalyticsReduce"], True),
    "Click Analytics Q2":         ("Click Analytics", "2", "clickanalytics_q2", "Click Analytics",
                                    ["ClickAnalyticsParser", "ClickAnalyticsGeo"], True),
    "Click Analytics Q3":         ("Click Analytics", "3", "clickanalytics_q3", "Click Analytics",
                                    ["ClickAnalyticsParser", "ClickAnalyticsRepeat", "ClickAnalyticsReduce"], True),
    "Google cloud Monitoring":    ("Google cloud Monitoring", "1", "gcm_q1", "Google Cloud Monitoring",
                                    ["Parser", "AvgCpuPerCat"], True),
    "Linear Road Q1":             ("Linear Road", "1", "linearroad_q1", "Linear Road",
                                    ["TollNotification", "FormatterTollNotification"], False),
    "Linear Road Q2":             ("Linear Road", "2", "linearroad_q2", "Linear Road",
                                    ["AccidentNotification", "FormatterAccidentNotification"], False),
    # Linear Road Q3/Q4 deliberately excluded -- see module docstring.
    "Log Processing":             ("Log Processing", "1", "logprocessing_q1", "Logs Processing",
                                    ["LogProcessorParser", "LogProcessorVolumeCounter"], True),
    "Machine Outlier":            ("Machine Outlier", "1", "machineoutlier_q1", "Machine Outlier",
                                    ["MachineUsageParser", "OutlierDetector", "unused_slot3" + UNOBSERVED], True),
    "Sentiment Analysis":         ("Sentiment Analysis", "1", "sentiment_q1", "Sentiment Analysis",
                                    ["TwitterParser", "TwitterAnalyzer"], True),
    "Smart Grid Q1":              ("Smart Grid", "1", "smartgrid_q1", "Flink smart grid job",
                                    ["HouseEventParser" + UNOBSERVED, "Globalload"], True),
    "Smart Grid Q2":              ("Smart Grid", "2", "smartgrid_q2", "Flink smart grid job",
                                    ["HouseEventParser" + UNOBSERVED, "Localload", "dead_slot3" + UNOBSERVED], True),
    "Spike Detection":            ("Spike Detection", "1", "spikedetection_q1", "Spike Detection",
                                    ["SpikeParser", "AvgCal", "SpikeDetector"], True),
    "TPCH":                       ("TPCH", "1", "tpch_q1", "TPCH",
                                    ["TpchEventParser", "TpchDataFilter", "TpchFlatMapper", "PriorityCounter"], True),
    "Traffic Monitoring":         ("Traffic Monitoring", "1", "trafficmonitoring_q1", "Traffic Monitoring",
                                    ["TrafficEventParser", "RoadMatcher", "AvgSpeed"], True),
    "Trending Topics":            ("Trending Topics", "1", "trendingtopics_q1", "Trending Topics",
                                    ["twitter-parser", "topic-extractor", "popularity-detector"], True),
}


def log(msg):
    print(f"[run_strategy_sweep_flink] {msg}", flush=True)


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def strategy_key(workload_key, strategy_name, tag, event_rate):
    return f"{strategy_name}|{workload_key}|{tag}|{event_rate}"


def strategy_folder(job_class, strategy_name):
    # <job_class>/flink/<strategy>/ -- keeps this isolated from the existing
    # flat uniform-sweep data and from the other engines' strategy runs
    # landing in the same <job_class>/ folder under a different engine name.
    folder = CONTROLLER_DIR / job_class / ENGINE_NAME / strategy_name
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _check_core_metrics_present(metrics_file):
    """Same "file exists is not the same as file is usable" gate every
    other strategy sweep uses -- adapted to getprom.py's own Flink
    evaluationMetrics shape, which has no queryMetrics.throughput_avg (see
    module docstring) -- throughput_50 is the closest analogous field.
    """
    with open(metrics_file) as fh:
        metrics = json.load(fh)
    checks = {
        "queryMetrics.throughput_50": metrics.get("queryMetrics", {}).get("throughput_50"),
        "hardwareMetrics.node_memory_98": metrics.get("hardwareMetrics", {}).get("node_memory_98"),
    }
    for path, value in checks.items():
        if value is None:
            return path
    return None


def flink_folder_name(flink_job_name):
    """getprom.py writes into a folder named after the LIVE Flink job's own
    self-reported "name" field (job_class.executeAsync("...")'s literal
    argument, e.g. "Flink word count job" -- WITH spaces, and sometimes
    genuinely different text from job_class, e.g. "Ads Analytics" vs the
    dispatch key "Ad Analytics") -- confirmed live 2026-09-13 via a real
    successful test run. See WORKLOADS's own flink_job_name column.
    """
    return flink_job_name


def move_new_metrics_files(flink_job_name, job_class, strategy_name, before_eval, before_meta, dest_job_class=None):
    """Same before/after-snapshot relocation pattern as every other
    engine's strategy sweep. Validates core fields before moving -- a bad
    pair is deleted rather than relocated.

    `dest_job_class` overrides which folder the result lands in, separate
    from `job_class` (which must stay the real dispatch key, e.g. "Smart
    Grid", for submit_job()'s payload). Needed because Smart Grid Q1/Q2
    share one job_class but are NOT the same pipeline -- without this,
    both queries' results land indistinguishably in "Smart Grid/flink/
    <strategy>/" (confirmed live 2026-09-16: the 500k pass did exactly
    this for its first several Smart Grid Q2 samples before this override
    was added, mirroring the same folder-collision already fixed once for
    the 100k backfill via a post-hoc file move -- this makes that fix
    apply going forward instead of needing another manual relocation
    afterward).
    """
    flat_folder = CONTROLLER_DIR / flink_folder_name(flink_job_name)
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

    dest = strategy_folder(dest_job_class or job_class, strategy_name)
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
    with job_enumeration_strategy="Custom" -- the whole lifecycle happens
    server-side in this one blocking call (see infra/runner.py's
    JobRunner._run_flink_job(), including its concurrent-polling fix for
    Flink's self-cancelling client race).
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


# Workloads whose job_class is shared with a sibling query but whose
# results must NOT be commingled in that shared job_class's folder --
# currently only Smart Grid Q2 (shares job_class="Smart Grid" with Q1,
# but is a structurally different pipeline/output). Every other
# multi-query workload family (Ad Analytics, Click Analytics, Linear
# Road) keeps its existing shared-folder convention -- not touched here,
# out of scope for this fix.
DEST_JOB_CLASS_OVERRIDE = {
    "Smart Grid Q2": "Smart Grid Q2",
}


def run_one(job_class, query_number, config_file, flink_job_name, full_pds, event_rate, strategy_name, dry_run=False, dest_job_class=None):
    flat_folder = CONTROLLER_DIR / flink_folder_name(flink_job_name)
    before_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    before_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))

    result = submit_job(job_class, query_number, config_file, full_pds, event_rate, dry_run=dry_run)
    if dry_run or result.get("status") != "ok":
        return result

    new_eval, bad_status, moved_file = move_new_metrics_files(flink_job_name, job_class, strategy_name, before_eval, before_meta, dest_job_class=dest_job_class)
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


def build_full_pds(assignment, has_independent_sink):
    if has_independent_sink:
        return [SOURCE_PARALLELISM] + list(assignment) + [SINK_PARALLELISM]
    return [SOURCE_PARALLELISM] + list(assignment)


def run_random(manifest, event_rate, dry_run=False):
    for workload_key, (job_class, query_number, config_file, flink_job_name, stage_names, has_independent_sink) in WORKLOADS.items():
        num_stages = len(stage_names)
        assignments = strat.random_strategy(RANDOM_LEVELS, num_stages, RANDOM_SAMPLES, seed=RANDOM_SEED)
        for i, assignment in enumerate(assignments):
            key = strategy_key(workload_key, "random", f"s{i}", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            full_pds = build_full_pds(assignment, has_independent_sink)
            log(f"[random] {workload_key} sample {i}/{len(assignments)} rate={event_rate} -> {full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, flink_job_name, full_pds, event_rate, "random", dry_run=dry_run, dest_job_class=DEST_JOB_CLASS_OVERRIDE.get(workload_key))
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
    for workload_key, (job_class, query_number, config_file, flink_job_name, stage_names, has_independent_sink) in WORKLOADS.items():
        num_stages = len(stage_names)
        baseline_assignment = [strat.MIN_PARALLELISM] * num_stages
        baseline_full_pds = build_full_pds(baseline_assignment, has_independent_sink)
        baseline_key = strategy_key(workload_key, "uniform", "baseline", event_rate)
        baseline_file = None

        if manifest.get(baseline_key) == "ok":
            log(f"SKIP (already done): {baseline_key}")
            baseline_file = manifest.get(baseline_key + "|file")
        else:
            log(f"[baseline] {workload_key} rate={event_rate} -> {baseline_full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, flink_job_name, baseline_full_pds, event_rate, "uniform", dry_run=dry_run, dest_job_class=DEST_JOB_CLASS_OVERRIDE.get(workload_key))
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

        stage_metrics = strat.load_stage_metrics_flink(baseline_file, stage_names)

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (event_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            full_pds = build_full_pds(assignment, has_independent_sink)
            log(f"[{strategy_name}] {workload_key} rate={event_rate} -> {full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, flink_job_name, full_pds, event_rate, strategy_name, dry_run=dry_run, dest_job_class=DEST_JOB_CLASS_OVERRIDE.get(workload_key))
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
