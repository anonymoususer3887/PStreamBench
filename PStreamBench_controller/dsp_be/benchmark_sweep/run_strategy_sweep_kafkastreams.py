"""
Non-uniform per-operator parallelism benchmarking for Kafka Streams --
Random, DS2, and Dhalion, against the real CloudLab cluster, mirroring
run_strategy_sweep_flink.py's shape but adapted to Kafka Streams' own
deployment model and, critically, its fundamentally different parallelism
model.

KAFKA STREAMS HAS NO PER-OPERATOR PARALLELISM CONTROL AT ALL. Confirmed
directly in every one of the 14 workload Topology.java classes under
PStreamBench_workload_generator/kafkastreams/dsp_kafkastreams_jobs/ (see
WordCountTopology.java's own javadoc, and Part A.1/A.2 of
KAFKA_STREAMS_RISINGWAVE_PLAN.md): every task in a topology is
parallelized uniformly by input-topic partition count, and
num.stream.threads only bounds how many of a *single instance's* tasks
run concurrently -- it does not vary by pipeline stage. Every Topology.java
class already made the same deliberate compromise, to keep the job_pds
vector shape identical to Flink/Storm/Spark's sweep configs (e.g.
"2,4,4,1"): `parallelism_degree[1]` (index 1 specifically, confirmed with
zero exceptions across all 14 workload files) is read as the sole
`num.stream.threads` value; every other index (source at [0], sink at the
tail) has no Kafka Streams equivalent and is silently ignored.

This means a genuinely non-uniform *per-stage* sweep is not architecturally
possible here -- there is exactly ONE real knob for the whole topology,
not several independent ones. Per explicit user direction (2026-09-19),
this script runs the sweep anyway, reusing the existing single-index
convention rather than skipping Kafka Streams or rearchitecting all 14
Java jobs to add real per-stage repartitioning: Random still produces a
meaningfully different single-thread-count choice per sample; DS2/Dhalion
still produce a data-driven choice of that one knob from real (job-wide)
bottleneck/throughput analysis via load_stage_metrics_kafkastreams() (see
that function's docstring in parallelism_strategies.py) -- a degenerate
but still legitimate comparison arm against Random and against the
already-existing uniform sweep.

Consequently WORKLOADS below carries exactly one representative
`stage_name` per workload/query (not several genuinely-independent
controllable stages the way every other engine's WORKLOADS does) -- chosen
as the last interior OperatorMetricsProcessor stage before the sink (the
stage whose measured throughput/selectivity is most representative of the
pipeline's real bottleneck), read directly off getprom_kafkastreams.py's
own JOB_CLASS_DISPATCH operator_names lists (already-verified ground
truth, not re-derived from Java source). SOURCE_PARALLELISM/
SINK_PARALLELISM below are placeholders only, matching Flink/Spark/
RisingWave's shared "2...1" convention purely for job_parallelization
logging consistency -- Kafka Streams never actually reads either position.

WINDOW TIME UNITS: like Flink, several workloads use Duration.ofSeconds()
instead of Duration.ofMillis() for their sliding window -- confirmed
2026-09-19 by grepping every Topology.java file directly: Smart Grid Q2
and Linear Road Q2 both use seconds (exactly mirroring the equivalent
Flink SmartGridJob.java/LinearRoad.java quirk found and fixed during that
engine's own sweep), every other windowed workload uses milliseconds.
configs/smartgrid_q2.json and configs/linearroad_q2.json were already
fixed for this during the Flink sweep (job_window_size_max tightened from
1000 to 100, since 1000 SECONDS is far longer than any job_run_time here)
-- reused as-is, no additional config changes needed for Kafka Streams.

Output folder convention: getprom_kafkastreams.py's main() writes into a
FLAT folder named after job_class exactly (e.g. "Linear Road/"), shared
across every query of that job_class -- there is no per-query
disambiguation at the source the way Flink's own self-reported job name
sometimes provided one. This script's own move_new_metrics_files() diffs
before/after snapshots per individual submission (not a bulk end-of-sweep
copy), so this sharing does NOT cause any cross-query data-mixing risk
during the live sweep itself (confirmed by direct comparison with the
Flink migration-script bug this project already hit and fixed) -- it only
matters for the eventual artifacts/ migration script, which must filter
by each file's own job_query_number the same way the Flink migration
script was fixed to do.

Usage:
    python3 run_strategy_sweep_kafkastreams.py run --dry-run
    python3 run_strategy_sweep_kafkastreams.py run --strategy random
    python3 run_strategy_sweep_kafkastreams.py run --strategy ds2
    python3 run_strategy_sweep_kafkastreams.py run --strategy dhalion
    python3 run_strategy_sweep_kafkastreams.py run --event-rate 1000000  # all three strategies
    python3 run_strategy_sweep_kafkastreams.py status
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
MANIFEST_PATH = SCRIPT_DIR / "manifest_strategy_kafkastreams.json"

CONTROLLER_URL = "http://localhost:8000"
CLUSTER_ID = "f48d6e92-7db8-48ae-bd8b-891b61c3e0ca"  # kafkastreams-strategy-sweep-v2, ms0807 master (old ms1113-based cluster's CloudLab experiment expired 2026-09-30)

SOURCE_PARALLELISM = 2  # placeholder only -- see module docstring, never actually read
SINK_PARALLELISM = 1    # placeholder only -- see module docstring, never actually read

RANDOM_LEVELS = [2, 4, 8, 16, 32, 40]
RANDOM_SAMPLES = 10
RANDOM_SEED = 42
BASELINE_EVENT_RATE = 100_000
JOB_RUN_TIME_MINUTES = 5
GAP_SECONDS = 60
ENGINE_NAME = "kafkastream"  # matches the existing uniform-sweep artifacts folder name exactly

# High num.stream.threads values (>=32, oversubscribed on this cluster's
# 16 real cores per node) correlated with a real null_core_metrics failure
# pattern across many workloads (confirmed live 2026-09-19 through
# 2026-09-24): every job's 5-minute countdown starts immediately after
# streams.start(), not once the stream actually reaches RUNNING -- with
# more threads there are more consumer-group rebalance participants (a
# slower rebalance) and, for workloads needing an auto-created internal
# repartition topic, more contention on its creation/propagation, either
# of which can eat a large fraction of a short window before real
# processing begins, and 40-thread instances also process each record
# slower than a 4- or 8-thread one on the same 16 cores regardless. Giving
# these combos more total wall-clock time (not more parallelism-sweep
# range) directly targets "not enough time to recover from a slow start",
# per-user decision (2026-09-24) to try this before any Java-side fix.
HIGH_THREAD_THRESHOLD = 32
LONG_JOB_RUN_TIME_MINUTES = 10

# getprom_kafkastreams.py's queryMetrics.kafka_throughput reading is an
# avg_over_time() over a FIXED 5-minute Prometheus lookback window ending
# ~30s after the job's Ansible playbook returns (see getKafkaMetrics() /
# main()'s timestamp_prom) -- that window size does not scale with
# job_run_time. At the 1M events/sec pass (2026-09-24), null_core_metrics
# failures showed up across ALL thread counts, including low ones (2/4/8)
# and workloads with no repartition topic (Word Count) -- ruling out the
# high-thread rebalance theory above as the sole cause here. The likely
# mechanism: topic setup / producer ramp-up to 1M events/sec is slower and
# more variable than at 100k/500k, and for a 5-minute job that overhead can
# eat enough of the run that the fixed 5-minute lookback window ends up
# looking mostly at pre-steady-state time. A longer run gives more real
# active-processing time to fall inside that same fixed window regardless
# of setup variance -- per-user decision (2026-09-24) to extend this to
# ALL combos at this event rate, not just high-thread ones.
HIGH_EVENT_RATE_THRESHOLD = 1_000_000


def job_run_time_for(swept_value, event_rate):
    if swept_value >= HIGH_THREAD_THRESHOLD or event_rate >= HIGH_EVENT_RATE_THRESHOLD:
        return LONG_JOB_RUN_TIME_MINUTES
    return JOB_RUN_TIME_MINUTES


def job_create_timeout_for(swept_value, event_rate):
    return job_run_time_for(swept_value, event_rate) * 60 + 900


# Every config's job_window_size/job_window_slide_size get IGNORED for the
# "Custom" enumeration strategy this sweep uses -- runner.py's make_configs()
# always draws window/slide randomly between job_window_size_min/_max (see
# JobRunner.make_configs(), "Custom" branch), regardless of the fixed
# top-level fields. Combined with most configs' min=10/max=1000 range, this
# meant window sizes as small as ~10-100ms were common -- confirmed live
# (2026-09-25) as a major contributor to the "expired window" record-loss
# bug (see the grace-period fix above/in Topology.java), and more
# fundamentally makes results non-reproducible/non-comparable across runs
# of the same combo. Per-user decision (2026-09-25): force a single FIXED
# window/slide by overriding _min and _max to the same value each, making
# runner.py's random draw degenerate to a constant. Applied uniformly to
# every workload, including Linear Road Q2 / Smart Grid Q2 which read this
# value in Duration.ofSeconds() rather than ofMillis() (a units difference
# already found and handled via their configs' own min/max range earlier
# this session) -- so for those two specifically this is a 250-SECOND
# window with a 10-second slide, not 250ms/10ms like everywhere else.
#
# Bumped from 100ms to 250ms (2026-09-25, per user decision) for the
# NORMAL sweep: even with the WindowConfig grace-period fix (grace
# defaults to max(windowSize, 500ms)), sub-100ms windows are still
# disproportionately sensitive to ordinary cross-thread processing skew
# under core oversubscription (confirmed live at num.stream.threads=40).
# STRESS_TEST_WINDOW_SIZE/_SLIDE preserve the original tiny-window values
# for anyone deliberately probing that sensitivity -- NOT used by
# run_random()/run_baseline_then_nudge() below, only available for a
# one-off invocation that explicitly opts in.
FIXED_WINDOW_SIZE = 250
FIXED_WINDOW_SLIDE_SIZE = 10
STRESS_TEST_WINDOW_SIZE = 10
STRESS_TEST_WINDOW_SLIDE_SIZE = 10

# -1 tells WindowConfig.build() (Java side) to use its own default of
# max(windowSizeMs, 500) rather than a caller-specified value -- see
# WindowConfig's javadoc. Overridable per-call via submit_job()'s
# grace_period_ms_override, used only for the explicit grace-period
# comparison sweep (see run_grace_comparison() below).
DEFAULT_GRACE_PERIOD_MS = -1

# workload_key -> (job_class, query_number, config_file, stage_name)
# stage_name is the SINGLE representative interior operator (last stage
# before sink, per getprom_kafkastreams.py's own JOB_CLASS_DISPATCH
# operator_names lists) -- see module docstring for why there is only one,
# unlike every other engine's WORKLOADS.
WORKLOADS = {
    "Word Count":                 ("Word Count", "1", "wordcount_q1", "counter"),
    "Ad Analytics Q1":            ("Ad Analytics", "1", "adanalytics_q1", "window"),
    "Ad Analytics Q2":            ("Ad Analytics", "2", "adanalytics_q2", "rollingCTR"),
    "Bargain Index":              ("Bargain Index", "1", "bargainindex_q1", "bargain"),
    "Click Analytics Q1":         ("Click Analytics", "1", "clickanalytics_q1", "reduce"),
    "Click Analytics Q2":         ("Click Analytics", "2", "clickanalytics_q2", "geo"),
    "Click Analytics Q3":         ("Click Analytics", "3", "clickanalytics_q3", "reduce"),
    "Google cloud Monitoring":    ("Google cloud Monitoring", "1", "gcm_q1", "window"),
    "Linear Road Q1":             ("Linear Road", "1", "linearroad_q1", "formatter"),
    "Linear Road Q2":             ("Linear Road", "2", "linearroad_q2", "formatter"),
    # Linear Road Q3/Q4 deliberately excluded -- source/sink only, no
    # OperatorMetricsProcessor interior stage at all (see LinearRoadTopology.java),
    # matching the project-wide convention already used for every other engine.
    "Log Processing":             ("Log Processing", "1", "logprocessing_q1", "counter"),
    "Machine Outlier":            ("Machine Outlier", "1", "machineoutlier_q1", "outlier"),
    "Sentiment Analysis":         ("Sentiment Analysis", "1", "sentiment_q1", "analyser"),
    "Smart Grid Q1":              ("Smart Grid", "1", "smartgrid_q1", "window"),
    "Smart Grid Q2":              ("Smart Grid", "2", "smartgrid_q2", "window"),
    "Spike Detection":            ("Spike Detection", "1", "spikedetection_q1", "spikedetector"),
    "TPCH":                       ("TPCH", "1", "tpch_q1", "counter"),
    "Traffic Monitoring":         ("Traffic Monitoring", "1", "trafficmonitoring_q1", "avgspeed"),
    "Trending Topics":            ("Trending Topics", "1", "trendingtopics_q1", "filter"),
}


def log(msg):
    print(f"[run_strategy_sweep_kafkastreams] {msg}", flush=True)


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def strategy_key(workload_key, strategy_name, tag, event_rate):
    return f"{strategy_name}|{workload_key}|{tag}|{event_rate}"


def strategy_folder(job_class, strategy_name):
    # <job_class>/kafkastream/<strategy>/ -- keeps this isolated from the
    # existing flat uniform-sweep data and from every other engine's
    # strategy runs landing in the same <job_class>/ folder under a
    # different engine name.
    folder = CONTROLLER_DIR / job_class / ENGINE_NAME / strategy_name
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def failed_strategy_folder(job_class, strategy_name):
    # <job_class>/kafkastream/<strategy>_failed/ -- per-user decision
    # (2026-09-30) to keep a failed run's actual evaluationMetrics/metaInfo
    # files for inspection instead of silently os.remove()-ing them (the
    # prior behavior for every engine's strategy sweep in this project --
    # a "bad pair is deleted rather than relocated" gate). The manifest's
    # status string alone doesn't carry the real queryMetrics/operatorMetrics/
    # hardwareMetrics values, which is what you'd want to look at to
    # understand WHY a specific run failed rather than just THAT it failed.
    folder = CONTROLLER_DIR / job_class / ENGINE_NAME / f"{strategy_name}_failed"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _check_core_metrics_present(metrics_file):
    """Same "file exists is not the same as file is usable" gate every
    other strategy sweep uses -- adapted to getprom_kafkastreams.py's own
    evaluationMetrics shape (job_name/operatorMetrics/queryMetrics/
    streamsMetrics/hardwareMetrics, see getCombo()).
    """
    with open(metrics_file) as fh:
        metrics = json.load(fh)
    checks = {
        "queryMetrics.kafka_throughput": metrics.get("queryMetrics", {}).get("kafka_throughput", {}).get("throughput"),
        "hardwareMetrics.node_memory_98": metrics.get("hardwareMetrics", {}).get("node_memory_98"),
    }
    for path, value in checks.items():
        if value is None:
            return path
    return None


def flat_folder_name(job_class):
    """getprom_kafkastreams.py writes into a folder named after job_class
    verbatim (e.g. "Linear Road") -- see main()'s job_name computation.
    """
    return job_class


def move_new_metrics_files(job_class, strategy_name, before_eval, before_meta):
    """Same before/after-snapshot relocation pattern as every other
    engine's strategy sweep. Validates core fields before moving -- a bad
    pair is deleted rather than relocated.
    """
    flat_folder = CONTROLLER_DIR / flat_folder_name(job_class)
    after_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    after_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))
    new_eval = sorted(after_eval - before_eval, key=os.path.getmtime)
    new_meta = sorted(after_meta - before_meta, key=os.path.getmtime)

    if not new_eval:
        return new_eval, None, None

    latest_eval = new_eval[-1]
    bad_field = _check_core_metrics_present(latest_eval)
    if bad_field:
        failed_dest = failed_strategy_folder(job_class, strategy_name)
        for f in new_eval + new_meta:
            target = failed_dest / Path(f).name
            os.rename(f, target)
        log(f"  METRICS FILE HAS NULL CORE FIELD ({bad_field}), moved to {failed_dest}: {Path(latest_eval).name}")
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


def submit_job(job_class, job_query_name, config_file, full_pds, event_rate, dry_run=False,
               window_size_override=None, window_slide_override=None, grace_period_ms_override=None,
               run_time_override=None):
    """One submit-wait-collect-kill cycle via POST /infra/jobcreate/<cluster_id>
    with job_enumeration_strategy="Custom" -- the whole lifecycle happens
    server-side in this one blocking call (see infra/runner.py's
    JobRunner._run_kafkastreams_job()).

    window_size_override/window_slide_override/grace_period_ms_override
    let a caller (e.g. run_grace_comparison()) deviate from the sweep's
    normal fixed window/default-grace for one specific combo, without
    touching the module-level constants every other call relies on.
    run_time_override (minutes) bypasses job_run_time_for()'s normal
    thread/event-rate-based auto-selection entirely, for a caller that
    wants a fixed run time regardless of swept_value/event_rate (e.g.
    retry_failed_at_lower_event_rate_fixed_config()).
    """
    swept_value = full_pds[1]
    run_time = run_time_override if run_time_override is not None else job_run_time_for(swept_value, event_rate)
    window_size = window_size_override if window_size_override is not None else FIXED_WINDOW_SIZE
    window_slide = window_slide_override if window_slide_override is not None else FIXED_WINDOW_SLIDE_SIZE
    grace_period_ms = grace_period_ms_override if grace_period_ms_override is not None else DEFAULT_GRACE_PERIOD_MS

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
    payload["job_run_time"] = run_time
    payload["job_iterations"] = 1
    payload["num_of_times_job_run"] = 1
    payload["job_iteration_strategy"] = "From Top"
    payload["job_window_size"] = window_size
    payload["job_window_size_min"] = window_size
    payload["job_window_size_max"] = window_size
    payload["job_window_slide_size"] = window_slide
    payload["job_window_slide_size_min"] = window_slide
    payload["job_window_slide_size_max"] = window_slide
    payload["job_grace_period_ms"] = grace_period_ms

    if dry_run:
        log(f"  DRY RUN would POST: job_class={job_class} query={job_query_name} "
            f"pds={full_pds} rate={event_rate} run_time={run_time}min")
        return {"status": "dry_run"}

    if run_time != JOB_RUN_TIME_MINUTES:
        if run_time_override is not None:
            reason = "run_time_override set"
        elif event_rate >= HIGH_EVENT_RATE_THRESHOLD:
            reason = "event_rate >= {:,}".format(HIGH_EVENT_RATE_THRESHOLD)
        else:
            reason = f"swept value {swept_value} >= {HIGH_THREAD_THRESHOLD}"
        log(f"  {reason}: using {run_time}min run time instead of {JOB_RUN_TIME_MINUTES}min")

    # Timeout derived from the ACTUAL run_time being used (not
    # re-derived via job_run_time_for()) -- with run_time_override set,
    # job_run_time_for() alone could under-estimate it (e.g. a low-thread,
    # sub-1M-event-rate combo normally gets 5min, but run_time here could
    # be forced to 10min), which would make the client time out on a job
    # that's still happily running server-side.
    client_timeout = run_time * 60 + 900

    resp = requests.post(
        f"{CONTROLLER_URL}/infra/jobcreate/{CLUSTER_ID}",
        json=payload, timeout=client_timeout,
    )
    if resp.status_code != 200:
        return {"status": f"http_{resp.status_code}: {resp.text[:300]}"}
    return {"status": "ok"}


def run_one(job_class, query_number, config_file, full_pds, event_rate, strategy_name, dry_run=False,
            window_size_override=None, window_slide_override=None, grace_period_ms_override=None,
            run_time_override=None):
    flat_folder = CONTROLLER_DIR / flat_folder_name(job_class)
    before_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    before_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))

    result = submit_job(job_class, query_number, config_file, full_pds, event_rate, dry_run=dry_run,
                         window_size_override=window_size_override, window_slide_override=window_slide_override,
                         grace_period_ms_override=grace_period_ms_override, run_time_override=run_time_override)
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


def build_full_pds(swept_value):
    # Always exactly 3 elements: [placeholder, swept_value, placeholder] --
    # index [1] is the only position any Topology.java ever reads (see
    # module docstring). No workload needs a longer array regardless of
    # how many operator_names it conceptually has.
    return [SOURCE_PARALLELISM, swept_value, SINK_PARALLELISM]


def run_random(manifest, event_rate, dry_run=False):
    for workload_key, (job_class, query_number, config_file, stage_name) in WORKLOADS.items():
        assignments = strat.random_strategy(RANDOM_LEVELS, 1, RANDOM_SAMPLES, seed=RANDOM_SEED)
        for i, assignment in enumerate(assignments):
            key = strategy_key(workload_key, "random", f"s{i}", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            full_pds = build_full_pds(assignment[0])
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
    for workload_key, (job_class, query_number, config_file, stage_name) in WORKLOADS.items():
        baseline_assignment = [strat.MIN_PARALLELISM]
        baseline_full_pds = build_full_pds(baseline_assignment[0])
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

        stage_metrics = strat.load_stage_metrics_kafkastreams(baseline_file, [stage_name])

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (event_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            full_pds = build_full_pds(assignment[0])
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


def retry_failed_at_parallelism(manifest, event_rate, old_pds_value=40, new_pds_value=36, dry_run=False):
    """Re-submit every still-not-ok combo at this event_rate whose actual
    attempted swept value was old_pds_value, using new_pds_value instead --
    per user decision (2026-09-30): num.stream.threads=40 is 2.5x
    oversubscribed on this cluster's 16-real-core nodes and empirically the
    single biggest source of residual failures across every pass this
    session; 36 is still meaningfully oversubscribed (2.25x) but a bit less
    marginal. Keeps the SAME manifest key on success/failure (does not
    create a new key) -- the underlying evaluationMetrics.json always
    records the real job_pds used, so there's no data-provenance ambiguity
    even though the "sample index" nominally still means "the seeded
    random draw" for bookkeeping purposes.

    For "random" combos, old_pds_value is looked up via the same seeded
    random_strategy() draw run_random() itself uses (deterministic, no
    cluster access needed). For "ds2"/"dhalion" combos, the actual
    assignment is workload-specific (computed from real baseline metrics,
    not a fixed seed) -- recomputed here from the already-saved baseline
    file so no cluster access is needed to check eligibility either.
    """
    random_assignments = strat.random_strategy(RANDOM_LEVELS, 1, RANDOM_SAMPLES, seed=RANDOM_SEED)

    for workload_key, (job_class, query_number, config_file, stage_name) in WORKLOADS.items():
        for i in range(RANDOM_SAMPLES):
            key = strategy_key(workload_key, "random", f"s{i}", event_rate)
            if manifest.get(key) == "ok" or manifest.get(key) is None:
                continue
            swept_value = random_assignments[i][0]
            if swept_value != old_pds_value:
                continue
            full_pds = build_full_pds(new_pds_value)
            log(f"[retry@{new_pds_value}] {workload_key} random s{i} rate={event_rate} -> {full_pds} (was {old_pds_value})")
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

        baseline_assignment = [strat.MIN_PARALLELISM]
        baseline_key = strategy_key(workload_key, "uniform", "baseline", event_rate)
        baseline_file = manifest.get(baseline_key + "|file")
        if not baseline_file or not os.path.exists(baseline_file):
            continue
        stage_metrics = strat.load_stage_metrics_kafkastreams(baseline_file, [stage_name])

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (event_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", event_rate)
            if manifest.get(key) == "ok" or manifest.get(key) is None:
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            swept_value = assignment[0]
            if swept_value != old_pds_value:
                continue
            full_pds = build_full_pds(new_pds_value)
            log(f"[retry@{new_pds_value}] {workload_key} {strategy_name} rate={event_rate} -> {full_pds} (was {old_pds_value})")
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


def retry_failed_at_lower_event_rate(manifest, source_rate, target_rate, old_pds_value=40, new_pds_value=36):
    """Diagnostic retry: for every still-failing combo at source_rate,
    resubmit the SAME workload/strategy/parallelism combo but at
    target_rate (a LOWER event rate) instead, to test whether event-rate
    itself -- not parallelism -- is the real bottleneck for combos that
    didn't recover even after the 40->36 parallelism retry (confirmed
    live 2026-09-30/10-01: 0/38 of the still-failing 1M combos recovered
    at 36 threads, vs 5/5 at 100k and 3/12 at 500k -- a pattern that
    tracks event rate, not thread count). Per-user request (2026-10-01):
    retry 500k failures at 300k, and 1M failures at 500k.

    Uses the SAME parallelism each combo was last actually attempted with
    (36 if it was eligible for and went through the 40->36 retry, else
    whatever its original random/ds2/dhalion-derived value was) -- this
    isolates event rate as the one changed variable.

    Results are recorded under a DISTINCT manifest key (tag suffixed with
    "_at<target_rate>") rather than the real strategy_key(..., target_rate)
    key, since target_rate may have its own genuine, unrelated pass data
    (true for 500k, and would be true for 300k/300000 if ever swept for
    real) that must not be overwritten or conflated with this diagnostic.
    """
    random_assignments = strat.random_strategy(RANDOM_LEVELS, 1, RANDOM_SAMPLES, seed=RANDOM_SEED)

    def _last_attempted_value(original_value):
        return new_pds_value if original_value == old_pds_value else original_value

    for workload_key, (job_class, query_number, config_file, stage_name) in WORKLOADS.items():
        for i in range(RANDOM_SAMPLES):
            key = strategy_key(workload_key, "random", f"s{i}", source_rate)
            status = manifest.get(key)
            if status is None or status == "ok":
                continue
            swept_value = _last_attempted_value(random_assignments[i][0])
            full_pds = build_full_pds(swept_value)
            diag_key = strategy_key(workload_key, "random", f"s{i}_at{target_rate}", source_rate)
            if manifest.get(diag_key) == "ok":
                log(f"SKIP (already done): {diag_key}")
                continue
            log(f"[lowrate {source_rate}->{target_rate}] {workload_key} random s{i} pds={full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, full_pds, target_rate, "random")
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[diag_key] = result.get("status", "unknown")
            save_manifest(manifest)
            log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
            time.sleep(GAP_SECONDS)

        baseline_assignment = [strat.MIN_PARALLELISM]
        baseline_key = strategy_key(workload_key, "uniform", "baseline", source_rate)
        baseline_file = manifest.get(baseline_key + "|file")
        if not baseline_file or not os.path.exists(baseline_file):
            continue
        stage_metrics = strat.load_stage_metrics_kafkastreams(baseline_file, [stage_name])

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (source_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", source_rate)
            status = manifest.get(key)
            if status is None or status == "ok":
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            swept_value = _last_attempted_value(assignment[0])
            full_pds = build_full_pds(swept_value)
            diag_key = strategy_key(workload_key, strategy_name, f"nudge_at{target_rate}", source_rate)
            if manifest.get(diag_key) == "ok":
                log(f"SKIP (already done): {diag_key}")
                continue
            log(f"[lowrate {source_rate}->{target_rate}] {workload_key} {strategy_name} pds={full_pds}")
            try:
                result = run_one(job_class, query_number, config_file, full_pds, target_rate, strategy_name)
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[diag_key] = result.get("status", "unknown")
            save_manifest(manifest)
            log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
            time.sleep(GAP_SECONDS)


def retry_failed_at_lower_event_rate_fixed_config(manifest, source_rate, target_rate,
                                                    window_size=100, window_slide=10, run_time_minutes=10):
    """Per explicit user decision (2026-10-01): abandon the 40->36
    parallelism lever (confirmed weak -- only 2/7 recovered on clean
    post-outage data) and directly test every CURRENTLY-failing
    source_rate combo at a lower target_rate instead, using each combo's
    own ORIGINAL parallelism (no swap), a smaller FIXED 100ms window (down
    from the sweep's normal 250ms default) / 10ms slide, and a forced
    10-minute run time regardless of the normal thread/event-rate-based
    auto-selection (job_run_time_for() would otherwise give most of these
    combos only 5 minutes, since source_rate/target_rate are usually
    <1,000,000 and many of these combos' own parallelism is <32).

    Results are recorded under DISTINCT manifest keys (tag suffixed
    "_at<target_rate>_w<window_size>") -- distinct from both the real
    strategy_key(..., target_rate) entries AND the earlier
    "_at<target_rate>" diagnostic keys from retry_failed_at_lower_event_rate()
    (same target rate, different window config), so none of these three
    get conflated with each other.
    """
    random_assignments = strat.random_strategy(RANDOM_LEVELS, 1, RANDOM_SAMPLES, seed=RANDOM_SEED)

    for workload_key, (job_class, query_number, config_file, stage_name) in WORKLOADS.items():
        for i in range(RANDOM_SAMPLES):
            key = strategy_key(workload_key, "random", f"s{i}", source_rate)
            status = manifest.get(key)
            if status is None or status == "ok":
                continue
            # full_pds uses this combo's OWN original parallelism, not a
            # swapped value -- derive it the same way run_random() did.
            full_pds = build_full_pds(random_assignments[i][0])
            diag_key = strategy_key(workload_key, "random", f"s{i}_at{target_rate}_w{window_size}", source_rate)
            if manifest.get(diag_key) == "ok":
                log(f"SKIP (already done): {diag_key}")
                continue
            log(f"[lowrate-fixedwin {source_rate}->{target_rate}] {workload_key} random s{i} pds={full_pds} window={window_size}ms")
            try:
                result = run_one(job_class, query_number, config_file, full_pds, target_rate, "random",
                                  window_size_override=window_size, window_slide_override=window_slide,
                                  run_time_override=run_time_minutes)
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[diag_key] = result.get("status", "unknown")
            save_manifest(manifest)
            log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
            time.sleep(GAP_SECONDS)

        baseline_assignment = [strat.MIN_PARALLELISM]
        baseline_key = strategy_key(workload_key, "uniform", "baseline", source_rate)
        baseline_file = manifest.get(baseline_key + "|file")
        if not baseline_file or not os.path.exists(baseline_file):
            continue
        stage_metrics = strat.load_stage_metrics_kafkastreams(baseline_file, [stage_name])

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (source_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_key, strategy_name, "nudge", source_rate)
            status = manifest.get(key)
            if status is None or status == "ok":
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            full_pds = build_full_pds(assignment[0])
            diag_key = strategy_key(workload_key, strategy_name, f"nudge_at{target_rate}_w{window_size}", source_rate)
            if manifest.get(diag_key) == "ok":
                log(f"SKIP (already done): {diag_key}")
                continue
            log(f"[lowrate-fixedwin {source_rate}->{target_rate}] {workload_key} {strategy_name} pds={full_pds} window={window_size}ms")
            try:
                result = run_one(job_class, query_number, config_file, full_pds, target_rate, strategy_name,
                                  window_size_override=window_size, window_slide_override=window_slide,
                                  run_time_override=run_time_minutes)
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                result = {"status": f"exception: {e!r}"}
            manifest[diag_key] = result.get("status", "unknown")
            save_manifest(manifest)
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
@click.option("--event-rate", type=int, default=BASELINE_EVENT_RATE)
@click.option("--old-value", type=int, default=40)
@click.option("--new-value", type=int, default=36)
@click.option("--dry-run", is_flag=True, default=False)
def retry_at(event_rate, old_value, new_value, dry_run):
    manifest = load_manifest()
    retry_failed_at_parallelism(manifest, event_rate, old_pds_value=old_value, new_pds_value=new_value, dry_run=dry_run)
    log("\n=== RETRY-AT DONE ===")


@cli.command()
def status():
    manifest = load_manifest()
    ok = sum(1 for v in manifest.values() if v == "ok")
    log(f"{ok} entries ok, {len(manifest) - ok} attempted-but-not-ok")
    for k, v in sorted(manifest.items()):
        log(f"  {k}: {v}")


if __name__ == "__main__":
    cli()
