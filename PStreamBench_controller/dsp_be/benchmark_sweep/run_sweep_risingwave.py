"""
PStreamBench RisingWave benchmark sweep.

Counterpart of run_sweep.py (Spark) for RisingWave. Direct orchestration
(psql + custom_producer.py + the real getprom_risingwave.py CLI), not the
generic multi-engine Django job-creation API -- same reasoning as this
project's earlier live-cluster validation work: RisingWave has no job_pds
CLI concept the shared API/Ansible payload builder expects, and building a
new dedicated code path is far lower-risk than reverse-engineering that
generic payload shape for an engine it was never designed for.

Sweeps:
  - parallelism (RisingWave aggregation-stage streaming_parallelism):
    2, 4, 8, 16, 32, 40 -- source fixed at 2, sink fixed at 1, matching the
    other engines' job_pds convention (source=2, sink=1, middle=swept) as
    closely as RisingWave's execution model allows. NOTE: RisingWave fuses
    a stateless per-row stage (e.g. Word Count's "tokenizer") into its
    upstream source scan -- that fused stage inherits the SOURCE's
    parallelism (2), not the swept value. Only the actual stateful
    aggregation fragment (the "real" MATERIALIZED VIEW stage) takes the
    swept value. Confirmed live against this cluster (2026-07-27) via
    rw_catalog.rw_fragments before running any real sweep points.
  - event rate: 100_000, 500_000, 1_000_000 events/sec
  - window size / slide: 10s / 10s (only meaningful for workloads with a
    HOP window in their SQL -- Word Count has none, ignored for it)
  - run time: 5 minutes per combo
  - gap between jobs: 2 minutes, so Kafka/RisingWave fully settle and the
    metrics file is written before the next combo starts

Usage:
    python3 run_sweep_risingwave.py run
    python3 run_sweep_risingwave.py run --dry-run
    python3 run_sweep_risingwave.py status
"""
import glob
import json
import os
import re
import subprocess
import time
from pathlib import Path

import click

SCRIPT_DIR = Path(__file__).resolve().parent
CONTROLLER_DIR = SCRIPT_DIR.parent
PSTREAMBENCH_ROOT = CONTROLLER_DIR.parent.parent
RISINGWAVE_DIR = PSTREAMBENCH_ROOT / "PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs"
GETPROM_SCRIPT = CONTROLLER_DIR / "infra/reporting_scripts/getprom_risingwave.py"
PRODUCER_DIR = PSTREAMBENCH_ROOT / "PStreamBench_Cloud_setup"
MANIFEST_PATH = SCRIPT_DIR / "manifest_risingwave.json"

MAIN_NODE = "ms1011.utah.cloudlab.us"  # actual meta+frontend node -- the
# previous ms0928-based cluster was terminated by CloudLab (2026-08-24);
# this is the new 6-node cluster (ms1011/ms1030/ms1021/ms1038/ms1029/
# ms1019), confirmed via rw_catalog.rw_worker_nodes (WORKER_TYPE_META/
# FRONTEND both on ms1011, matching the master host passed to
# setup_new_cluster.sh this time; the other 5 nodes are
# WORKER_TYPE_COMPUTE_NODE)
RW_PORT = "4566"
KAFKA_BOOTSTRAP = MAIN_NODE + ":9092"
HARDWARE_TYPE = "m510"
NUM_NODES = "6"
NUM_SLOTS = "16"
DATA_DIR = "/Users/pratyushagnihotri/pstreambench_data/"
PSQL = "/opt/homebrew/opt/libpq/bin/psql"
KAFKA_BIN = "/opt/homebrew/opt/kafka/bin"
VENV_PY = str(Path(__file__).parent / ".sweep_venv" / "bin" / "python3")

PARALLELISM_LEVELS = [2, 4, 8, 16, 32, 40]
EVENT_RATES = [100_000, 500_000, 1_000_000]
WINDOW_SIZE_SEC = 10
WINDOW_SLIDE_SEC = 10
THRESHOLD = 5  # matches PStreamBench_wui/dsp_fe's f_job_threshold default

# Per-workload override for THRESHOLD. Bargain Index's __THRESHOLD__ is a
# quote_price/vwap RATIO filter (bargain_index.sql), not a count like
# Trending Topics' __THRESHOLD__ (COUNT(*) > N) -- the global default of 5
# means "last price is 5x the window's volume-weighted average", which is
# unrealistic for real stock data and produces zero output rows 100% of
# the time (confirmed live, 2026-08-04: real per-window vwap/quote_price
# values diverge normally, e.g. ratios of 1.10/1.06/1.05, but never
# anywhere close to 5). 1.02 is confirmed live to pass a real, non-trivial
# fraction of windows (3/12 in a controlled test) without being so loose
# it passes nearly everything. Trending Topics is NOT overridden here --
# its count-based threshold already produces clean, non-null output at
# the global default of 5.
WORKLOAD_THRESHOLDS = {
    "Bargain Index": 1.02,
}
RUN_TIME_MINUTES = 5
GAP_SECONDS = 120  # "keep some minutes gap between job so that data get downloaded"

# (workload_name, query_number, sql_relpath, in_topic, out_topic, producer_app_type, needs_window)
#
# All 14 RisingWave workloads have both a getprom_risingwave.py
# JOB_CLASS_DISPATCH entry and producer support in custom_producer.py --
# CORRECTION (2026-07-31): an earlier pass over this file concluded Linear
# Road had no producer support and excluded it, based on grepping
# custom_producer.py for "LINEAR"/"Linear_Road"-shaped names. That was
# wrong -- Linear Road's app_type constant is named "LRB" (Linear Road
# Benchmark, matching the original Flink/Storm/Kafka Streams jobs' own
# naming), with its own data file (lrb.csv, already present on every node
# via download_datasets.sh) and its own branch in both read_data_file()
# and event_producer(). Added below now that this was caught.
#
# needs_window determined by grepping each .sql file for the
# __WINDOW_SIZE_SEC__ placeholder (only HOP-windowed queries have it) rather
# than guessing from the query's English description.
#
# Only Word Count and Ad Analytics have been live-validated end-to-end on
# a real cluster before this sweep (see BUGS_AND_FIXES.md) -- the other 11
# workloads' .sql translations are written but unvalidated, per each
# workload's own file-header notes. Expect some of these to fail on first
# contact with a real cluster; the sweep's per-combo status tracking (see
# run_one_combo below) already tolerates and records individual failures
# without blocking the rest of the sweep.
WORKLOADS = [
    ("Word Count", "1", "wordcount/word_count.sql", "WordCountIn", "WordCountOut", "Word_Count", False),
    ("Ad Analytics", "2", "AdsAnalytics/ads_analytics_q2.sql", "AdAnalyticsIn", "AdAnalyticsOut", "Ad_Analytics", True),
    ("Smart Grid", "1", "smartgrid/smart_grid.sql", "SmartGridIn", "SmartGridOut", "Smart_Grid", True),
    ("Log Processing", "1", "LogsAnalyzer/logs_analyzer.sql", "LogIn", "LogOut", "Log_Processing", True),
    ("Machine Outlier", "1", "MachineOutlier/machine_outlier.sql", "MOIn", "MOOut", "Machine_Outlier", True),
    ("Bargain Index", "1", "BargainIndex/bargain_index.sql", "BIIn", "BIOut", "Bargain_Index", True),
    ("Trending Topics", "1", "TrendingTopics/trending_topics.sql", "TTIn", "TTOut", "Trending_Topic", True),
    ("Click Analytics", "1", "ClickAnalytics/click_analytics.sql", "CAIn", "CAOut", "Click_Analytics", True),
    ("Traffic Monitoring", "1", "TrafficMonitoring/traffic_monitoring.sql", "TMIn", "TMOut", "Traffic_Monitoring", True),
    ("Spike Detection", "1", "SpikeDetection/spike_detection.sql", "SpikeIn", "SpikeOut", "Spike_detection", True),
    ("Sentiment Analysis", "1", "SentimentAnalysis/sentiment_analysis.sql", "SentimentIn", "SentimentOut", "Sentiment_Analysis", False),
    ("TPCH", "1", "TPCH/tpch.sql", "TPIn", "TPOut", "Tpch", False),
    ("Google cloud Monitoring", "1", "GoogleCloudMonitoring/google_cloud_monitoring.sql", "GoogleIn", "GoogleOut", "Google_Cloud_Monitoring", True),
    ("Linear Road", "1", "LinearRoad/linear_road.sql", "LRIn", "LROut", "LRB", False),
]

# Additional query variants for workloads that have more than one (Q2/Q3),
# not merged into WORKLOADS above -- deliberately deferred until the
# one-query-per-workload pass finishes (see 2026-07-30 conversation: "run
# one per workload now when it is finished then we can decide" -> "you
# continue benchmarking other queries from other workloads"). Reuses each
# workload's existing topics/app_type (same producer, same data -- only the
# SQL query and therefore needs_window differs), so each gets its own
# distinct workload_name (e.g. "Ad Analytics Q1") to keep its manifest keys
# and evaluationMetrics output folder separate from the Q2 pass already run.
# needs_window determined the same way as WORKLOADS above (grepped each
# .sql for the __WINDOW_SIZE_SEC__ placeholder).
ADDITIONAL_QUERY_VARIANTS = [
    ("Ad Analytics Q1", "1", "AdsAnalytics/ads_analytics.sql", "AdAnalyticsIn", "AdAnalyticsOut", "Ad_Analytics", False),
    ("Click Analytics Q2", "2", "ClickAnalytics/click_analytics_q2.sql", "CAIn", "CAOut", "Click_Analytics", False),
    ("Click Analytics Q3", "3", "ClickAnalytics/click_analytics_q3.sql", "CAIn", "CAOut", "Click_Analytics", True),  # True because it needs __WINDOW_SIZE_MS__ substituted, not because it has a HOP window
    ("Smart Grid Q2", "2", "smartgrid/smart_grid_q2.sql", "SmartGridIn", "SmartGridOut", "Smart_Grid", True),
    ("Log Processing Q2", "2", "LogsAnalyzer/logs_analyzer_q2.sql", "LogIn", "LogOut", "Log_Processing", True),
    ("TPCH Q2", "2", "TPCH/tpch_q2.sql", "TPIn", "TPOut", "Tpch", True),
    ("Google cloud Monitoring Q2", "2", "GoogleCloudMonitoring/google_cloud_monitoring_q2.sql", "GoogleIn", "GoogleOut", "Google_Cloud_Monitoring", True),
]


def log(msg):
    print(f"[run_sweep_risingwave] {msg}", flush=True)


def sh(cmd, timeout=60):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def sh_resilient(cmd, timeout=90):
    """Never raises -- a DROP against this distributed cluster can
    legitimately take longer than local (bug #16 in BUGS_AND_FIXES.md).
    On timeout, issue RECOVER (RisingWave's own documented remediation)
    and move on rather than aborting the whole sweep.
    """
    try:
        return sh(cmd, timeout=timeout)
    except subprocess.TimeoutExpired:
        log(f"  [drop] command exceeded {timeout}s, issuing RECOVER and moving on")
        try:
            sh(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -c "RECOVER;" 2>&1', timeout=30)
        except subprocess.TimeoutExpired:
            pass
        return None


def object_prefix_for(workload_name):
    """Discovered live (2026-08-01): every ADDITIONAL_QUERY_VARIANTS entry
    is named "<base workload> Q<N>" (e.g. "Ad Analytics Q1", "Click
    Analytics Q2") to keep its manifest keys and evaluationMetrics folder
    separate from the base workload's own run -- but the underlying SQL
    objects are always named after the BASE workload
    (ad_analytics_source/_mv/_sink, click_analytics_source/_mv/_sink,
    ...) regardless of query variant, per _risingwave_job_manipulation()'s
    "<base>_q<N>.sql" file-naming convention (a second query variant is a
    different FILE, not a differently-named set of objects). Computing the
    prefix from the display name unstripped ("Ad Analytics Q1" ->
    "ad_analytics_q1") silently drops every DROP statement onto
    nonexistent objects while the CREATE statements collide with the
    real, still-existing ones from a previous run -- every combo for
    every multi-variant workload failed this way (either an immediate
    "already exists" catalog error, or a subsequent psql timeout from a
    leftover/orphaned job) before this was caught. Stripping a trailing
    " Q<digits>" before computing the prefix fixes it; harmless no-op for
    every base-workload name, which never has this suffix.
    """
    workload_name = re.sub(r' Q\d+$', '', workload_name)
    return re.sub(r'([A-Z])', r'_\1', workload_name.replace(" ", "")).lower().lstrip('_')


# Auxiliary reference TABLEs a workload's SQL creates alongside its normal
# source/mview/sink (bug #24, BUGS_AND_FIXES.md): drop_all() below only
# ever knew how to drop SINK/MATERIALIZED VIEW/SOURCE, so a workload that
# also does its own `CREATE TABLE IF NOT EXISTS` + `INSERT INTO ... VALUES`
# (click_analytics_q2.sql's geo_ip_lookup, a ~256-row IP-to-country lookup)
# had that table's rows accumulate, uncleaned, across every retry -- 512
# rows the first time this was caught manually, 4096 the second time (this
# fix). Keyed by workload_name since object_prefix_for() strips the "Q2"
# suffix, so the prefix alone can't distinguish "needs this cleanup" from
# any other Click Analytics variant.
WORKLOAD_AUX_TABLES = {
    "Click Analytics Q2": ["geo_ip_lookup"],
    # Same class of bug as geo_ip_lookup (bug #24): sentiment_analysis.sql
    # creates its own reference TABLE (afinn_lexicon, a ~2477-row sentiment
    # lexicon) via CREATE TABLE IF NOT EXISTS + a single large INSERT INTO
    # ... VALUES with no dedup guard -- drop_all() never cleaned this up
    # either, so repeated failed/retried combos kept re-inserting the same
    # ~2477 rows on top of whatever was already there. Discovered live
    # (2026-08-09): the strategy sweep's Sentiment Analysis/TPCH/Google
    # cloud Monitoring combos failed identically 3 retry attempts in a row
    # with SQL-submission timeouts even right after confirming the cluster
    # was otherwise healthy -- TPCH and Google cloud Monitoring have no
    # auxiliary tables of their own, so this was Sentiment Analysis's bloated
    # INSERT degrading the cluster for whichever combo ran right after it
    # in the fixed WORKLOADS iteration order, not three independent faults.
    "Sentiment Analysis": ["afinn_lexicon"],
}


def cancel_stale_jobs(prefix):
    """Cancel any in-flight "creating" streaming job matching this
    workload's object-name prefix. Confirmed live (2026-08-15): when a
    CREATE MATERIALIZED VIEW genuinely takes longer than run_one_combo()'s
    own SQL-submission timeout (real backfill progress, not a hang --
    `SHOW JOBS` shows a climbing row count) and the client gives up, the
    job keeps running server-side but never becomes a committed catalog
    entry -- so drop_all()'s `rw_catalog.rw_materialized_views` lookup
    below can't see it and never cleans it up. That zombie then blocks
    every subsequent combo's attempt to (re)create the same-named object,
    turning one timeout into a permanent, self-perpetuating failure for
    every later combo in the run (observed: 8 consecutive "table or source
    not found" failures downstream of a single abandoned CREATE). `SHOW
    JOBS` is the only place an in-flight job is visible before it commits.
    """
    r = sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -t -c "SHOW JOBS;"')
    if r is None or not r.stdout.strip():
        return
    for line in r.stdout.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2:
            continue
        job_id, statement = parts[0], parts[1]
        if not job_id.isdigit() or prefix not in statement.lower():
            continue
        sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -c "CANCEL JOBS {job_id};" 2>&1')


def drop_all(prefix, workload_name=None):
    cancel_stale_jobs(prefix)
    sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -c "DROP SINK IF EXISTS {prefix}_sink;" 2>&1')
    r = sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -t -c "SELECT name FROM rw_catalog.rw_materialized_views WHERE name LIKE \'{prefix}%\';"')
    for name in (r.stdout.split() if r is not None else []):
        sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -c "DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE;"')
    sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -c "DROP SOURCE IF EXISTS {prefix}_source CASCADE;"')
    # RisingWave doesn't support TRUNCATE TABLE -- DROP TABLE and let the
    # next combo's own CREATE TABLE IF NOT EXISTS + INSERT recreate it clean.
    for table_name in WORKLOAD_AUX_TABLES.get(workload_name, []):
        sh_resilient(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -c "DROP TABLE IF EXISTS {table_name};" 2>&1')


def build_sql_with_parallelism(sql_path, kafka_bootstrap, needs_window, parallelism, workload_name=None):
    """Inserts `SET streaming_parallelism = N;` before each top-level CREATE
    statement -- source=2 (fixed), materialized view=parallelism (swept),
    sink=1 (fixed). Verified live against this cluster (2026-07-27) that
    this staged-SET approach correctly pins source/sink while letting the
    aggregation fragment take the swept value -- see module docstring for
    the caveat about fused stateless stages.

    `parallelism` accepts either a single int (applied to every
    CREATE MATERIALIZED VIEW uniformly -- the original, still-default
    behavior used by the uniform sweep) or a list of ints (assigned
    positionally, one per CREATE MATERIALIZED VIEW, in file order -- for
    the non-uniform per-stage strategies in parallelism_strategies.py,
    e.g. Linear Road's 3 controllable MVs each getting a different value).
    A list's length must exactly match the workload's real MV count;
    mismatches raise rather than silently reusing or dropping values.
    """
    content = sql_path.read_text()
    content = content.replace("__KAFKA_BOOTSTRAP_SERVER__", kafka_bootstrap)
    if needs_window:
        content = content.replace("__WINDOW_SIZE_SEC__", str(WINDOW_SIZE_SEC))
        content = content.replace("__WINDOW_SLIDE_SEC__", str(WINDOW_SLIDE_SEC))
        content = content.replace("__WINDOW_SIZE_MS__", str(WINDOW_SIZE_SEC * 1000))
    # __THRESHOLD__ (bargain_index.sql, trending_topics.sql) -- discovered
    # live (2026-07-31) that this sweep script never substituted it at all,
    # unlike the real controller job-submission path
    # (views.py::_risingwave_job_manipulation, which takes threshold from
    # ansible_variables["job_threshold"]). Every combo for both workloads
    # failed with "Item not found: Invalid column: __threshold__" -- RisingWave
    # parsed the literal placeholder text as a bareword column reference.
    # THRESHOLD's value matches this project's own frontend default
    # (ClusterInfo.vue's f_job_threshold: 5), not an arbitrary guess.
    # WORKLOAD_THRESHOLDS overrides this per-workload where the global
    # default is semantically wrong (see its own comment -- Bargain Index).
    threshold = WORKLOAD_THRESHOLDS.get(workload_name, THRESHOLD)
    content = content.replace("__THRESHOLD__", str(threshold))

    parts = re.split(r'(?m)(?=^CREATE (?:SOURCE|MATERIALIZED VIEW|SINK))', content)

    mv_values = parallelism if isinstance(parallelism, list) else None
    mv_index = 0
    out = []
    for part in parts:
        if part.startswith("CREATE SOURCE"):
            out.append("SET streaming_parallelism = 2;\n" + part)
        elif part.startswith("CREATE MATERIALIZED VIEW"):
            if mv_values is not None:
                if mv_index >= len(mv_values):
                    raise ValueError(
                        f"build_sql_with_parallelism: {sql_path.name} has more "
                        f"CREATE MATERIALIZED VIEW statements than parallelism "
                        f"values supplied ({len(mv_values)})"
                    )
                val = mv_values[mv_index]
                mv_index += 1
            else:
                val = parallelism
            out.append(f"SET streaming_parallelism = {val};\n" + part)
        elif part.startswith("CREATE SINK"):
            out.append("SET streaming_parallelism = 1;\n" + part)
        else:
            out.append(part)

    if mv_values is not None and mv_index != len(mv_values):
        raise ValueError(
            f"build_sql_with_parallelism: {sql_path.name} has {mv_index} "
            f"CREATE MATERIALIZED VIEW statements but {len(mv_values)} "
            f"parallelism values were supplied"
        )
    return "".join(out)


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def _parallelism_tag(parallelism):
    """Filesystem/job-id-safe representation -- an int stays as-is, a list
    becomes hyphen-joined (e.g. [2,4,8] -> "2-4-8"), since commas/spaces/
    brackets are unsafe in filenames and manifest-key segments.
    """
    if isinstance(parallelism, list):
        return "-".join(str(p) for p in parallelism)
    return str(parallelism)


def _parallelism_meta(parallelism):
    """metaInfo/--job_parallelization representation -- comma-separated,
    matching every other engine's job_pds convention and what
    1_data_extractor.py already parses via .split(',').
    """
    if isinstance(parallelism, list):
        return ",".join(str(p) for p in parallelism)
    return str(parallelism)


def run_key(workload_name, parallelism, event_rate):
    return f"{workload_name}|{_parallelism_tag(parallelism)}|{event_rate}"


def run_one_combo(workload_name, query_number, sql_relpath, in_topic, out_topic,
                   app_type, needs_window, parallelism, event_rate, dry_run=False,
                   enumeration_strategy="None"):
    label = f"{workload_name} Q{query_number} | parallelism={parallelism} | rate={event_rate}"
    log(f"=== {label} ===")
    if dry_run:
        log("  (dry run, skipping)")
        return {"label": label, "status": "dry_run"}

    prefix = object_prefix_for(workload_name)

    sh(f"{KAFKA_BIN}/kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} --create --topic {in_topic} --partitions 6 --if-not-exists 2>&1")
    sh(f"{KAFKA_BIN}/kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} --create --topic {out_topic} --partitions 6 --if-not-exists 2>&1")

    drop_all(prefix, workload_name=workload_name)

    sql_path = RISINGWAVE_DIR / sql_relpath
    content = build_sql_with_parallelism(sql_path, KAFKA_BOOTSTRAP, needs_window, parallelism, workload_name=workload_name)
    tmp_sql = f"/tmp/sweep_rw_{prefix}_{_parallelism_tag(parallelism)}_{event_rate}.sql"
    with open(tmp_sql, "w") as f:
        f.write(content)

    # 240s, not 90 -- discovered live (2026-08-09) that Sentiment Analysis's
    # afinn_lexicon reference table (a ~2477-row literal INSERT INTO ...
    # VALUES, see WORKLOAD_AUX_TABLES) alone can push total submission time
    # (DROP+CREATE TABLE+INSERT+CREATE SOURCE/MVIEW/SINK, all one psql -f
    # script) past 90s even on a healthy cluster -- the client gave up
    # before ever reaching the CREATE SOURCE/MVIEW/SINK statements that
    # actually matter for the run, repeatedly, deterministically. A higher
    # ceiling costs nothing for every other (fast) workload -- it's a
    # ceiling, not a fixed wait.
    #
    # Raised again to 600s (2026-08-15): at 500k events/sec, Word Count's
    # CREATE MATERIALIZED VIEW backfill genuinely takes longer than 240s to
    # consume through the initial backlog (`SHOW JOBS` confirmed real,
    # non-stalled progress -- millions of rows consumed, just slowly) --
    # this isn't a stuck job, the client was just giving up too early. Worse,
    # every timeout here triggers this function's own auto-RECOVER (see the
    # TimeoutExpired handler in run_strategy_sweep_risingwave.py), which
    # resets the control stream and interrupts backfill actors mid-flight
    # (confirmed live via risingwave-compute.log's repeating "control stream
    # reset ... end of stream" / "backfill_resume invoked twice" warnings,
    # roughly once per timeout) -- so a too-short timeout doesn't just fail
    # once, it actively prevents the job from ever completing by continually
    # resetting its own progress.
    r = sh(f'{PSQL} -h {MAIN_NODE} -p {RW_PORT} -d dev -U root -f {tmp_sql} 2>&1', timeout=600)
    if "ERROR" in r.stdout:
        log(f"  SQL SUBMISSION ERROR:\n{r.stdout}")
        return {"label": label, "status": "sql_submit_failed", "output": r.stdout}

    folder = CONTROLLER_DIR / workload_name
    # Snapshot before launching getprom_risingwave.py -- confirmed live
    # (2026-07-28) that just taking files[-1] after the fact silently
    # re-reports a PREVIOUS combo's file (and the manifest records "ok")
    # if this run's own subprocess crashes/fails to write, since the old
    # file is still the newest one that *exists*. Must compare against
    # what already existed before this specific run.
    files_before = set(glob.glob(str(folder / "evaluationMetrics-*.json")))

    env = os.environ.copy()
    env["PSTREAMBENCH_DATA_DIR"] = DATA_DIR
    proc = subprocess.Popen(
        [VENV_PY, "custom_producer.py", "--event_per_second", str(event_rate),
         "--app_type", app_type, "--kafka_bootstrap_server", KAFKA_BOOTSTRAP,
         "--kafka_input_topic", in_topic],
        cwd=str(PRODUCER_DIR), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(RUN_TIME_MINUTES * 60)

        job_id = f"{prefix}_p{_parallelism_tag(parallelism)}_r{event_rate}"
        args = [
            VENV_PY, str(GETPROM_SCRIPT),
            "--main_node_ip", MAIN_NODE,
            "--rw_port", RW_PORT,
            "--job_id", job_id,
            "--job_class", workload_name,
            "--job_run_time", str(RUN_TIME_MINUTES),
            "--job_parallelization", _parallelism_meta(parallelism),
            "--job_query_number", query_number,
            "--job_window_size", str(WINDOW_SIZE_SEC * 1000 if needs_window else 0),
            "--job_window_slide_size", str(WINDOW_SLIDE_SEC * 1000 if needs_window else 0),
            "--job_sample", "0",
            "--job_iteration", "0",
            "--producer_event_per_second", str(event_rate),
            "--hardware_type", HARDWARE_TYPE,
            "--num_nodes", NUM_NODES,
            "--num_slots", NUM_SLOTS,
            "--enumeration_strategy", enumeration_strategy,
        ]
        r2 = subprocess.run(args, cwd=str(CONTROLLER_DIR), capture_output=True, text=True, timeout=120)
    finally:
        proc.terminate()

    files_after = set(glob.glob(str(folder / "evaluationMetrics-*.json")))
    new_files = sorted(files_after - files_before, key=os.path.getmtime)
    result = {"label": label, "parallelism": parallelism, "event_rate": event_rate}
    if r2.returncode != 0 or not new_files:
        log(f"  NO NEW METRICS FILE WRITTEN (returncode={r2.returncode}). stderr:\n{r2.stderr[-3000:]}")
        result["status"] = "no_metrics_file"
        result["stderr"] = r2.stderr[-3000:]
        result["returncode"] = r2.returncode
    else:
        latest = new_files[-1]
        bad_field = _check_core_metrics_present(latest)
        if bad_field:
            log(f"  METRICS FILE HAS NULL CORE FIELD ({bad_field}), discarding: {latest}")
            result["status"] = "null_core_metrics"
            result["bad_field"] = bad_field
            # Discard rather than leave behind: this status isn't "ok" so
            # the strategy driver won't move it into risingwave/<strategy>/,
            # but the file would still sit in the flat dsp_be/<workload>/
            # folder where the uniform sweep's own check_nulls also scans --
            # leaving it there would contaminate that separate accounting.
            meta_sibling = latest.replace("evaluationMetrics-", "metaInfo-")
            for f in (latest, meta_sibling):
                if os.path.exists(f):
                    os.remove(f)
        else:
            result["status"] = "ok"
            result["file"] = latest
            log(f"  metrics written: {latest}")

    drop_all(prefix, workload_name=workload_name)
    return result


def _check_core_metrics_present(metrics_file):
    """Reject a combo as non-"ok" if its Kafka/hardware collection silently
    degraded to nulls -- confirmed live (2026-08-13) this happens whenever
    the Kafka JMX exporter or node_exporter isn't actually reachable (e.g.
    after a manual broker restart that forgot KAFKA_OPTS), while getprom
    itself still exits 0 and writes a file, so the existing
    returncode/file-exists check alone let ~54 silently-null files through
    as "ok" before this was caught by a manual audit. Scoped to just the
    fields actually seen going null from a real infra outage (not a full
    find_nulls() sweep) since some fields are legitimately null for
    specific workloads (e.g. Linear Road's untracked vehicle_latest/
    segment_stats stages) and shouldn't block an otherwise-good run.
    Returns the dotted field path that was null, or None if all present.
    """
    with open(metrics_file) as fh:
        metrics = json.load(fh)
    checks = {
        "queryMetrics.kafka_throughput.throughput": metrics.get("queryMetrics", {}).get("kafka_throughput", {}).get("throughput"),
        "queryMetrics.kafka_event_rate.event rate": metrics.get("queryMetrics", {}).get("kafka_event_rate", {}).get("event rate"),
        "hardwareMetrics.node_memory_98": metrics.get("hardwareMetrics", {}).get("node_memory_98"),
    }
    for path, value in checks.items():
        if value is None:
            return path
    return None


def find_nulls(obj, path="root"):
    findings = []
    if obj is None:
        findings.append((path, "None"))
    elif isinstance(obj, float) and obj != obj:
        findings.append((path, "NaN"))
    elif isinstance(obj, dict):
        for k, v in obj.items():
            findings.extend(find_nulls(v, f"{path}.{k}"))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            findings.extend(find_nulls(v, f"{path}[{i}]"))
    return findings


@click.group()
def cli():
    pass


@cli.command()
@click.option("--dry-run", is_flag=True, default=False)
def run(dry_run):
    manifest = load_manifest()
    results = []
    total = len(WORKLOADS) * len(PARALLELISM_LEVELS) * len(EVENT_RATES)
    done = 0
    for workload_name, query_number, sql_relpath, in_topic, out_topic, app_type, needs_window in WORKLOADS:
        for parallelism in PARALLELISM_LEVELS:
            for event_rate in EVENT_RATES:
                key = run_key(workload_name, parallelism, event_rate)
                done += 1
                if manifest.get(key) == "ok":
                    log(f"[{done}/{total}] SKIP (already done): {key}")
                    continue
                log(f"[{done}/{total}] {key}")
                try:
                    result = run_one_combo(
                        workload_name, query_number, sql_relpath, in_topic, out_topic,
                        app_type, needs_window, parallelism, event_rate, dry_run=dry_run,
                    )
                except Exception as e:
                    log(f"  UNEXPECTED EXCEPTION: {e!r}")
                    result = {"label": key, "status": f"exception: {e!r}"}
                results.append(result)
                manifest[key] = result.get("status", "unknown")
                save_manifest(manifest)
                if not dry_run:
                    log(f"  sleeping {GAP_SECONDS}s gap before next combo...")
                    time.sleep(GAP_SECONDS)

    log("\n=== SUMMARY ===")
    for r in results:
        log(f"{r['label']} -> {r.get('status')}")


@cli.command()
def status():
    manifest = load_manifest()
    total = len(WORKLOADS) * len(PARALLELISM_LEVELS) * len(EVENT_RATES)
    ok = sum(1 for v in manifest.values() if v == "ok")
    log(f"{ok}/{total} combos completed ok, {len(manifest) - ok} attempted-but-not-ok, {total - len(manifest)} not yet started")
    for k, v in sorted(manifest.items()):
        log(f"  {k}: {v}")


@cli.command()
def check_nulls():
    """Scan every evaluationMetrics-*.json this sweep wrote for null/NaN values."""
    total_files = 0
    total_nulls = 0
    for workload_name, *_ in WORKLOADS:
        folder = CONTROLLER_DIR / workload_name
        for f in sorted(glob.glob(str(folder / "evaluationMetrics-*.json")), key=os.path.getmtime):
            with open(f) as fh:
                metrics = json.load(fh)
            nulls = find_nulls(metrics)
            total_files += 1
            total_nulls += len(nulls)
            status = "CLEAN" if not nulls else f"{len(nulls)} NULLS"
            log(f"{f}: {status}")
            for p, v in nulls:
                log(f"    NULL at {p} = {v}")
    log(f"\n=== {total_files} files scanned, {total_nulls} total null findings ===")


if __name__ == "__main__":
    cli()
