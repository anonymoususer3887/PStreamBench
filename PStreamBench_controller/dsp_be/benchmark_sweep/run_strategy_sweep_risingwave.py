"""
Non-uniform per-stage parallelism benchmarking for RisingWave -- Random,
DS2, and Dhalion, against the real CloudLab cluster, for all 21 workload/
query combos (base WORKLOADS + ADDITIONAL_QUERY_VARIANTS).

Unlike run_strategy_local.py (which targets a local single-node Mac
instance purely to validate ds2_strategy/dhalion_strategy against real
metrics before committing cluster time), this script reuses
run_sweep_risingwave's already-configured MAIN_NODE/RW_PORT/KAFKA_BOOTSTRAP
as-is -- no localhost override -- since the local validation already
happened (Word Count, 2026-07-28) and this is the real run.

Data isolation: every combo's evaluationMetrics/metaInfo pair is moved out
of the flat dsp_be/<Job Class>/ folder into
dsp_be/<Job Class>/risingwave/<strategy>/ (random/ds2/dhalion, baseline
saved as uniform/) -- the engine-name layer keeps this isolated both from
the existing flat uniform-sweep data and from any future Flink/Storm/
Spark/Kafka Streams strategy runs landing in the same <Job Class>/ folder.
Reuses run_strategy_local.py's strategy_folder()/move_new_metrics_files()
*pattern* (adapted here to insert the engine segment), so nothing here can
overwrite or collide with the existing uniform-sweep data or its
manifest_risingwave.json entries -- this script keeps its own separate
manifest (manifest_strategy_risingwave.json).

CONTROLLABLE_STAGE_NAMES maps each workload to the getprom_risingwave.py
operatorMetrics key(s) for its real MVIEW-typed stage(s), in the same
top-to-bottom order the CREATE MATERIALIZED VIEW statements appear in the
SQL file (verified against every stage_relations list in
getprom_risingwave.py, cross-checked against a live count of
'^CREATE MATERIALIZED VIEW' per SQL file -- see run_strategy_sweep's own
`--check-stages` command). Linear Road is a known partial-instrumentation
case: its SQL has 3 MVs but getprom_risingwave.py's getLinearRoad() only
tracks one ("result") as MVIEW-typed -- the other two (vehicle_latest,
segment_stats) have no real operatorMetrics entry, so
load_stage_metrics_risingwave() safely returns neutral defaults for them
(DS2/Dhalion leave those two stages unchanged from baseline rather than
guessing) -- not a bug here, a pre-existing getprom coverage gap.

Usage:
    python3 run_strategy_sweep_risingwave.py run --dry-run
    python3 run_strategy_sweep_risingwave.py run --strategy random
    python3 run_strategy_sweep_risingwave.py run --strategy ds2
    python3 run_strategy_sweep_risingwave.py run --strategy dhalion
    python3 run_strategy_sweep_risingwave.py run  # all three
    python3 run_strategy_sweep_risingwave.py check-stages
"""
import glob
import json
import os
import time
from pathlib import Path

import click

import run_sweep_risingwave as base
import parallelism_strategies as strat

MANIFEST_PATH = base.SCRIPT_DIR / "manifest_strategy_risingwave.json"

RANDOM_LEVELS = [2, 4, 8, 16, 32, 40]
RANDOM_SAMPLES = 10
RANDOM_SEED = 42
BASELINE_EVENT_RATE = 100_000

ALL_COMBOS = list(base.WORKLOADS) + list(base.ADDITIONAL_QUERY_VARIANTS)

# workload_name -> getprom_risingwave.py operatorMetrics key(s) for its
# MVIEW-typed (genuinely controllable) stage(s), in SQL CREATE-statement
# order. One list entry per CREATE MATERIALIZED VIEW.
CONTROLLABLE_STAGE_NAMES = {
    "Word Count": ["counter"],
    "Ad Analytics": ["ctr"],
    "Smart Grid": ["window"],
    "Log Processing": ["window"],
    "Machine Outlier": ["reference", "outlier"],
    "Bargain Index": ["vwap"],
    "Trending Topics": ["counter"],
    "Click Analytics": ["counter"],
    "Traffic Monitoring": ["avgspeed"],
    "Spike Detection": ["average", "spikedetector"],
    "Sentiment Analysis": ["analyser"],
    "TPCH": ["counter"],
    "Google cloud Monitoring": ["aggregate"],
    "Linear Road": ["vehicle_latest", "segment_stats", "result"],
    "Ad Analytics Q1": ["ctr"],
    "Click Analytics Q2": ["counter"],
    "Click Analytics Q3": ["counter"],
    "Smart Grid Q2": ["window"],
    "Log Processing Q2": ["window"],
    "TPCH Q2": ["counter"],
    "Google cloud Monitoring Q2": ["aggregate"],
}


def log(msg):
    print(f"[run_strategy_sweep_risingwave] {msg}", flush=True)


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def strategy_key(workload_name, strategy_name, tag, event_rate):
    return f"{strategy_name}|{workload_name}|{tag}|{event_rate}"


ENGINE_NAME = "risingwave"


def strategy_folder(workload_name, strategy_name):
    # <workload>/<streaming_system>/<strategy>/ -- the engine layer keeps
    # this isolated from any future Flink/Storm/Spark/Kafka Streams
    # strategy runs landing in the same <workload>/ folder later, and from
    # the existing flat uniform-sweep data (which never used a strategy or
    # engine subfolder at all).
    folder = base.CONTROLLER_DIR / workload_name / ENGINE_NAME / strategy_name
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def move_new_metrics_files(workload_name, strategy_name, before_eval, before_meta):
    """Same before/after-snapshot relocation pattern as run_strategy_local.py."""
    flat_folder = base.CONTROLLER_DIR / workload_name
    after_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    after_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))
    new_eval = sorted(after_eval - before_eval, key=os.path.getmtime)
    new_meta = sorted(after_meta - before_meta, key=os.path.getmtime)

    dest = strategy_folder(workload_name, strategy_name)
    moved = []
    for f in new_eval + new_meta:
        target = dest / Path(f).name
        os.rename(f, target)
        moved.append(str(target))
    return new_eval, moved


def recover_if_stalled(exc):
    """run_one_combo's SQL-submission call (unlike drop_all()'s DROP calls,
    which are wrapped in sh_resilient() and already self-heal via RECOVER
    on timeout) has no built-in RECOVER-on-timeout -- confirmed live
    (2026-08-08): once the cluster enters the familiar high-barrier-latency
    stall, every subsequent SQL submission timed out and failed back-to-back
    for 25+ combos in a row (Sentiment Analysis/TPCH/Google cloud
    Monitoring) with no self-recovery, silently wasting real cluster time
    across this multi-hour sweep until caught manually. Only issue RECOVER
    for a genuine submission timeout (TimeoutExpired) -- a real SQL error
    (sql_submit_failed) wouldn't be fixed by it and RECOVER itself briefly
    disrupts in-flight jobs, so it's not free to call unconditionally.
    """
    if "TimeoutExpired" not in repr(exc):
        return
    log(f"  submission timed out ({exc!r}) -- issuing RECOVER before the next combo")
    try:
        base.sh(f'{base.PSQL} -h {base.MAIN_NODE} -p {base.RW_PORT} -d dev -U root -c "RECOVER;" 2>&1', timeout=30)
    except Exception as recover_exc:
        log(f"  RECOVER itself failed: {recover_exc!r}")


def num_controllable_stages(sql_relpath):
    import re
    content = (base.RISINGWAVE_DIR / sql_relpath).read_text()
    return len(re.findall(r'(?m)^CREATE MATERIALIZED VIEW', content))


def run_one(workload_name, query_number, sql_relpath, in_topic, out_topic,
            app_type, needs_window, parallelism, event_rate, strategy_name,
            dry_run=False):
    """One deploy-run-collect cycle at a given (possibly per-stage-list)
    parallelism, reusing run_sweep_risingwave.run_one_combo's exact
    machinery, then relocates the resulting files into
    <workload>/<strategy_name>/.
    """
    flat_folder = base.CONTROLLER_DIR / workload_name
    before_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    before_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))

    result = base.run_one_combo(
        workload_name, query_number, sql_relpath, in_topic, out_topic,
        app_type, needs_window, parallelism, event_rate, dry_run=dry_run,
        enumeration_strategy=strategy_name,
    )
    if dry_run or result.get("status") != "ok":
        return result

    new_eval, moved = move_new_metrics_files(workload_name, strategy_name, before_eval, before_meta)
    if not new_eval:
        log(f"  WARNING: run_one_combo reported ok but no new evaluationMetrics file was found to move")
        result["status"] = "ok_but_file_not_found_for_move"
        return result

    result["file"] = str(strategy_folder(workload_name, strategy_name) / Path(new_eval[-1]).name)
    log(f"  saved -> {result['file']}")
    return result


def run_random(manifest, event_rate, dry_run=False):
    for workload_name, query_number, sql_relpath, in_topic, out_topic, app_type, needs_window in ALL_COMBOS:
        num_stages = num_controllable_stages(sql_relpath)
        assignments = strat.random_strategy(RANDOM_LEVELS, num_stages, RANDOM_SAMPLES, seed=RANDOM_SEED)
        for i, assignment in enumerate(assignments):
            key = strategy_key(workload_name, "random", f"s{i}", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            log(f"[random] {workload_name} sample {i}/{len(assignments)} rate={event_rate} -> {assignment}")
            try:
                result = run_one(
                    workload_name, query_number, sql_relpath, in_topic, out_topic,
                    app_type, needs_window, assignment, event_rate, "random",
                    dry_run=dry_run,
                )
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                recover_if_stalled(e)
                result = {"status": f"exception: {e!r}"}
            manifest[key] = result.get("status", "unknown")
            save_manifest(manifest)
            if not dry_run:
                log(f"  sleeping {base.GAP_SECONDS}s gap before next combo...")
                time.sleep(base.GAP_SECONDS)


def run_baseline_then_nudge(manifest, event_rate, dry_run=False):
    """Shared baseline-then-nudge flow for DS2 and Dhalion -- one baseline
    deploy per workload covers both strategies (only the nudge formula
    differs), matching the plan's cost accounting (1 baseline + 2 proposed
    runs per workload, not 4).
    """
    for workload_name, query_number, sql_relpath, in_topic, out_topic, app_type, needs_window in ALL_COMBOS:
        num_stages = num_controllable_stages(sql_relpath)
        stage_names = CONTROLLABLE_STAGE_NAMES[workload_name]
        assert len(stage_names) == num_stages, (
            f"{workload_name}: CONTROLLABLE_STAGE_NAMES has {len(stage_names)} "
            f"entries but the SQL file has {num_stages} CREATE MATERIALIZED VIEW "
            f"statements -- fix the mapping before running"
        )

        baseline_assignment = [strat.MIN_PARALLELISM] * num_stages
        baseline_key = strategy_key(workload_name, "uniform", "baseline", event_rate)
        baseline_file = None

        if manifest.get(baseline_key) == "ok":
            log(f"SKIP (already done): {baseline_key}")
            baseline_file = manifest.get(baseline_key + "|file")
        else:
            log(f"[baseline] {workload_name} rate={event_rate} -> {baseline_assignment}")
            try:
                result = run_one(
                    workload_name, query_number, sql_relpath, in_topic, out_topic,
                    app_type, needs_window, baseline_assignment, event_rate,
                    "uniform", dry_run=dry_run,
                )
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                recover_if_stalled(e)
                result = {"status": f"exception: {e!r}"}
            manifest[baseline_key] = result.get("status", "unknown")
            if result.get("status") == "ok":
                baseline_file = result["file"]
                manifest[baseline_key + "|file"] = baseline_file
            save_manifest(manifest)
            if not dry_run:
                log(f"  sleeping {base.GAP_SECONDS}s gap before next combo...")
                time.sleep(base.GAP_SECONDS)

        if dry_run or not baseline_file:
            log(f"  no baseline file available for {workload_name} -- skipping DS2/Dhalion nudge")
            continue

        stage_metrics = strat.load_stage_metrics_risingwave(baseline_file, stage_names)

        for strategy_name, strategy_fn, extra_args in [
            ("ds2", strat.ds2_strategy, (event_rate,)),
            ("dhalion", strat.dhalion_strategy, ()),
        ]:
            key = strategy_key(workload_name, strategy_name, "nudge", event_rate)
            if manifest.get(key) == "ok":
                log(f"SKIP (already done): {key}")
                continue
            assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
            log(f"[{strategy_name}] {workload_name} rate={event_rate} -> {assignment}")
            try:
                result = run_one(
                    workload_name, query_number, sql_relpath, in_topic, out_topic,
                    app_type, needs_window, assignment, event_rate,
                    strategy_name, dry_run=dry_run,
                )
            except Exception as e:
                log(f"  UNEXPECTED EXCEPTION: {e!r}")
                recover_if_stalled(e)
                result = {"status": f"exception: {e!r}"}
            manifest[key] = result.get("status", "unknown")
            save_manifest(manifest)
            if not dry_run:
                log(f"  sleeping {base.GAP_SECONDS}s gap before next combo...")
                time.sleep(base.GAP_SECONDS)


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
def check_stages():
    """Verify CONTROLLABLE_STAGE_NAMES matches each SQL file's real MV count."""
    ok = True
    for workload_name, query_number, sql_relpath, *_ in ALL_COMBOS:
        expected = num_controllable_stages(sql_relpath)
        actual = len(CONTROLLABLE_STAGE_NAMES.get(workload_name, []))
        status = "OK" if expected == actual else "MISMATCH"
        if status == "MISMATCH":
            ok = False
        log(f"{status}: {workload_name:35s} sql_mvs={expected} mapped_stage_names={actual}")
    if not ok:
        raise SystemExit(1)


@cli.command()
def status():
    manifest = load_manifest()
    ok = sum(1 for v in manifest.values() if v == "ok")
    log(f"{ok} entries ok, {len(manifest) - ok} attempted-but-not-ok")
    for k, v in sorted(manifest.items()):
        log(f"  {k}: {v}")


if __name__ == "__main__":
    cli()
