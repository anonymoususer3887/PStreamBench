"""
Local, single-node validation harness for the observation-driven
parallelism strategies (ds2_strategy / dhalion_strategy in
parallelism_strategies.py) -- deliberately separate from
run_sweep_risingwave.py, not a CLI flag on it, because the two scripts
target genuinely different things: run_sweep_risingwave.py drives the
real CloudLab cluster for the production parallelism/event-rate sweep,
this script drives the local `risingwave single-node` + local Kafka
already running on this Mac (confirmed listening on 4566/9092, see
BUGS_AND_FIXES.md-style session notes) purely to get real operatorMetrics
to feed ds2_strategy/dhalion_strategy and see what they actually
recommend, without waiting on CloudLab availability.

Both new strategies need a *baseline* run's measured metrics before they
can propose anything (unlike uniform/exhaustive/minavgmax/increasing/
random, which need no prior observation) -- so the flow per workload is:
  1. deploy at MIN_PARALLELISM on every controllable stage (the
     "uniform" baseline), run, collect metrics.
  2. feed that baseline's operatorMetrics into ds2_strategy and
     dhalion_strategy independently -- each proposes its own nudged
     assignment.
  3. deploy each proposed assignment in turn, run, collect metrics.
  4. every run's evaluationMetrics/metaInfo files get moved into
     <workload>/<strategy>/ (baseline counted as "uniform"), so results
     are organized by strategy within each workload's folder, matching
     the flat directory getprom_risingwave.py already writes into for
     the CloudLab sweep (see run_sweep_risingwave.py's WORKLOADS/output
     handling) but namespaced one level deeper.

Reuses run_sweep_risingwave.py's SQL-building/drop/prefix helpers by
importing that module and overriding its cluster-target globals (MAIN_
NODE, RW_PORT, KAFKA_BOOTSTRAP) to localhost -- those helpers read the
globals at call time (not at def time), so overriding after import is
sufficient and avoids duplicating drop_all/build_sql_with_parallelism/
sh_resilient's subtler logic (RECOVER-on-timeout, DROP ordering, etc).

Usage:
    python3 run_strategy_local.py run --workload "Word Count"
    python3 run_strategy_local.py run --workload "Word Count" --dry-run
"""
import glob
import json
import os
import subprocess
import time
from pathlib import Path

import click

import run_sweep_risingwave as base
import parallelism_strategies as strat

# --- point the imported module's helpers at localhost instead of CloudLab ---
base.MAIN_NODE = "localhost"
base.RW_PORT = "4566"
base.KAFKA_BOOTSTRAP = "localhost:9092"
base.PSQL = "/opt/homebrew/opt/libpq/bin/psql"
base.KAFKA_BIN = "/opt/homebrew/opt/kafka/bin"
base.HARDWARE_TYPE = "local-mac"
base.NUM_NODES = "1"
base.NUM_SLOTS = "1"
base.VENV_PY = "/private/tmp/claude-503/-Users-pratyushagnihotri-AI-Agent-PStreamBench/804c377f-4eaa-46bd-8f13-6cf489e49052/scratchpad/sweep_venv/bin/python3"

RUN_TIME_MINUTES_LOCAL = 3  # shorter than the CloudLab sweep's 5 -- this is
                            # strategy validation, not the final published
                            # numbers; long enough for busy_ratio/backpressure
                            # to stabilize on a single-node local instance.

# (workload_name, strategy_stage_names, event_rate) -- strategy_stage_names
# are the getprom_risingwave.py operatorMetrics keys for exactly the
# *controllable* stages, in pipeline order (see parallelism_strategies.py's
# load_stage_metrics_risingwave docstring for why this can't just be
# "every stage").
STRATEGY_TARGETS = {
    "Word Count": {
        "query_number": "1", "sql_relpath": "wordcount/word_count.sql",
        "in_topic": "WordCountIn", "out_topic": "WordCountOut",
        "app_type": "Word_Count", "needs_window": False,
        "controllable_stage_names": ["counter"],
    },
    "Ad Analytics": {
        "query_number": "2", "sql_relpath": "AdsAnalytics/ads_analytics_q2.sql",
        "in_topic": "AdAnalyticsIn", "out_topic": "AdAnalyticsOut",
        "app_type": "Ad_Analytics", "needs_window": True,
        "controllable_stage_names": ["ctr"],
    },
}

BASELINE_EVENT_RATE = 100_000


def log(msg):
    print(f"[run_strategy_local] {msg}", flush=True)


def strategy_folder(workload_name, strategy_name):
    folder = base.CONTROLLER_DIR / workload_name / strategy_name
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def move_new_metrics_files(workload_name, strategy_name, before_eval, before_meta):
    """Moves whatever evaluationMetrics-*.json / metaInfo-*.json files
    appeared since `before_*` (getprom_risingwave.py always writes both
    with the same run_timestamp suffix) into <workload>/<strategy_name>/,
    using the same before/after-snapshot pattern run_sweep_risingwave.py's
    run_one_combo() already uses to avoid silently re-reporting a stale
    file from a previous run.
    """
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


def run_one(workload_name, cfg, parallelism, event_rate, strategy_name, dry_run=False):
    """One deploy-run-collect cycle at a single (uniform, since RisingWave
    fuses everything not itself the swept MATERIALIZED VIEW into the
    source's fixed parallelism -- see build_sql_with_parallelism)
    parallelism value for every controllable stage, reusing
    run_sweep_risingwave.run_one_combo's exact machinery, then relocates
    the resulting files into <workload>/<strategy_name>/.
    """
    flat_folder = base.CONTROLLER_DIR / workload_name
    before_eval = set(glob.glob(str(flat_folder / "evaluationMetrics-*.json")))
    before_meta = set(glob.glob(str(flat_folder / "metaInfo-*.json")))

    result = base.run_one_combo(
        workload_name, cfg["query_number"], cfg["sql_relpath"],
        cfg["in_topic"], cfg["out_topic"], cfg["app_type"], cfg["needs_window"],
        parallelism, event_rate, dry_run=dry_run,
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


@click.group()
def cli():
    pass


@cli.command()
@click.option("--workload", required=True, type=click.Choice(list(STRATEGY_TARGETS)))
@click.option("--event-rate", default=BASELINE_EVENT_RATE, type=int)
@click.option("--dry-run", is_flag=True, default=False)
def run(workload, event_rate, dry_run):
    cfg = STRATEGY_TARGETS[workload]
    stage_names = cfg["controllable_stage_names"]
    num_stages = len(stage_names)

    base.RUN_TIME_MINUTES = RUN_TIME_MINUTES_LOCAL

    # --- Step 1: baseline at MIN_PARALLELISM on every controllable stage ---
    baseline_p = strat.MIN_PARALLELISM
    log(f"=== baseline (uniform) {workload} | parallelism={baseline_p} | rate={event_rate} ===")
    baseline_result = run_one(workload, cfg, baseline_p, event_rate, "uniform", dry_run=dry_run)
    log(f"baseline result: {baseline_result.get('status')}")
    if dry_run or baseline_result.get("status") != "ok":
        log("baseline did not complete ok -- stopping here (nothing to feed ds2/dhalion)")
        return

    baseline_file = baseline_result["file"]
    stage_metrics = strat.load_stage_metrics_risingwave(baseline_file, stage_names)
    log(f"baseline stage_metrics ({stage_names}): {stage_metrics}")

    current_assignment = [baseline_p] * num_stages

    # --- Step 2: compute each strategy's proposed nudge from that baseline ---
    ds2_assignment = strat.ds2_strategy(stage_metrics, event_rate, current_assignment)[0]
    dhalion_assignment = strat.dhalion_strategy(stage_metrics, current_assignment)[0]
    log(f"ds2_strategy proposes: {ds2_assignment}")
    log(f"dhalion_strategy proposes: {dhalion_assignment}")

    # --- Step 3: deploy and measure each proposal ---
    # NOTE: run_one_combo takes a single int (all controllable stages get
    # the same value via build_sql_with_parallelism's per-MV SET) -- fine
    # today since every STRATEGY_TARGETS workload has exactly one
    # controllable stage; a multi-stage workload (Machine Outlier) would
    # need build_sql_with_parallelism extended to accept one value per MV
    # in source order, not yet done since neither workload here needs it.
    assert num_stages == 1, "run_one_combo only accepts one parallelism value; extend for multi-stage workloads"

    for strategy_name, assignment in [("ds2", ds2_assignment), ("dhalion", dhalion_assignment)]:
        p = assignment[0]
        log(f"=== {strategy_name} {workload} | parallelism={p} | rate={event_rate} ===")
        result = run_one(workload, cfg, p, event_rate, strategy_name, dry_run=dry_run)
        log(f"{strategy_name} result: {result.get('status')}")

    log("\n=== SUMMARY ===")
    log(f"baseline (uniform, p={baseline_p}): {baseline_result.get('status')} -> {baseline_result.get('file')}")
    log(f"ds2 (p={ds2_assignment[0]}): see {workload}/ds2/")
    log(f"dhalion (p={dhalion_assignment[0]}): see {workload}/dhalion/")


if __name__ == "__main__":
    cli()
