#!/usr/bin/env python3
"""Full batch driver for 7_plot_pdsp_enumeration_analysis.py (Pipeline 2 + Pipeline 3).

This is the one place that owns the full workload x hardware x event-rate x metric
matrix for the enumeration-strategy plot families. It replaces ad-hoc shell loops
(previous ones lived only in a scratch script and only covered one example workload)
with a reproducible, committed driver that 7_make_enumeration_plots.sh's `batch` /
`batch-dry` commands call into.

Mode coverage and loop shape (why each mode loops what it loops):
  B  config_perf_single   -- hw x rate x metric x strategy. Framework-group is always
                              all_5 only: B's output path has no group component, and
                              all_5 is a superset of every framework, so looping the
                              other two groups would just rewrite the same files.
  C  config_perf_compare  -- hw x rate x metric x strategy x group (path includes group).
  03B config_eventrate    -- hw x metric x strategy x group. Rate is NOT looped: the
                              function always sweeps RATE_VALUES internally for the
                              x-axis, so different --rate values produce identical
                              content at a different nominal path -- wasteful to repeat.
  F  selected             -- hw x rate x group. Metric is FIXED at throughput_50: F
                              selects the best config via build_configs(...,metric,...)
                              directly, so looping metric would change *which config
                              counts as best*, not just what gets reported (this
                              project's rule is "selection objective: throughput only").
  E  strategy_framework   -- same shape and same throughput-only constraint as F.
  25 tp_latency           -- hw x rate x group. Fixed throughput/latency pair, not a
                              single `--metric` axis.
  18 selected_performance -- hw x rate x metric x group. Safe to loop metric: it
                              selects via selected_for(), which always re-selects by
                              throughput_50 first and only *reports* the requested
                              metric for that same selected config.
  22 strategy_hardware    -- rate x metric x group. --hw is NOT looped: the function
                              sweeps HW_VALUES internally and never reads args.hw for
                              content, only --rate (fixed) and the output path.
  23 strategy_rate        -- hw x metric x group. --rate is NOT looped, mirror of 22.
  21 strategy_workload    -- hw x rate x metric x group, once (query/variant blank --
                              the function sweeps every workload in the group itself).
  G  operator_own_metrics -- hw x rate x own-metric x group. Independent of
                              operator_templates.csv entirely (raw operator names), so
                              it already has full workload coverage regardless of B/C/E/F.

Usage:
  PYTHON=/path/to/venv/bin/python python3 12_run_full_enumeration_batch.py --dry-run
  PYTHON=/path/to/venv/bin/python python3 12_run_full_enumeration_batch.py --workers 10 \
      --log /tmp/full_batch.log
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "7_plot_pdsp_enumeration_analysis.py")
CSV_PATH = os.path.join(HERE, "query_performance_data.csv")

GROUPS = ["all_5", "flink_spark_storm", "kafka_risingwave"]
GROUP_FRAMEWORKS = {
    "all_5": ["flink", "storm", "spark", "kafkastream", "risingwave"],
    "flink_spark_storm": ["flink", "spark", "storm"],
    "kafka_risingwave": ["kafkastream", "risingwave"],
}
STRATEGIES = ["uniform", "random", "ds2", "dhalion"]
METRICS = ["throughput_50", "endtoend_latency", "cpu_50", "memory_50", "network_50"]
HW_LIST = ["m510", "c6525-25g", "c6525-100g"]
RATE_LIST = [100000, 500000, 1000000]
OWN_METRICS = ["throughput_per_sec", "selectivity", "backpressure_ratio", "busy_ratio"]
SELECTION_METRIC = "throughput_50"  # the project's one selection objective (F / E)


def workloads_for(df, group):
    fws = GROUP_FRAMEWORKS[group]
    sub = df[df["framework"].isin(fws)].copy()
    sub["query_variant"] = sub["query_variant"].fillna("")
    pairs = sub[["query", "query_variant"]].drop_duplicates()
    return sorted(pairs.itertuples(index=False))


def build_calls(df):
    """Returns a list of argv lists (without the leading script path)."""
    calls = []

    def emit(mode, group, query, variant, hw, rate, metric, extra=()):
        argv = ["--mode", mode, "--framework-group", group, "--query", query,
                "--query-variant", variant, "--hw", hw, "--rate", str(rate),
                "--metric", metric, "--figure-size", "double"]
        argv.extend(extra)
        calls.append(argv)

    for group in GROUPS:
        for q, v in workloads_for(df, group):
            # B / C: hw x rate x metric x strategy
            for hw in HW_LIST:
                for rate in RATE_LIST:
                    for metric in METRICS:
                        for s in STRATEGIES:
                            if group == "all_5":
                                emit("config_perf_single", group, q, v, hw, rate, metric, ["--strategy", s])
                            emit("config_perf_compare", group, q, v, hw, rate, metric, ["--strategy", s])
            # 03B config_eventrate: hw x metric x strategy (rate fixed nominal, internal sweep)
            for hw in HW_LIST:
                for metric in METRICS:
                    for s in STRATEGIES:
                        emit("config_eventrate", group, q, v, hw, 500000, metric, ["--strategy", s])
            # F / E / 25 tp_latency: hw x rate, metric fixed at the selection objective
            for hw in HW_LIST:
                for rate in RATE_LIST:
                    emit("selected", group, q, v, hw, rate, SELECTION_METRIC)
                    emit("strategy_framework", group, q, v, hw, rate, SELECTION_METRIC)
                    emit("tp_latency", group, q, v, hw, rate, SELECTION_METRIC)
            # 18 selected_performance: hw x rate x metric
            for hw in HW_LIST:
                for rate in RATE_LIST:
                    for metric in METRICS:
                        emit("selected_performance", group, q, v, hw, rate, metric)
            # 22 strategy_hardware: rate x metric (hw fixed nominal, internal sweep)
            for rate in RATE_LIST:
                for metric in METRICS:
                    emit("strategy_hardware", group, q, v, "m510", rate, metric)
            # 23 strategy_rate: hw x metric (rate fixed nominal, internal sweep)
            for hw in HW_LIST:
                for metric in METRICS:
                    emit("strategy_rate", group, q, v, hw, 500000, metric)
            # G 19G operator_own_metrics: hw x rate x own-metric (metric arg unused by
            # this mode but still required by argparse; own-metric passed via extra)
            for hw in HW_LIST:
                for rate in RATE_LIST:
                    for om in OWN_METRICS:
                        emit("operator_own_metrics", group, q, v, hw, rate, SELECTION_METRIC, ["--own-metric", om])
        # 21 strategy_workload: once per group x hw x rate x metric (workload swept internally)
        for hw in HW_LIST:
            for rate in RATE_LIST:
                for metric in METRICS:
                    emit("strategy_workload", group, "", "", hw, rate, metric)

    return calls


def run_one(args):
    python_bin, argv = args
    cmd = [python_bin, SCRIPT] + argv
    try:
        p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, timeout=120)
        ok = p.returncode == 0
        tail = (p.stdout + p.stderr)[-4000:]
        return ok, argv, tail
    except Exception as e:  # noqa: BLE001
        return False, argv, str(e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Only print the planned call count.")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--log", default=os.path.join("/tmp", "full_enumeration_batch.log"))
    ap.add_argument("--fail-log", default=os.path.join("/tmp", "full_enumeration_batch_fail.log"))
    args = ap.parse_args()

    python_bin = os.environ.get("PYTHON", "python3")
    df = pd.read_csv(CSV_PATH, low_memory=False)
    calls = build_calls(df)
    print(f"planned calls: {len(calls)}")
    if args.dry_run:
        return

    ok_count = fail_count = 0
    t0 = time.time()
    with open(args.log, "w") as logf, open(args.fail_log, "w") as failf:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(run_one, (python_bin, argv)): argv for argv in calls}
            for i, fut in enumerate(as_completed(futures), 1):
                ok, argv, tail = fut.result()
                if ok:
                    ok_count += 1
                else:
                    fail_count += 1
                    failf.write(f"FAILED: {' '.join(argv)}\n{tail}\n\n")
                    failf.flush()
                logf.write(f"[{i}/{len(calls)}] ok={ok} {' '.join(argv)}\n")
                if i % 200 == 0:
                    elapsed = time.time() - t0
                    logf.write(f">>> progress {i}/{len(calls)} ok={ok_count} fail={fail_count} elapsed={elapsed:.0f}s\n")
                    logf.flush()
                    print(f"[{i}/{len(calls)}] ok={ok_count} fail={fail_count} elapsed={elapsed:.0f}s")
    elapsed = time.time() - t0
    summary = f"ALL DONE. total={len(calls)} ok={ok_count} fail={fail_count} elapsed={elapsed:.0f}s"
    print(summary)
    with open(args.log, "a") as logf:
        logf.write(summary + "\n")


if __name__ == "__main__":
    main()
