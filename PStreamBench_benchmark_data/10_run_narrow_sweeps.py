#!/usr/bin/env python3
"""Narrow driver for the baseline scaling/event-rate families (menu ids 8, 9, 12, 13, 14).

script 6's shell loops for these families try every (query x hardware x rate x framework x
metric) combination blindly, including the vast majority with no data at all -- that's what
made ids 8 and 12 run ~50,000 Python launches each per strategy. This script queries the CSV
first, for exactly the combinations that have the data a "scaling" or "event-rate" plot needs
(>=2 distinct x-axis values), and calls 3_plot_pdsp_query_metrics.py directly only for those.
3_plot_pdsp_query_metrics.py and 6_make_all_plots_risingwave.sh are not modified or imported
for execution -- this only reads the CSV with pandas and shells out to the plot script, the
same way 6_'s own run_* functions do.
"""
import argparse
import os
import subprocess
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PLOT_SCRIPT = os.path.join(HERE, "3_plot_pdsp_query_metrics.py")

# Mirrors PARALLELISM_WHITELIST in 3_plot_pdsp_query_metrics.py (not imported, since that
# module has top-level plotting/argparse side effects) -- the baseline silently drops any
# parallelism_avg not in this list before plotting, so pre-filtering here avoids queuing
# calls that are guaranteed to fail with "No data left after filtering".
PARALLELISM_WHITELIST = {
    "m510": [2, 8, 16, 32, 40],
    "c6525-25g": [2, 8, 16, 32, 40, 80],
    "c6525-100g": [2, 8, 16, 32, 40, 96],
}

DEFAULT_METRICS = ["throughput_50", "endtoend_latency", "cpu_50", "memory_50", "network_50"]
ALL_METRICS = DEFAULT_METRICS + [
    "throughput_95", "throughput_98", "event_rate_50", "event_rate_95", "event_rate_98",
    "cpu_95", "cpu_98", "memory_95", "memory_98", "network_95", "network_98",
]


def safe(s):
    return str(s).replace(" ", "_").replace("/", "_")


def load(csv_path):
    df = pd.read_csv(csv_path, low_memory=False)
    df["query_id"] = df["query"] + "_" + df["job_query_number"].astype(str)
    # Apply the baseline's own parallelism whitelist here too, so "real combo" counts below
    # match what the baseline will actually plot, not what's in the raw CSV.
    keep = pd.Series(True, index=df.index)
    for hw, allowed in PARALLELISM_WHITELIST.items():
        hw_mask = df["hardware_type"] == hw
        keep &= (~hw_mask) | df["parallelism_avg"].isin(allowed)
    dropped = (~keep).sum()
    if dropped:
        print(f"  (whitelist) dropped {dropped} rows with a parallelism_avg the baseline would reject")
    return df[keep]


def build_calls(df, mode, out_dir, metrics, frameworks_for_compare):
    """CLI flag names below were confirmed against 6_make_all_plots_risingwave.sh's own
    run_* functions (grepped, not guessed) -- eventrate modes take --parallelism_avg (not
    --parallelism), and every *_compare_fw / app_* mode takes --frameworks (a list)."""
    calls = []
    if mode == "scaling_single":
        g = df.groupby(["query_id", "framework", "hardware_type", "producer_rate"])
        for (qid, fw, hw, rate), sub in g:
            if sub["parallelism_avg"].nunique() < 2:
                continue
            for metric in metrics:
                save = os.path.join(out_dir, "scaling_single", safe(qid), fw, hw, f"{int(rate)}_{safe(metric)}")
                calls.append(["--plot", "scaling_single", "--framework", fw, "--query", qid,
                              "--hardware_type", hw, "--producer_rate", str(int(rate)),
                              "--metric", metric, "--agg", "mean", "--save", save])
    elif mode == "scaling_compare_fw":
        g = df.groupby(["query_id", "hardware_type", "producer_rate"])
        for (qid, hw, rate), sub in g:
            if sub["parallelism_avg"].nunique() < 2:
                continue
            for metric in metrics:
                save = os.path.join(out_dir, "scaling_compare_fw", safe(qid), hw, f"{int(rate)}_{safe(metric)}")
                calls.append(["--plot", "scaling_compare_fw", "--frameworks", *frameworks_for_compare,
                              "--query", qid, "--hardware_type", hw, "--producer_rate", str(int(rate)),
                              "--metric", metric, "--agg", "mean", "--save", save])
    elif mode == "eventrate_single":
        g = df.groupby(["query_id", "framework", "hardware_type", "parallelism_avg"])
        for (qid, fw, hw, par), sub in g:
            if sub["producer_rate"].nunique() < 2:
                continue
            for metric in metrics:
                save = os.path.join(out_dir, "eventrate_single", safe(qid), fw, hw,
                                    f"par{int(par)}_{safe(metric)}")
                calls.append(["--plot", "eventrate_single", "--framework", fw, "--query", qid,
                              "--hardware_type", hw, "--parallelism_avg", str(int(par)),
                              "--metric", metric, "--agg", "mean", "--save", save])
    elif mode == "eventrate_compare_fw":
        g = df.groupby(["query_id", "hardware_type", "parallelism_avg"])
        for (qid, hw, par), sub in g:
            if sub["producer_rate"].nunique() < 2:
                continue
            for metric in metrics:
                save = os.path.join(out_dir, "eventrate_compare_fw", safe(qid), hw,
                                    f"par{int(par)}_{safe(metric)}")
                calls.append(["--plot", "eventrate_compare_fw", "--frameworks", *frameworks_for_compare,
                              "--query", qid, "--hardware_type", hw, "--parallelism_avg", str(int(par)),
                              "--metric", metric, "--agg", "mean", "--save", save])
    elif mode == "app_scaling_all_hw":
        g = df.groupby(["query_id", "producer_rate"])
        for (qid, rate), sub in g:
            if sub["hardware_type"].nunique() < 2 and sub["parallelism_avg"].nunique() < 2:
                continue
            for metric in metrics:
                save = os.path.join(out_dir, "app_scaling_all_hw", safe(qid), f"{int(rate)}_{safe(metric)}")
                calls.append(["--plot", "app_scaling_all_hw", "--frameworks", *frameworks_for_compare,
                              "--query", qid, "--producer_rate", str(int(rate)), "--metric", metric,
                              "--agg", "mean", "--save", save])
    else:
        raise SystemExit(f"unknown mode: {mode}")
    return calls


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--csv", required=True)
    p.add_argument("--mode", required=True,
                   choices=["scaling_single", "scaling_compare_fw", "eventrate_single",
                           "eventrate_compare_fw", "app_scaling_all_hw"])
    p.add_argument("--out-dir", required=True)
    p.add_argument("--frameworks", nargs="*", default=None,
                   help="restrict to these frameworks (single-framework modes only)")
    p.add_argument("--metrics", nargs="*", default=DEFAULT_METRICS, choices=ALL_METRICS)
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--mpl-style", default="science")
    p.add_argument("--hw-palette", default="modern")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    df = load(args.csv)
    frameworks = args.frameworks or sorted(df["framework"].unique().tolist())
    df = df[df["framework"].isin(frameworks)]
    calls = build_calls(df, args.mode, args.out_dir, args.metrics, frameworks)
    print(f"{args.mode}: {len(calls)} calls planned (metrics={args.metrics})")
    if args.dry_run:
        return

    common = ["--csv", args.csv, "--agg", "mean", "--all-formats", "--no-show",
             "--mpl-style", args.mpl_style, "--hw-palette", args.hw_palette, "--show-values"]
    ok, failed = 0, 0
    for i, extra in enumerate(calls, 1):
        os.makedirs(os.path.dirname(extra[extra.index("--save") + 1]), exist_ok=True)
        cmd = [args.python, PLOT_SCRIPT] + common + extra
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0:
            ok += 1
        else:
            failed += 1
            print(f"  [{i}/{len(calls)}] FAILED: {' '.join(extra)}\n{r.stderr[-400:]}")
        if i % 200 == 0:
            print(f"  [{i}/{len(calls)}] ok={ok} failed={failed}")
    print(f"{args.mode}: done. ok={ok} failed={failed}")


if __name__ == "__main__":
    main()
