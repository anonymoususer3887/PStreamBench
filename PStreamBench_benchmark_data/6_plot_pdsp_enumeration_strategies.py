#!/usr/bin/env python3
"""
Compare performance across parallelism ENUMERATION STRATEGIES -- Uniform,
Random, DS2, and Dhalion (see PStreamBench_controller/dsp_be/
benchmark_sweep/parallelism_strategies.py for what each strategy computes
and how it's swept: run_strategy_sweep_risingwave.py / _spark.py).

Three comparisons:

  1. strategy_comparison        -- same streaming system ("platform") +
                                    same query + same hardware + same
                                    event rate, bars grouped by strategy.
  2. strategy_vs_eventrate      -- same streaming system + same query +
                                    same hardware, x=event rate, one
                                    series per strategy.
  3. strategy_across_frameworks -- same query + same hardware + same
                                    event rate, x=streaming system
                                    ("platform"), bars grouped by
                                    strategy.

Plus a `coverage` mode that prints which (framework, hardware, strategy)
combinations actually have data, since strategy coverage is uneven and
growing over time (see below) -- run this first to see what's actually
plottable before picking a query/rate/hardware for the other modes.

Strategy normalization (see normalize_strategy()): the CSV's raw
`enumeration_strategy` column only literally says "random"/"ds2"/
"dhalion" for the dedicated strategy-sweep runs. Every other value (NaN,
"Custom", "None", "Rulebased" -- the project's pre-existing full
parallelism sweep, which already varies parallelism uniformly across
every level for every framework/hardware) IS a uniform-parallelism run
in substance, just not labeled that in metaInfo's older schema -- folded
into "uniform" here rather than dropped, so Uniform already has full
framework/hardware coverage while Random/DS2/Dhalion are RisingWave-only
on m510 today (2026-08-26) and will gain coverage on other streaming
systems/hardware as those sweeps are run -- no changes needed here when
that happens, the new rows just show up once re-extracted into the CSV.
"""

import argparse
import os
import re
import warnings
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.colors import to_rgb, to_hex

METRICS = [
    "event_rate_50", "event_rate_95", "event_rate_98",
    "throughput_50", "throughput_95", "throughput_98",
    "endtoend_latency",
    "cpu_50", "cpu_95", "cpu_98",
    "memory_50", "memory_95", "memory_98",
    "network_50", "network_95", "network_98",
]

STRATEGY_ORDER = ["uniform", "random", "ds2", "dhalion"]
STRATEGY_LABELS = {"uniform": "Uniform", "random": "Random", "ds2": "DS2", "dhalion": "Dhalion"}
STRATEGY_COLORS = {
    "uniform": "#4C72B0",
    "random": "#DD8452",
    "ds2": "#55A868",
    "dhalion": "#C44E52",
}
STRATEGY_MARKER = {"uniform": "o", "random": "s", "ds2": "^", "dhalion": "D"}
STRATEGY_LINESTYLE = {"uniform": "-", "random": "--", "ds2": "-.", "dhalion": ":"}
DEFAULT_COLOR = "#888888"

FRAMEWORK_ORDER = ["flink", "storm", "spark", "kafkastream", "risingwave"]
FRAMEWORK_HATCH = {"flink": "", "storm": "xx", "spark": "..", "kafkastream": "//", "risingwave": "\\\\"}

HARDWARE_ORDER = ["m510", "c6525-25g", "c6525-100g"]

MAX_REL_ERR = 0.15


# -----------------------------
# Data loading / normalization
# -----------------------------
def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "framework" in df.columns:
        df["framework"] = df["framework"].astype(str).str.lower()
    df = add_query_id_columns(df)
    df = normalize_strategy(df)
    return df


_QID_SUFFIX_RE = re.compile(r"^(?P<base>.*)_(?P<num>\d+)$")


def add_query_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "query" not in df.columns:
        df["query"] = ""
    df["query_base"] = df["query"].astype(str)
    if "job_query_number" in df.columns:
        df["job_query_number"] = pd.to_numeric(df["job_query_number"], errors="coerce").fillna(1).astype(int)
        df["query_id"] = df["query_base"] + "_" + df["job_query_number"].astype(str)
    else:
        df["query_id"] = df["query_base"]
    return df


def normalize_strategy(df: pd.DataFrame) -> pd.DataFrame:
    if "enumeration_strategy" not in df.columns:
        raise ValueError("CSV has no 'enumeration_strategy' column -- re-run 1_data_extractor.py.")
    df = df.copy()
    raw = df["enumeration_strategy"].astype(str).str.strip().str.lower()
    known = {"random", "ds2", "dhalion"}
    df["strategy"] = raw.where(raw.isin(known), "uniform")
    return df


def slugify(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s)).strip("_")


def order_hardware(hws: List[str]) -> List[str]:
    present = [h for h in HARDWARE_ORDER if h in hws]
    others = sorted([h for h in hws if h not in HARDWARE_ORDER])
    return present + others


def order_frameworks(fws: List[str]) -> List[str]:
    present = [f for f in FRAMEWORK_ORDER if f in fws]
    others = sorted([f for f in fws if f not in FRAMEWORK_ORDER])
    return present + others


def order_strategies(strats: Sequence[str]) -> List[str]:
    present = [s for s in STRATEGY_ORDER if s in strats]
    others = sorted([s for s in strats if s not in STRATEGY_ORDER])
    return present + others


# -----------------------------
# Style / plot mechanics (mirrors 3_plot_pdsp_query_metrics.py's conventions)
# -----------------------------
def configure_style(fig_w: float = 6.5, fig_h: float = 3.5):
    plt.rcParams.update({
        "figure.figsize": (fig_w, fig_h),
        "font.size": 11,
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "legend.fontsize": 9,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "hatch.linewidth": 0.8,
    })


def darken_color(color: str, factor: float = 0.65) -> str:
    r, g, b = to_rgb(color)
    return to_hex((r * factor, g * factor, b * factor))


def compute_capped_errors(values: np.ndarray, std: np.ndarray, max_rel: float = MAX_REL_ERR) -> np.ndarray:
    vals = np.asarray(values, dtype=float)
    stds = np.where(np.isnan(std), 0.0, std)
    caps = np.abs(vals) * max_rel
    errs = np.minimum(stds, caps)
    return np.where(np.isnan(errs), 0.0, errs)


def _agg_stats(grouped, agg: str):
    if agg not in ("mean", "median", "max", "min"):
        raise ValueError(f"Unsupported agg='{agg}'")
    stats = grouped.agg([agg, "std"]).reset_index()
    return stats, agg


def apply_y_scale(ax, y_scale: str, plotted_values):
    if y_scale == "linear":
        ax.set_yscale("linear")
        return
    vals = np.asarray(plotted_values, dtype=float)
    vals = vals[np.isfinite(vals) & (vals > 0)]
    if len(vals) == 0:
        warnings.warn("Log scale requested but no positive plotted values exist; using linear.")
        ax.set_yscale("linear")
        return
    ax.set_yscale("log")
    cur_min, cur_max = ax.get_ylim()
    ax.set_ylim(float(np.min(vals)) / 10.0, cur_max)


def apply_axis_limits(ax, ymin=None, ymax=None):
    if ymin is None and ymax is None:
        return
    cur_min, cur_max = ax.get_ylim()
    new_min = cur_min if ymin is None else float(ymin)
    new_max = cur_max if ymax is None else float(ymax)
    if new_max <= new_min:
        warnings.warn(f"Invalid y-limits (ymin={new_min}, ymax={new_max}); ignoring.")
        return
    ax.set_ylim(new_min, new_max)


def add_value_labels(ax, spacing: int = 3, fontsize: int = 8, max_bars: int = 60):
    from matplotlib.container import BarContainer
    bars = [p for c in getattr(ax, "containers", []) if isinstance(c, BarContainer) for p in c.patches]
    if len(bars) > max_bars:
        return
    for bar in bars:
        height = bar.get_height()
        if height is None or not np.isfinite(height) or height == 0:
            continue
        ax.annotate(f"{height:.1f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, spacing), textcoords="offset points",
                    ha="center", va="bottom", fontsize=fontsize, rotation=90)


def _ensure_dir_for_file(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _save_figure_multi(fig, save_path: str, all_formats: bool):
    if not save_path:
        return
    _ensure_dir_for_file(save_path)
    root, _ext = os.path.splitext(save_path)
    root = root or save_path
    fig.savefig(root + ".png", dpi=300, bbox_inches="tight")
    if all_formats:
        fig.savefig(root + ".pdf", bbox_inches="tight")
        fig.savefig(root + ".svg", bbox_inches="tight")


def _add_headroom(ax):
    """Extra top margin so value labels on tall bars don't collide with the
    title -- add_value_labels() places text directly above each bar top."""
    cur_min, cur_max = ax.get_ylim()
    if ax.get_yscale() == "log":
        ax.set_ylim(cur_min, cur_max * (10 ** 0.35))
    else:
        span = cur_max - cur_min
        ax.set_ylim(cur_min, cur_max + span * 0.12)


def _finish(fig, ax, title, save_path, show, all_formats, rect=(0, 0, 1, 1), add_headroom=False):
    if add_headroom:
        _add_headroom(ax)
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    plt.tight_layout(rect=list(rect))
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# -----------------------------
# Coverage report
# -----------------------------
def print_coverage(df: pd.DataFrame):
    """Prints which (strategy, framework, hardware) combinations have any
    data at all, and how many (query, event_rate) combos each covers --
    run this first, since Random/DS2/Dhalion coverage is partial and
    growing (see module docstring)."""
    cov = (
        df.groupby(["strategy", "framework", "hardware_type"])
        .apply(lambda g: pd.Series({
            "rows": len(g),
            "queries": g["query_id"].nunique(),
            "event_rates": sorted(g["producer_rate"].dropna().unique().tolist()),
        }))
        .reset_index()
    )
    cov = cov.sort_values(["strategy", "framework", "hardware_type"])
    with pd.option_context("display.max_rows", None, "display.width", 160):
        print(cov.to_string(index=False))


# -----------------------------
# Plot 1: strategy_comparison (x=strategy, hatch=framework, color=strategy)
# -----------------------------
def plot_strategy_comparison(
    df: pd.DataFrame,
    query: str,
    hardware_type: str,
    producer_rate: int,
    frameworks: List[str],
    metric: str,
    agg: str = "mean",
    strategies: Optional[List[str]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    sub = df[
        (df["query_id"] == query)
        & (df["hardware_type"] == hardware_type)
        & (df["producer_rate"] == producer_rate)
        & (df["framework"].isin(frameworks))
    ]
    if sub.empty:
        raise ValueError(f"No data for query={query} hw={hardware_type} rate={producer_rate} frameworks={frameworks}")

    strategies = strategies or order_strategies(sub["strategy"].unique().tolist())
    if not strategies:
        raise ValueError("No strategies found in filtered data.")

    stats, value_col = _agg_stats(sub.groupby(["framework", "strategy"])[metric], agg)
    pivot_val = stats.pivot(index="strategy", columns="framework", values=value_col).reindex(strategies)
    pivot_std = stats.pivot(index="strategy", columns="framework", values="std").reindex(strategies)

    fws_present = order_frameworks([f for f in frameworks if f in pivot_val.columns])
    if not fws_present:
        raise ValueError("No frameworks found after aggregation.")

    x = np.arange(len(strategies))
    fig, ax = plt.subplots(figsize=(max(6.5, 1.3 * len(strategies) + 2), 3.8))
    n = len(fws_present)
    bw = 0.8 / max(1, n)
    plotted = []
    for i, fw in enumerate(fws_present):
        vals = pivot_val[fw].to_numpy(dtype=float)
        stds = pivot_std[fw].to_numpy(dtype=float)
        errs = compute_capped_errors(vals, stds)
        plotted.append(vals)
        colors = [STRATEGY_COLORS.get(s, DEFAULT_COLOR) for s in strategies]
        x_pos = x + (i - (n - 1) / 2) * bw
        ax.bar(x_pos, np.nan_to_num(vals, nan=0.0), width=bw, yerr=errs, capsize=3,
               color=colors, edgecolor="black", hatch=FRAMEWORK_HATCH.get(fw, ""), alpha=0.9)

    ax.set_xticks(x)
    ax.set_xticklabels([STRATEGY_LABELS.get(s, s) for s in strategies])
    ax.set_xlabel("enumeration strategy")
    ax.set_ylabel(metric)

    strat_handles = [Patch(facecolor=STRATEGY_COLORS.get(s, DEFAULT_COLOR), edgecolor="black",
                            label=STRATEGY_LABELS.get(s, s)) for s in strategies]
    leg1 = ax.legend(handles=strat_handles, title="Strategy (color)", loc="upper left",
                      bbox_to_anchor=(1.02, 1.0), frameon=True)
    ax.add_artist(leg1)
    if n > 1:
        fw_handles = [Patch(facecolor="white", edgecolor="black", hatch=FRAMEWORK_HATCH.get(fw, ""), label=fw)
                      for fw in fws_present]
        ax.legend(handles=fw_handles, title="Platform (hatch)", loc="upper left",
                  bbox_to_anchor=(1.02, 0.55), frameon=True)

    if show_values:
        add_value_labels(ax)

    plotted_all = np.concatenate(plotted) if plotted else np.array([])
    apply_y_scale(ax, y_scale, plotted_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    title = title or f"{query} | {hardware_type} | rate={int(producer_rate)} | {metric} ({agg})"
    _finish(fig, ax, title, save_path, show, all_formats, rect=(0, 0, 0.78, 1),
            add_headroom=show_values and ymax is None)


def batch_strategy_comparison(
    df: pd.DataFrame, frameworks: List[str], hardware_types: List[str], out_dir: str,
    agg: str, show: bool, all_formats: bool, show_values: bool, metrics: Optional[List[str]] = None,
):
    metrics = metrics or METRICS
    queries = sorted(df["query_id"].dropna().unique().tolist())
    for q in queries:
        df_q = df[df["query_id"] == q]
        for hw in hardware_types:
            df_hw = df_q[df_q["hardware_type"] == hw]
            rates = sorted(df_hw["producer_rate"].dropna().unique().tolist())
            for metric in metrics:
                if metric not in df.columns:
                    continue
                for r in rates:
                    save_base = os.path.join(out_dir, "strategy_comparison", slugify(q), hw, metric, f"rate{int(r)}")
                    print(f"[INFO] {q} | {hw} | {metric} | rate={int(r)} -> {save_base}")
                    try:
                        plot_strategy_comparison(
                            df=df, query=q, hardware_type=hw, producer_rate=int(r),
                            frameworks=frameworks, metric=metric, agg=agg,
                            save_path=save_base, show=show, all_formats=all_formats, show_values=show_values,
                        )
                    except Exception as e:
                        print(f"[WARN] Skipping {q} | {hw} | {metric} | rate={int(r)}: {e}")


# -----------------------------
# Shared: bars grouped by an arbitrary category, hued/colored by strategy
# (used by strategy_vs_eventrate's bar mode AND strategy_across_frameworks)
# -----------------------------
def _plot_strategy_hued_bars(
    sub: pd.DataFrame, x_col: str, x_order: List[str], x_labels: List[str], xlabel: str,
    metric: str, agg: str, strategies: Optional[List[str]],
    title: str, save_path: Optional[str], show: bool, all_formats: bool,
    y_scale: str, show_values: bool, ymin: Optional[float], ymax: Optional[float],
):
    if sub.empty:
        raise ValueError("No data to plot (empty selection).")
    if metric not in sub.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    stats, value_col = _agg_stats(sub.groupby([x_col, "strategy"])[metric], agg)
    pivot_val = stats.pivot(index=x_col, columns="strategy", values=value_col).reindex(x_order)
    pivot_std = stats.pivot(index=x_col, columns="strategy", values="std").reindex(x_order)

    strategies = strategies or order_strategies([s for s in pivot_val.columns if s in pivot_val.columns])
    strategies = [s for s in strategies if s in pivot_val.columns]
    if not strategies:
        raise ValueError("No strategies found in filtered data.")

    x = np.arange(len(x_order))
    fig, ax = plt.subplots(figsize=(max(6.5, 1.3 * len(x_order) + 2), 3.8))
    n = len(strategies)
    bw = 0.8 / max(1, n)
    plotted = []
    for i, s in enumerate(strategies):
        vals = pivot_val[s].to_numpy(dtype=float)
        stds = pivot_std[s].to_numpy(dtype=float)
        errs = compute_capped_errors(vals, stds)
        plotted.append(vals)
        x_pos = x + (i - (n - 1) / 2) * bw
        ax.bar(x_pos, np.nan_to_num(vals, nan=0.0), width=bw, yerr=errs, capsize=3,
               color=STRATEGY_COLORS.get(s, DEFAULT_COLOR), edgecolor="black", alpha=0.9,
               label=STRATEGY_LABELS.get(s, s))

    ax.set_xticks(x)
    if any(len(str(l)) > 6 for l in x_labels):
        ax.set_xticklabels(x_labels, rotation=15, ha="right")
    else:
        ax.set_xticklabels(x_labels)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(metric)
    ax.legend(title="Strategy", loc="upper left", bbox_to_anchor=(1.02, 1.0), frameon=True)

    if show_values:
        add_value_labels(ax)

    plotted_all = np.concatenate(plotted) if plotted else np.array([])
    apply_y_scale(ax, y_scale, plotted_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    _finish(fig, ax, title, save_path, show, all_formats, rect=(0, 0, 0.82, 1),
            add_headroom=show_values and ymax is None)


# -----------------------------
# Plot 2: strategy_vs_eventrate (x=producer_rate, series=strategy)
# -----------------------------
def plot_strategy_vs_eventrate(
    df: pd.DataFrame,
    query: str,
    hardware_type: str,
    framework: str,
    metric: str,
    agg: str = "mean",
    chart: str = "line",
    strategies: Optional[List[str]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    sub = df[
        (df["query_id"] == query)
        & (df["hardware_type"] == hardware_type)
        & (df["framework"] == framework.lower())
    ]
    if sub.empty:
        raise ValueError(f"No data for query={query} hw={hardware_type} framework={framework}")

    rates = sorted(sub["producer_rate"].dropna().unique().tolist())
    if not rates:
        raise ValueError("No event rates found in filtered data.")

    title = title or f"{query} | {hardware_type} | {framework} | {metric} vs event rate ({agg})"

    if chart == "bar":
        _plot_strategy_hued_bars(
            sub=sub, x_col="producer_rate", x_order=rates, x_labels=[str(int(r)) for r in rates],
            xlabel="producer_rate (events/sec)", metric=metric, agg=agg, strategies=strategies,
            title=title, save_path=save_path, show=show, all_formats=all_formats,
            y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax,
        )
        return

    stats, value_col = _agg_stats(sub.groupby(["strategy", "producer_rate"])[metric], agg)
    pivot_val = stats.pivot(index="producer_rate", columns="strategy", values=value_col).reindex(rates)
    pivot_std = stats.pivot(index="producer_rate", columns="strategy", values="std").reindex(rates)

    strategies = strategies or order_strategies([s for s in pivot_val.columns])
    strategies = [s for s in strategies if s in pivot_val.columns]
    if not strategies:
        raise ValueError("No strategies found in filtered data.")

    x = np.arange(len(rates))
    fig, ax = plt.subplots()
    plotted = []
    for s in strategies:
        vals = pivot_val[s].to_numpy(dtype=float)
        stds = pivot_std[s].to_numpy(dtype=float)
        errs = compute_capped_errors(vals, stds)
        plotted.append(vals)
        ax.errorbar(
            x, vals, yerr=errs, marker=STRATEGY_MARKER.get(s, "o"), linestyle=STRATEGY_LINESTYLE.get(s, "-"),
            color=STRATEGY_COLORS.get(s, DEFAULT_COLOR), label=STRATEGY_LABELS.get(s, s),
            capsize=3, linewidth=2.0, markersize=6,
        )

    ax.set_xticks(x)
    ax.set_xticklabels([str(int(r)) for r in rates])
    ax.set_xlabel("producer_rate (events/sec)")
    ax.set_ylabel(metric)
    ax.legend(title="Strategy", loc="upper left", bbox_to_anchor=(1.02, 1.0), frameon=True)

    if show_values:
        add_value_labels(ax)

    plotted_all = np.concatenate(plotted) if plotted else np.array([])
    apply_y_scale(ax, y_scale, plotted_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    _finish(fig, ax, title, save_path, show, all_formats, rect=(0, 0, 0.82, 1))


def batch_strategy_vs_eventrate(
    df: pd.DataFrame, frameworks: List[str], hardware_types: List[str], out_dir: str,
    agg: str, chart: str, show: bool, all_formats: bool, show_values: bool, metrics: Optional[List[str]] = None,
):
    metrics = metrics or METRICS
    queries = sorted(df["query_id"].dropna().unique().tolist())
    for q in queries:
        df_q = df[df["query_id"] == q]
        for fw in frameworks:
            df_fw = df_q[df_q["framework"] == fw]
            for hw in hardware_types:
                df_hw = df_fw[df_fw["hardware_type"] == hw]
                if df_hw.empty:
                    continue
                for metric in metrics:
                    if metric not in df.columns:
                        continue
                    save_base = os.path.join(out_dir, "strategy_vs_eventrate", slugify(q), fw, hw, metric)
                    print(f"[INFO] {q} | {fw} | {hw} | {metric} -> {save_base}")
                    try:
                        plot_strategy_vs_eventrate(
                            df=df, query=q, hardware_type=hw, framework=fw, metric=metric,
                            agg=agg, chart=chart, save_path=save_base, show=show,
                            all_formats=all_formats, show_values=show_values,
                        )
                    except Exception as e:
                        print(f"[WARN] Skipping {q} | {fw} | {hw} | {metric}: {e}")


# -----------------------------
# Plot 3: strategy_across_frameworks (x=framework/"platform", series=strategy)
# -----------------------------
def plot_strategy_across_frameworks(
    df: pd.DataFrame,
    query: str,
    hardware_type: str,
    producer_rate: int,
    frameworks: List[str],
    metric: str,
    agg: str = "mean",
    strategies: Optional[List[str]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    sub = df[
        (df["query_id"] == query)
        & (df["hardware_type"] == hardware_type)
        & (df["producer_rate"] == producer_rate)
        & (df["framework"].isin(frameworks))
    ]
    fws_present = order_frameworks([f for f in frameworks if f in sub["framework"].unique()])
    title = title or f"{query} | {hardware_type} | rate={int(producer_rate)} | {metric} across platforms ({agg})"

    _plot_strategy_hued_bars(
        sub=sub, x_col="framework", x_order=fws_present, x_labels=fws_present,
        xlabel="streaming system (platform)", metric=metric, agg=agg, strategies=strategies,
        title=title, save_path=save_path, show=show, all_formats=all_formats,
        y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax,
    )


def batch_strategy_across_frameworks(
    df: pd.DataFrame, frameworks: List[str], hardware_types: List[str], out_dir: str,
    agg: str, show: bool, all_formats: bool, show_values: bool, metrics: Optional[List[str]] = None,
):
    metrics = metrics or METRICS
    queries = sorted(df["query_id"].dropna().unique().tolist())
    for q in queries:
        df_q = df[df["query_id"] == q]
        for hw in hardware_types:
            df_hw = df_q[df_q["hardware_type"] == hw]
            rates = sorted(df_hw["producer_rate"].dropna().unique().tolist())
            for metric in metrics:
                if metric not in df.columns:
                    continue
                for r in rates:
                    save_base = os.path.join(out_dir, "strategy_across_frameworks", slugify(q), hw, metric, f"rate{int(r)}")
                    print(f"[INFO] {q} | {hw} | {metric} | rate={int(r)} -> {save_base}")
                    try:
                        plot_strategy_across_frameworks(
                            df=df, query=q, hardware_type=hw, producer_rate=int(r),
                            frameworks=frameworks, metric=metric, agg=agg,
                            save_path=save_base, show=show, all_formats=all_formats, show_values=show_values,
                        )
                    except Exception as e:
                        print(f"[WARN] Skipping {q} | {hw} | {metric} | rate={int(r)}: {e}")


# -----------------------------
# CLI
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Compare performance across parallelism enumeration strategies.")
    p.add_argument("--csv", type=str, default="query_performance_data.csv")
    p.add_argument("--plot", type=str, required=True, choices=[
        "coverage",
        "strategy_comparison", "batch_strategy_comparison",
        "strategy_vs_eventrate", "batch_strategy_vs_eventrate",
        "strategy_across_frameworks", "batch_strategy_across_frameworks",
    ])
    p.add_argument("--frameworks", nargs="*", default=["risingwave"],
                    help="Streaming systems ('platforms') to include, e.g. --frameworks risingwave flink spark")
    p.add_argument("--hardware-types", nargs="*", default=None,
                    help="Hardware types to include. If omitted, uses all found in CSV (single-plot modes need exactly one via --hardware-type).")
    p.add_argument("--hardware-type", type=str, default="m510",
                    help="Single hardware type, used by non-batch plot modes.")
    p.add_argument("--query", type=str, default=None, help="query_id, e.g. 'Word Count' or 'Ad Analytics_1'.")
    p.add_argument("--metric", type=str, default=None, choices=METRICS)
    p.add_argument("--metrics", nargs="*", default=None, help="Override metrics list in batch mode.")
    p.add_argument("--producer_rate", type=int, default=None)
    p.add_argument("--framework", type=str, default=None, help="Single framework, used by strategy_vs_eventrate.")
    p.add_argument("--strategies", nargs="*", default=None, choices=STRATEGY_ORDER,
                    help="Restrict to specific strategies; default is whatever's present.")
    p.add_argument("--agg", type=str, default="mean", choices=["mean", "median", "max", "min"])
    p.add_argument("--chart", type=str, default="line", choices=["line", "bar"],
                    help="Only used by strategy_vs_eventrate.")
    p.add_argument("--save", type=str, default=None)
    p.add_argument("--out_dir", type=str, default="plots")
    p.add_argument("--no-show", action="store_true")
    p.add_argument("--all-formats", action="store_true")
    p.add_argument("--no-values", action="store_true")
    p.add_argument("--y-scale", type=str, default="linear", choices=["linear", "log"])
    p.add_argument("--ymin", type=float, default=None)
    p.add_argument("--ymax", type=float, default=None)
    return p.parse_args()


def main():
    configure_style()
    args = parse_args()

    df = load_data(args.csv)

    frameworks = [f.lower() for f in (args.frameworks or [])]
    show = not args.no_show
    show_values = not args.no_values

    if args.hardware_types and len(args.hardware_types) > 0:
        hardware_types = order_hardware(list(args.hardware_types))
    else:
        hardware_types = order_hardware(df["hardware_type"].dropna().unique().tolist())

    if args.plot == "coverage":
        print_coverage(df)
        return

    if args.plot == "strategy_comparison":
        if args.query is None or args.metric is None or args.producer_rate is None:
            raise SystemExit("strategy_comparison requires --query, --metric, --producer_rate")
        plot_strategy_comparison(
            df=df, query=args.query, hardware_type=args.hardware_type, producer_rate=args.producer_rate,
            frameworks=frameworks, metric=args.metric, agg=args.agg, strategies=args.strategies,
            save_path=args.save, show=show, all_formats=args.all_formats, show_values=show_values,
            y_scale=args.y_scale, ymin=args.ymin, ymax=args.ymax,
        )

    elif args.plot == "batch_strategy_comparison":
        batch_strategy_comparison(
            df=df, frameworks=frameworks, hardware_types=hardware_types, out_dir=args.out_dir,
            agg=args.agg, show=show, all_formats=args.all_formats, show_values=show_values,
            metrics=args.metrics,
        )

    elif args.plot == "strategy_vs_eventrate":
        framework = args.framework or (frameworks[0] if frameworks else None)
        if args.query is None or args.metric is None or framework is None:
            raise SystemExit("strategy_vs_eventrate requires --query, --metric, and --framework (or --frameworks)")
        plot_strategy_vs_eventrate(
            df=df, query=args.query, hardware_type=args.hardware_type, framework=framework, metric=args.metric,
            agg=args.agg, chart=args.chart, strategies=args.strategies,
            save_path=args.save, show=show, all_formats=args.all_formats, show_values=show_values,
            y_scale=args.y_scale, ymin=args.ymin, ymax=args.ymax,
        )

    elif args.plot == "batch_strategy_vs_eventrate":
        batch_strategy_vs_eventrate(
            df=df, frameworks=frameworks, hardware_types=hardware_types, out_dir=args.out_dir,
            agg=args.agg, chart=args.chart, show=show, all_formats=args.all_formats, show_values=show_values,
            metrics=args.metrics,
        )

    elif args.plot == "strategy_across_frameworks":
        if args.query is None or args.metric is None or args.producer_rate is None:
            raise SystemExit("strategy_across_frameworks requires --query, --metric, --producer_rate")
        plot_strategy_across_frameworks(
            df=df, query=args.query, hardware_type=args.hardware_type, producer_rate=args.producer_rate,
            frameworks=frameworks, metric=args.metric, agg=args.agg, strategies=args.strategies,
            save_path=args.save, show=show, all_formats=args.all_formats, show_values=show_values,
            y_scale=args.y_scale, ymin=args.ymin, ymax=args.ymax,
        )

    elif args.plot == "batch_strategy_across_frameworks":
        batch_strategy_across_frameworks(
            df=df, frameworks=frameworks, hardware_types=hardware_types, out_dir=args.out_dir,
            agg=args.agg, show=show, all_formats=args.all_formats, show_values=show_values,
            metrics=args.metrics,
        )


if __name__ == "__main__":
    main()
