#!/usr/bin/env python3

import argparse
import json
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter


METRICS = [
    "event_rate_50", "event_rate_95", "event_rate_98",
    "throughput_50", "throughput_95", "throughput_98",
    "endtoend_latency",
    "cpu_50", "cpu_95", "cpu_98",
    "memory_50", "memory_95", "memory_98",
    "network_50", "network_95", "network_98",
]

# Optional alpha per framework
FRAMEWORK_ALPHA = {"flink": 1.0, "storm": 0.90}

# Framework hatch patterns (distinct!)
FRAMEWORK_HATCH = {
    "flink": "",     # solid
    "storm": "xx",   # cross-hatch
}

# Hardware colors
HARDWARE_COLORS = {
    "m510": "#1b9e77",
    "c6525-25g": "#d95f02",
    "c6525-100g": "#7570b3",
}
DEFAULT_HW_COLOR = "#666666"

# Canonical hardware order (when present)
HARDWARE_ORDER = ["m510", "c6525-25g", "c6525-100g"]


def configure_style():
    plt.rcParams.update({
        "figure.figsize": (4, 2),
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.labelsize": 12,
        "legend.fontsize": 9,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.autolayout": True,
    })


def human_k_formatter(x, pos=None):
    """Format ticks as 1k, 1.5k, 2M, etc."""
    x = float(x)
    ax_abs = abs(x)

    if ax_abs >= 1_000_000_000:
        v = x / 1_000_000_000
        s = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{s}B"
    if ax_abs >= 1_000_000:
        v = x / 1_000_000
        s = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{s}M"
    if ax_abs >= 1_000:
        v = x / 1_000
        s = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{s}k"

    # keep small numbers clean
    if ax_abs >= 10:
        return f"{x:.0f}"
    if ax_abs >= 1:
        return f"{x:.1f}".rstrip("0").rstrip(".")
    return f"{x:.2f}".rstrip("0").rstrip(".")


def add_value_labels(ax, spacing: int = 3, fontsize: int = 8, rotation: int = 90, max_bars: int = 120):
    total_bars = sum(len(c) for c in ax.containers)
    if total_bars > max_bars:
        return
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            if height is None or np.isnan(height) or height == 0:
                continue
            ax.annotate(
                f"{height:.1f}",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, spacing),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=fontsize,
                rotation=rotation,
            )


def _ensure_dir_for_file(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _save_figure_multi(fig, save_path: str, all_formats: bool):
    if not save_path:
        return
    _ensure_dir_for_file(save_path)
    root, _ext = os.path.splitext(save_path)
    if not root:
        root = save_path
    fig.savefig(root + ".png", dpi=300)
    if all_formats:
        fig.savefig(root + ".pdf")
        fig.savefig(root + ".svg")


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = {"framework", "query", "hardware_type", "producer_rate", "parallelism_avg"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns in CSV: {sorted(missing)}")
    df["framework"] = df["framework"].astype(str).str.lower()
    return df


def parse_par_map_json(s: Optional[str]) -> Dict[str, Tuple[int, int]]:
    """
    Accepts either:
      {"m510":[2,40], "c6525-25g":[2,80]}
    or
      {"m510":{"low":2,"high":40}, ...}
    Returns:
      { hw: (low, high) }
    """
    if not s:
        return {}

    try:
        obj = json.loads(s)
    except Exception as e:
        raise SystemExit(f"Failed to parse --par-map-json: {e}")

    out: Dict[str, Tuple[int, int]] = {}
    for hw, v in obj.items():
        if isinstance(v, (list, tuple)):
            if len(v) != 2:
                raise SystemExit(f"--par-map-json: '{hw}' must be [low, high]. Got: {v}")
            out[hw] = (int(v[0]), int(v[1]))
        elif isinstance(v, dict):
            if "low" not in v or "high" not in v:
                raise SystemExit(f"--par-map-json: '{hw}' dict must have low/high. Got: {v}")
            out[hw] = (int(v["low"]), int(v["high"]))
        else:
            raise SystemExit(f"--par-map-json: '{hw}' must map to [low,high] or {{low,high}}. Got: {v}")
    return out


def order_hardware(hws: List[str]) -> List[str]:
    """Force the canonical order when possible. Any unknown hardware goes after, sorted."""
    present = [h for h in HARDWARE_ORDER if h in hws]
    others = sorted([h for h in hws if h not in HARDWARE_ORDER])
    return present + others


def plot_low_high_across_hardware(
    df: pd.DataFrame,
    query: str,
    metric: str,
    producer_rate: int,
    frameworks: List[str],
    hardware_types: List[str],
    par_map: Dict[str, Tuple[int, int]],
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    show_values: bool = True,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    frameworks = [f.lower() for f in frameworks]
    hardware_types = order_hardware(hardware_types)

    # Validate par_map coverage
    for hw in hardware_types:
        if hw not in par_map:
            raise ValueError(f"Hardware '{hw}' missing from --par-map-json")

    df_sel = df[
        (df["query"] == query) &
        (df["producer_rate"] == producer_rate) &
        (df["framework"].isin(frameworks)) &
        (df["hardware_type"].isin(hardware_types))
    ].copy()

    if df_sel.empty:
        raise ValueError(f"No data for query={query}, rate={producer_rate}, hw={hardware_types}, fw={frameworks}")

    # Keep only per-hardware low/high points
    pieces = []
    for hw in hardware_types:
        low, high = par_map[hw]
        df_hw = df_sel[(df_sel["hardware_type"] == hw) & (df_sel["parallelism_avg"].isin([low, high]))]
        if not df_hw.empty:
            pieces.append(df_hw)

    if not pieces:
        raise ValueError("No rows matched per-hardware low/high parallelism selections.")

    df_pts = pd.concat(pieces, ignore_index=True)

    # Aggregate
    group_cols = ["hardware_type", "framework", "parallelism_avg"]
    g = df_pts.groupby(group_cols)[metric]
    if agg == "mean":
        stats = g.mean().reset_index().rename(columns={metric: "val"})
    elif agg == "median":
        stats = g.median().reset_index().rename(columns={metric: "val"})
    elif agg == "max":
        stats = g.max().reset_index().rename(columns={metric: "val"})
    elif agg == "min":
        stats = g.min().reset_index().rename(columns={metric: "val"})
    else:
        raise ValueError(f"Unsupported agg='{agg}'")

    # X axis: two groups -> LOW, HIGH
    x_groups = ["LOW", "HIGH"]
    x = np.arange(len(x_groups))

    # Inside each group: Flink(all HW in order) then Storm(all HW in order)
    series = []
    for fw in frameworks:
        for hw in hardware_types:
            series.append((fw, hw))  # order matters!

    bars_per_group = len(series)
    bar_width = 0.8 / max(1, bars_per_group)

    fig, ax = plt.subplots(figsize=(3, 2.5))

    # Apply human-readable y-axis tick formatting (k/M/B)
    ax.yaxis.set_major_formatter(FuncFormatter(human_k_formatter))

    for s_idx, (fw, hw) in enumerate(series):
        offset = (s_idx - (bars_per_group - 1) / 2) * bar_width
        low_par, high_par = par_map[hw]

        def _get_val(par: int) -> float:
            row = stats[
                (stats["hardware_type"] == hw) &
                (stats["framework"] == fw) &
                (stats["parallelism_avg"] == par)
            ]
            return float(row["val"].iloc[0]) if not row.empty else np.nan

        vals = np.array([_get_val(low_par), _get_val(high_par)], dtype=float)

        color = HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR)
        alpha = FRAMEWORK_ALPHA.get(fw, 0.9)
        hatch = FRAMEWORK_HATCH.get(fw, "")

        bars = ax.bar(
            x + offset,
            np.nan_to_num(vals, nan=0.0),
            width=bar_width,
            color=color,
            alpha=alpha,
            edgecolor="black",
            hatch=hatch,
        )

        # Visually indicate missing data
        for b, v in zip(bars, vals):
            if np.isnan(v):
                b.set_alpha(0.15)

    ax.set_xticks(x)
    ax.set_xticklabels(["LOW", "HIGH"])

    ax.set_ylabel(metric)

    if title is None:
        #title = f"{query} | rate={producer_rate}"
        title = ""
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Two legends: hardware (color) and framework (hatch)
    hw_handles = [
        Patch(facecolor=HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR), edgecolor="black", label=hw, hatch="")
        for hw in hardware_types
    ]
    fw_handles = [
        Patch(facecolor="white", edgecolor="black", label=fw, hatch=FRAMEWORK_HATCH.get(fw, ""))
        for fw in frameworks
    ]

    #leg1 = ax.legend(handles=hw_handles, title="Hardware (color)", loc="upper left", ncol=1, frameon=True)
    #ax.add_artist(leg1)
    #ax.legend(handles=fw_handles, title="Framework (hatch)", loc="upper right", ncol=1, frameon=True)

    if show_values:
        add_value_labels(ax, spacing=3, fontsize=7, rotation=90, max_bars=200)

    plt.tight_layout()
    _save_figure_multi(fig, save_path, all_formats)

    if show:
        plt.show()
    else:
        plt.close(fig)


def batch_low_high_across_hw(
    df: pd.DataFrame,
    frameworks: List[str],
    hardware_types: List[str],
    par_map: Dict[str, Tuple[int, int]],
    out_dir: str,
    agg: str,
    show: bool,
    all_formats: bool,
    show_values: bool,
    metrics: Optional[List[str]] = None,
):
    metrics = metrics or METRICS
    queries = sorted(df["query"].dropna().unique().tolist())

    for q in queries:
        df_q = df[df["query"] == q]
        rates = sorted(df_q["producer_rate"].dropna().unique().tolist())
        for metric in metrics:
            if metric not in df.columns:
                continue
            for r in rates:
                save_base = os.path.join(
                    out_dir,
                    "low_high_across_hw",
                    q.replace(" ", "_"),
                    metric,
                    f"rate{int(r)}",
                )
                print(f"[INFO] {q} | {metric} | rate={int(r)} -> {save_base}")
                try:
                    plot_low_high_across_hardware(
                        df=df,
                        query=q,
                        metric=metric,
                        producer_rate=int(r),
                        frameworks=frameworks,
                        hardware_types=hardware_types,
                        par_map=par_map,
                        agg=agg,
                        title=None,
                        save_path=save_base,
                        show=show,
                        all_formats=all_formats,
                        show_values=show_values,
                    )
                except Exception as e:
                    print(f"[WARN] Skipping {q} | {metric} | rate={int(r)}: {e}")


def parse_args():
    p = argparse.ArgumentParser(description="Low vs high parallelism plots across multiple hardware in one figure.")
    p.add_argument("--csv", type=str, default="data.csv")
    p.add_argument("--plot", type=str, required=True, choices=["low_high_across_hw", "batch_low_high_across_hw"])
    p.add_argument("--frameworks", nargs="*", default=["flink", "storm"])
    p.add_argument("--hardware-types", nargs="*", default=None,
                   help="Hardware types to include. If omitted, uses all found in CSV.")
    p.add_argument("--par-map-json", type=str, default=None,
                   help="JSON mapping hw -> [low, high] or {low:..,high:..}. Required for selected hardware.")
    p.add_argument("--query", type=str, default=None)
    p.add_argument("--metric", type=str, default=None)
    p.add_argument("--producer_rate", type=int, default=None)
    p.add_argument("--agg", type=str, default="mean", choices=["mean", "median", "max", "min"])
    p.add_argument("--save", type=str, default=None)
    p.add_argument("--out_dir", type=str, default="plots")
    p.add_argument("--no-show", action="store_true")
    p.add_argument("--all-formats", action="store_true")
    p.add_argument("--no-values", action="store_true")
    p.add_argument("--metrics", nargs="*", default=None,
                   help="Override metrics list in batch mode, e.g. --metrics throughput_50 endtoend_latency")
    return p.parse_args()


def main():
    configure_style()
    args = parse_args()

    df = load_data(args.csv)

    frameworks = [f.lower() for f in (args.frameworks or [])]

    if args.hardware_types and len(args.hardware_types) > 0:
        hardware_types = list(args.hardware_types)
    else:
        hardware_types = sorted(df["hardware_type"].dropna().unique().tolist())

    hardware_types = order_hardware(hardware_types)

    par_map = parse_par_map_json(args.par_map_json)
    missing_hw = [hw for hw in hardware_types if hw not in par_map]
    if missing_hw:
        raise SystemExit(
            "Missing low/high parallelism for hardware: "
            + ", ".join(missing_hw)
            + "\nProvide them via --par-map-json."
        )

    show = not args.no_show
    show_values = not args.no_values

    if args.plot == "low_high_across_hw":
        if args.query is None or args.metric is None or args.producer_rate is None:
            raise SystemExit("low_high_across_hw requires --query, --metric, --producer_rate")

        plot_low_high_across_hardware(
            df=df,
            query=args.query,
            metric=args.metric,
            producer_rate=int(args.producer_rate),
            frameworks=frameworks,
            hardware_types=hardware_types,
            par_map=par_map,
            agg=args.agg,
            title=None,
            save_path=args.save,
            show=show,
            all_formats=args.all_formats,
            show_values=show_values,
        )

    elif args.plot == "batch_low_high_across_hw":
        metrics = args.metrics if args.metrics else METRICS
        batch_low_high_across_hw(
            df=df,
            frameworks=frameworks,
            hardware_types=hardware_types,
            par_map=par_map,
            out_dir=args.out_dir,
            agg=args.agg,
            show=show,
            all_formats=args.all_formats,
            show_values=show_values,
            metrics=metrics,
        )


if __name__ == "__main__":
    main()
