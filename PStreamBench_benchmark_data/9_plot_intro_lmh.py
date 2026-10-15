#!/usr/bin/env python3
"""Intro figure in the paper layout: LOW / HIGH parallelism, one panel per workload.

Reuses the baseline helpers from 3_plot_pdsp_query_metrics.py (data loading,
query IDs, parallelism whitelist, style, hardware palettes, framework hatches).
Encoding (same as the baseline low/med/high plots):
  colour = hardware, hatch = framework (Flink solid, Storm xx, Spark .., Kafka Streams //, RisingWave \\)
  value labels inside the bars.
Missing (framework, hardware, level) combinations are not drawn; they are not zero.
Speed-up arrows are left to be added by hand.
"""
import argparse
import importlib.util
import os
import warnings

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
BASELINE = os.path.join(HERE, "3_plot_pdsp_query_metrics.py")

FRAMEWORK_ORDER = ["flink", "storm", "spark", "kafkastream", "risingwave"]
FRAMEWORK_LABEL = {"flink": "Flink", "storm": "Storm", "spark": "Spark",
                   "kafkastream": "Kafka Streams", "risingwave": "RisingWave"}


def load_baseline():
    spec = importlib.util.spec_from_file_location("baseline3", BASELINE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def short_value(v):
    if v >= 1000:
        return f"{v / 1000:.0f}k" if v >= 10000 else f"{v / 1000:.1f}k"
    return f"{v:.0f}"


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--csv", default="query_performance_data.csv")
    p.add_argument("--strategy", default="uniform")
    p.add_argument("--queries", nargs="+", default=["Ad Analytics_1", "Smart Grid_1"])
    p.add_argument("--hardware", nargs="+", default=["m510", "c6525-25g"])
    p.add_argument("--frameworks", nargs="+", default=FRAMEWORK_ORDER)
    p.add_argument("--rate", type=float, default=1_000_000)
    p.add_argument("--metric", default="throughput_50")
    p.add_argument("--palette", default="modern")
    p.add_argument("--out", required=True, help="output path without extension")
    p.add_argument("--formats", default="png,pdf,svg")
    args = p.parse_args()

    b = load_baseline()
    b.configure_style(mpl_style="science", latex_text=False)
    palette = b.HW_PALETTES[args.palette]

    df = b.load_data(args.csv)
    df = b.add_query_id_columns(df)
    df = b.apply_parallelism_whitelist(df)
    df = df[(df["enumeration_strategy"] == args.strategy)
            & (df["producer_rate"].astype(float) == args.rate)]

    # LOW = first, HIGH = last entry of the baseline parallelism map (2, 16, 40 on all hardware)
    par_map = {"m510": (2, 16, 40), "c6525-25g": (2, 16, 40), "c6525-100g": (2, 16, 40)}
    levels = ["LOW", "HIGH"]

    frameworks = [f for f in FRAMEWORK_ORDER if f in args.frameworks]
    fig, axes = plt.subplots(1, len(args.queries), figsize=(4.5 * len(args.queries), 3.4), squeeze=False)
    axes = axes[0]
    width = 0.8 / (len(frameworks) * len(args.hardware))

    for ax, query in zip(axes, args.queries):
        sub = df[df["query_id"] == query]
        if sub.empty:
            warnings.warn(f"no rows for {query} / {args.strategy}; panel left empty")
        for g, level in enumerate(levels):
            k = 0
            for fw in frameworks:
                for hw in args.hardware:
                    par = par_map[hw][0] if level == "LOW" else par_map[hw][2]
                    row = sub[(sub["framework"] == fw) & (sub["hardware_type"] == hw)
                              & (sub["parallelism_avg"] == par)]
                    x = g + (k - (len(frameworks) * len(args.hardware) - 1) / 2) * width
                    k += 1
                    if row.empty:
                        continue  # missing combination: not drawn, not zero
                    value = row[args.metric].astype(float).mean()
                    hatch = b.FRAMEWORK_HATCH_LMH.get(fw, "")
                    ax.bar(x, value, width=width, color=palette.get(hw, "gray"), hatch=hatch,
                           edgecolor="black", linewidth=0.4)
                    inside = value >= 0.12 * sub[args.metric].max()
                    ax.text(x, value / 2 if inside else value, short_value(value), rotation=90,
                            ha="center", va="center" if inside else "bottom",
                            fontsize=6.5, fontweight="bold",
                            color="white" if inside else "black")
        ax.set_xticks(np.arange(len(levels)))
        ax.set_xticklabels(levels)
        ax.set_xlabel("Degree of Parallelism (per operator)")
        ax.set_title(query.rsplit("_", 1)[0], fontsize=9)
        ax.set_ylabel("Throughput (Events/sec.)")
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)

    hw_handles = [Patch(facecolor=palette.get(hw, "gray"), edgecolor="black", linewidth=0.4, label=hw)
                  for hw in args.hardware]
    fw_handles = [Patch(facecolor="white", edgecolor="black", hatch=b.FRAMEWORK_HATCH_LMH.get(fw, ""),
                        linewidth=0.4, label=FRAMEWORK_LABEL[fw]) for fw in frameworks]
    fig.legend(handles=hw_handles, title="Hardware", loc="upper left", ncol=len(hw_handles),
               bbox_to_anchor=(0.05, 1.02), frameon=True, fontsize=8, title_fontsize=8)
    fig.legend(handles=fw_handles, title="DSPS", loc="upper right", ncol=len(fw_handles),
               bbox_to_anchor=(0.95, 1.02), frameon=True, fontsize=8, title_fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.9))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    for fmt in [f.strip() for f in args.formats.split(",") if f.strip()]:
        path = f"{args.out}.{fmt}"
        fig.savefig(path, dpi=300 if fmt == "png" else None, bbox_inches="tight")
        print(f"  wrote {path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
