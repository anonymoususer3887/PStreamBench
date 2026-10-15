#!/usr/bin/env python3
"""Enumeration-strategy analysis plots (validation stage).

Separate from 3_/6_ on purpose: those remain the baseline. This script reuses
the baseline's uniform scaling helper only for the Figure F regression check.

Two dimensions, kept apart:
  A. search space  -> every unique configuration a strategy evaluated
  C/D/E. selected  -> one configuration per strategy under one objective

Two parallelism representations, both shown:
  mean / sum of INNER operators (source and sink excluded)
  per-operator allocation (from operator_templates.csv)

Operator-level figures use only (framework, query, variant) rows whose
template status is verified or inferred. Kafka Streams and RisingWave are
job-level only: Kafka Streams = effective stream threads (vector[1]),
RisingWave = job parallelism.

Usage examples: see 7_make_enumeration_plots.sh (validate / list of modes).
"""
import argparse
import csv
import glob
import importlib.util
import os
import sys
import warnings

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "query_performance_data.csv")
TEMPLATE_PATH = os.path.join(HERE, "operator_templates.csv")
ARTIFACTS = os.path.join(HERE, "artifacts")
BASELINE_PATH = os.path.join(HERE, "3_plot_pdsp_query_metrics.py")
OUT_ROOT = os.path.join(HERE, "plots_enumeration")
# B/C/E/F (the config-ID based replacements for the 1-14 baseline families on Random/DS2/
# Dhalion) are written alongside Uniform's actual 1-14 output, not into plots_enumeration/.
PLOTS_TODATE_ROOT = os.path.join(HERE, "plots_todate")

# ---------------------------------------------------------------------------
# Central definitions (defined once; the shell wrapper reads the groups via
# --print-group so they are not duplicated there)
# ---------------------------------------------------------------------------
FRAMEWORK_GROUPS = {
    "all_5": ["flink", "storm", "spark", "kafkastream", "risingwave"],
    "flink_spark_storm": ["flink", "spark", "storm"],
    "kafka_risingwave": ["kafkastream", "risingwave"],
}
FRAMEWORK_ORDER = ["flink", "storm", "spark", "kafkastream", "risingwave"]
FRAMEWORK_LABEL = {
    "flink": "Flink",
    "storm": "Storm",
    "spark": "Spark",
    "kafkastream": "Kafka Streams",
    "risingwave": "RisingWave",
}
JOB_LEVEL_FRAMEWORKS = {"kafkastream", "risingwave"}

STRATEGY_ORDER = ["uniform", "random", "ds2", "dhalion"]
STRATEGY_LABEL = {"uniform": "Uniform", "random": "Random", "ds2": "DS2", "dhalion": "Dhalion"}
# Strategy owns color + marker + linestyle + hatch in every figure.
STRATEGY_STYLE = {
    "uniform": dict(color="#000000", marker="s", linestyle="--", hatch=""),
    "random": dict(color="#0072B2", marker="o", linestyle="-", hatch="//"),
    "ds2": dict(color="#009E73", marker="^", linestyle="-.", hatch=".."),
    "dhalion": dict(color="#D55E00", marker="D", linestyle=":", hatch="xx"),
}
# Framework palette, used only where framework is the sole varying dimension.
FRAMEWORK_COLOR = {
    "flink": "#332288",
    "storm": "#DDCC77",
    "spark": "#117733",
    "kafkastream": "#CC6677",
    "risingwave": "#88CCEE",
}

# Centralized typography / geometry (points / inches)
STYLE = dict(
    font_axis=11, font_tick=9, font_legend=9, font_title=11, font_annot=8,
    line_width=1.2, marker_size=5, errorbar_capsize=2, dpi=300,
)
FIGURE_SIZE = {"single": (3.5, 2.8), "double": (7.0, 3.2)}

# Pairs of (framework, query) whose logical semantics were checked by reading
# the job code. Used to decide whether a cross-framework job-level figure is
# allowed. Anything not listed here is not compared.
SEMANTIC_EQUIVALENT = {
    ("kafkastream", "risingwave", "Word Count"): (
        "Both run an unwindowed running word count: KS WordCountTopology "
        "(flatMapValues split + groupByKey().count()); RW word_count.sql "
        "(unnest split + GROUP BY word, COUNT(*))."
    ),
}


def semantics_ok(framework_group, query):
    """Job-level engines (Kafka Streams, RisingWave) may only share a figure when the
    query is recorded as semantically equivalent for that pair. Operator-level engines
    follow the template status rules instead. Returns (ok, reason)."""
    frameworks = FRAMEWORK_GROUPS[framework_group]
    job_level = [f for f in frameworks if f in JOB_LEVEL_FRAMEWORKS]
    if len(job_level) < 2:
        return True, "no job-level pair in this group"
    for a in job_level:
        for b in job_level:
            if a < b and not any({k[0], k[1]} == {a, b} and k[2] == query for k in SEMANTIC_EQUIVALENT):
                return False, (f"{FRAMEWORK_LABEL[a]} vs {FRAMEWORK_LABEL[b]} semantics not recorded "
                               f"for query '{query}'")
    return True, "job-level pair recorded as equivalent"


def apply_style(paper_mode):
    plt.rcParams.update({
        "font.size": STYLE["font_tick"],
        "axes.labelsize": STYLE["font_axis"],
        "xtick.labelsize": STYLE["font_tick"],
        "ytick.labelsize": STYLE["font_tick"],
        "legend.fontsize": STYLE["font_legend"],
        "axes.titlesize": STYLE["font_title"],
        "lines.linewidth": STYLE["line_width"],
        "lines.markersize": STYLE["marker_size"],
        "pdf.fonttype": 42,
        "svg.fonttype": "none",
        "axes.spines.top": False,
        "axes.spines.right": False,
    })


def _safe_tight_layout(fig, **kwargs):
    """fig.tight_layout() wrapper that works around a real matplotlib bug, confirmed by
    direct reproduction: with an extreme shared-axis value range (e.g. 600 vs 3.6M,
    forcing scientific-notation y-axis offset text) combined with rotated tick/bar
    labels, tight_layout's internal solve can diverge and leave axes positions as NaN.
    That doesn't fail here -- it fails later, inside savefig, with "cannot convert
    float NaN to integer", which looked like a data bug until traced to this. Falls
    back to a fixed-margin layout instead of losing the whole batch run to one figure."""
    fig.tight_layout(**kwargs)
    for ax in fig.get_axes():
        pos = ax.get_position()
        if not all(np.isfinite([pos.x0, pos.y0, pos.x1, pos.y1])):
            fig.subplots_adjust(left=0.12, right=0.96, bottom=0.22, top=0.85, wspace=0.35, hspace=0.4)
            break


def save_figure(fig, out_base, formats):
    os.makedirs(os.path.dirname(out_base), exist_ok=True)
    for fmt in formats:
        path = f"{out_base}.{fmt}"
        if fmt == "png":
            fig.savefig(path, dpi=STYLE["dpi"], bbox_inches="tight")
        else:
            fig.savefig(path, bbox_inches="tight")
        print(f"  wrote {path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Data loading and operator templates
# ---------------------------------------------------------------------------
def load_csv():
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df["query_variant"] = df["query_variant"].fillna("").astype(str)
    for col in ["producer_rate", "throughput_50", "endtoend_latency", "cpu_50",
                "memory_50", "network_50"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def count_excluded_uniform_1():
    files = glob.glob(os.path.join(ARTIFACTS, "**", "uniform_1", "**", "evaluationMetrics-*.json"),
                      recursive=True)
    return len(files)


def load_templates():
    templates = {}
    with open(TEMPLATE_PATH, newline="") as f:
        for row in csv.DictReader(f):
            key = (row["framework"], row["query"], row["query_variant"], row["expected_length"])
            templates.setdefault(key, []).append(row)
    for key in templates:
        templates[key].sort(key=lambda r: int(r["stage_index"]))
    return templates


def lookup_template(templates, framework, query, variant, vec_len):
    """Return (stages, status) or (None, reason)."""
    rows = templates.get((framework, query, variant, str(vec_len)))
    if rows is None and framework in JOB_LEVEL_FRAMEWORKS:
        rows = templates.get((framework, query, variant, "any"))
    if not rows:
        return None, "no template for this vector length"
    statuses = {r["status"] for r in rows}
    if "unresolved" in statuses:
        return None, "template unresolved"
    return rows, ("job_level_only" if statuses == {"job_level_only"} else "verified" if statuses == {"verified"} else "inferred")


def make_label(vals, stages):
    parts = []
    last = len(vals) - 1
    for i, v in enumerate(vals):
        if i == 0 and stages[i]["includes_source"] == "1":
            parts.append(f"s{v}")
        elif i == last and stages[i]["includes_sink"] == "1":
            parts.append(f"k{v}")
        else:
            parts.append(f"x{v}")
    return "_".join(parts)


def build_configs(df, framework_group, query, variant, hw, rate, metric, templates):
    """One record per run, with the configuration identity attached.
    Rows that cannot be mapped to a template are skipped and reported."""
    frameworks = FRAMEWORK_GROUPS[framework_group]
    sub = df[(df["query"] == query) & (df["query_variant"] == variant)
             & (df["hardware_type"] == hw) & (df["producer_rate"] == rate)
             & (df["framework"].isin(frameworks))]
    records, skipped = [], []
    for _, r in sub.iterrows():
        fw = r["framework"]
        vec = [int(float(x)) for x in str(r["parallelism"]).split(",") if x.strip() != ""]
        stages, status = lookup_template(templates, fw, query, variant, len(vec))
        if stages is None:
            skipped.append((fw, r["enumeration_strategy"], len(vec), status))
            continue
        if fw in JOB_LEVEL_FRAMEWORKS:
            if fw == "kafkastream":
                if len(vec) < 2:
                    skipped.append((fw, r["enumeration_strategy"], len(vec), "no vector[1]"))
                    continue
                eff = vec[1]
                cfg_id = f"StreamThreads:{eff}"
                label = f"t{eff}"
            else:
                eff = vec[0]
                cfg_id = f"JobParallelism:{eff}"
                label = f"j{eff}"
            rec = dict(framework=fw, strategy=r["enumeration_strategy"], config_id=cfg_id,
                       config_label=label, job_level=True, effective=eff,
                       stage_names=[], inner=[], mean_inner=np.nan, sum_inner=np.nan,
                       max_par=eff, total_par=eff, vec_len=len(vec), status=status,
                       metric=r[metric], path=r.get("path", ""))
        else:
            names = [s["stage_name"] for s in stages]
            inner = [v for v, s in zip(vec, stages)
                     if s["includes_source"] == "0" and s["includes_sink"] == "0"]
            cfg_id = "|".join(f"{n}:{v}" for n, v in zip(names, vec))
            rec = dict(framework=fw, strategy=r["enumeration_strategy"], config_id=cfg_id,
                       config_label=make_label(vec, stages), job_level=False, effective=np.nan,
                       stage_names=names, vec=vec, inner=inner,
                       mean_inner=float(np.mean(inner)) if inner else np.nan,
                       sum_inner=float(np.sum(inner)) if inner else np.nan,
                       max_par=max(vec), total_par=sum(vec), vec_len=len(vec), status=status,
                       metric=r[metric], path=r.get("path", ""))
        records.append(rec)
    return pd.DataFrame(records), skipped


def aggregate_configs(runs):
    """Collapse runs to unique configurations. Repetitions of the same config
    are one point; run_count is kept, and uncertainty only if n >= 2."""
    if runs.empty:
        return runs
    keys = ["framework", "strategy", "config_id"]
    agg = runs.groupby(keys, dropna=False).agg(
        config_label=("config_label", "first"),
        job_level=("job_level", "first"),
        effective=("effective", "first"),
        stage_names=("stage_names", "first"),
        status=("status", "first"),
        mean_inner=("mean_inner", "first"),
        sum_inner=("sum_inner", "first"),
        max_par=("max_par", "first"),
        total_par=("total_par", "first"),
        metric_mean=("metric", "mean"),
        metric_std=("metric", "std"),
        run_count=("metric", "size"),
    ).reset_index()
    agg["metric_std"] = agg["metric_std"].where(agg["run_count"] >= 2, np.nan)
    return agg


def select_best(agg):
    """Deterministic selection: highest mean metric, then lower total
    parallelism, then config_id. Applied per (framework, strategy)."""
    if agg.empty:
        return agg
    ordered = agg.sort_values(["metric_mean", "total_par", "config_id"],
                              ascending=[False, True, True])
    return ordered.groupby(["framework", "strategy"], as_index=False).head(1)


# ---------------------------------------------------------------------------
# Coverage (printed before every validation plot)
# ---------------------------------------------------------------------------
def print_coverage(runs, agg, skipped, framework_group, query, variant, hw, rate):
    print("\n=== COVERAGE  group=%s  query=%s  variant=%s  hw=%s  rate=%s ===" %
          (framework_group, query, variant or "-", hw, rate))
    if runs.empty and not skipped:
        print("  no rows for this selection")
        return
    rows = []
    frameworks = [f for f in FRAMEWORK_ORDER if f in FRAMEWORK_GROUPS[framework_group]]
    for fw in frameworks:
        for s in STRATEGY_ORDER:
            r = runs[(runs["framework"] == fw) & (runs["strategy"] == s)] if not runs.empty else runs
            a = agg[(agg["framework"] == fw) & (agg["strategy"] == s)] if not agg.empty else agg
            if r.empty:
                rows.append(dict(framework_group=framework_group, framework=FRAMEWORK_LABEL[fw],
                                 strategy=STRATEGY_LABEL[s], query=query, variant=variant or "-",
                                 hardware=hw, rate=rate, unique_configs=0, runs=0,
                                 vector_lengths="-", template_status="MISSING (N/A, not zero)",
                                 min_mean_inner="-", max_mean_inner="-", min_sum_inner="-", max_sum_inner="-"))
                continue
            rows.append(dict(
                framework_group=framework_group, framework=FRAMEWORK_LABEL[fw],
                strategy=STRATEGY_LABEL[s], query=query, variant=variant or "-", hardware=hw, rate=rate,
                unique_configs=len(a), runs=len(r),
                vector_lengths=",".join(str(x) for x in sorted(set(r["vec_len"]))) if "vec_len" in r else "-",
                template_status=",".join(sorted(set(r["status"]))),
                min_mean_inner=_fmt(a["mean_inner"].min()), max_mean_inner=_fmt(a["mean_inner"].max()),
                min_sum_inner=_fmt(a["sum_inner"].min()), max_sum_inner=_fmt(a["sum_inner"].max()),
            ))
    table = pd.DataFrame(rows)
    with pd.option_context("display.max_columns", None, "display.width", 250):
        print(table.to_string(index=False))
    if skipped:
        print("  SKIPPED (template unresolved / no template / no thread info):")
        seen = {}
        for fw, s, n, why in skipped:
            seen[(fw, s, n, why)] = seen.get((fw, s, n, why), 0) + 1
        for (fw, s, n, why), c in seen.items():
            print(f"    {FRAMEWORK_LABEL.get(fw, fw)} / {s} / vector_len={n}: {why} ({c} runs)")


def _fmt(x):
    return "-" if pd.isna(x) else f"{x:.2f}"


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------
def _short_value(v):
    """Compact bar label: 426k, 2.7k, 84 (paper-style)."""
    if v >= 1000:
        return f"{v / 1000:.0f}k" if v >= 10000 else f"{v / 1000:.1f}k"
    return f"{v:.0f}"


def strategy_handles(ax, strategies, linestyled=False):
    handles = []
    for s in strategies:
        st = STRATEGY_STYLE[s]
        h = ax.plot([], [], color=st["color"], marker=st["marker"], linestyle=st["linestyle"] if linestyled else "none",
                    markeredgecolor="black", markeredgewidth=0.4, label=STRATEGY_LABEL[s])[0]
        handles.append(h)
    return handles


def fig_searchspace(agg, args, out_dir):
    """A1 (mean inner parallelism) or A2 (sum inner parallelism): every unique config."""
    xcol = "mean_inner" if args.parallelism_summary == "mean" else "sum_inner"
    xlabel = ("Mean inner-operator parallelism (source & sink excluded)" if xcol == "mean_inner"
              else "Sum of inner-operator parallelism (source & sink excluded)")
    fws = [f for f in FRAMEWORK_ORDER if f in set(agg["framework"]) and f not in JOB_LEVEL_FRAMEWORKS]
    if not fws:
        print("  A: no operator-level frameworks with data; skipping (job-level excluded here)")
        return
    scale = 1000 if args.metric.startswith("throughput") else 1
    unit = "k events/s" if scale == 1000 else ""
    width = 1.9 * len(fws) + 0.9
    fig, axes = plt.subplots(1, len(fws), figsize=(width, 2.6), sharey=True, squeeze=False)
    axes = axes[0]
    for i, (ax, fw) in enumerate(zip(axes, fws)):
        sub = agg[agg["framework"] == fw]
        for s in STRATEGY_ORDER:
            d = sub[sub["strategy"] == s]
            if d.empty:
                continue
            st = STRATEGY_STYLE[s]
            yerr = d["metric_std"].fillna(0).values / scale  # n=1 -> 0 (no uncertainty shown)
            ax.errorbar(d[xcol], d["metric_mean"] / scale, yerr=yerr, fmt=st["marker"], color=st["color"],
                        markeredgecolor="black", markeredgewidth=0.4, linestyle="none",
                        markersize=4.5, capsize=2, elinewidth=0.6,
                        label=STRATEGY_LABEL[s])
        ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_tick"] + 1, pad=3)
        ax.set_xlabel("Mean inner parallelism" if xcol == "mean_inner" else "Sum of inner parallelism",
                      fontsize=STYLE["font_axis"] - 1)
        ax.grid(alpha=0.3, linewidth=0.5)
        ax.tick_params(labelsize=STYLE["font_tick"] - 1)
    axes[0].set_ylabel(f"Throughput ({unit})" if unit else args.metric, fontsize=STYLE["font_axis"] - 1)
    handles, labels = axes[0].get_legend_handles_labels()
    axes[0].legend(handles, labels, fontsize=STYLE["font_legend"] - 1, frameon=False, loc="upper left",
                   handletextpad=0.2, borderaxespad=0.2)
    _safe_tight_layout(fig, w_pad=0.8)
    tag = "A1_searchspace_mean" if xcol == "mean_inner" else "A2_searchspace_sum"
    save_figure(fig, os.path.join(out_dir, tag), args.formats)


def fig_config_labels(agg, args, out_dir):
    """B: x = compact configuration label, bars colored by strategy."""
    fws = [f for f in FRAMEWORK_ORDER if f in set(agg["framework"]) and f not in JOB_LEVEL_FRAMEWORKS]
    if not fws:
        print("  B: no operator-level frameworks with data (job-level only here); skipping")
        return
    fig, axes = plt.subplots(len(fws), 1, figsize=(FIGURE_SIZE[args.figure_size][0],
                                                   FIGURE_SIZE[args.figure_size][1] * max(1, len(fws)) * 0.9),
                             squeeze=False)
    for ax, fw in zip(axes[:, 0], fws):
        sub = agg[agg["framework"] == fw].copy()
        sub["s_order"] = sub["strategy"].map({s: i for i, s in enumerate(STRATEGY_ORDER)})
        sub = sub.sort_values(["s_order", "config_label"])
        xs = np.arange(len(sub))
        for i, (_, r) in enumerate(sub.iterrows()):
            st = STRATEGY_STYLE[r["strategy"]]
            err = r["metric_std"] if (r["run_count"] >= 2 and not pd.isna(r["metric_std"])) else 0
            ax.bar(xs[i], r["metric_mean"], color=st["color"], hatch=st["hatch"], edgecolor="black",
                   linewidth=0.4, yerr=err, capsize=STYLE["errorbar_capsize"], error_kw=dict(elinewidth=0.8))
            top = ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else r["metric_mean"]
            if r["metric_mean"] >= 0.12 * max(sub["metric_mean"].max(), 1e-9):
                ax.text(xs[i], r["metric_mean"] / 2, _short_value(r["metric_mean"]), rotation=90, ha="center",
                        va="center", fontsize=STYLE["font_annot"], fontweight="bold", color="white")
            else:
                ax.text(xs[i], r["metric_mean"], _short_value(r["metric_mean"]), rotation=90, ha="center",
                        va="bottom", fontsize=STYLE["font_annot"], fontweight="bold", color="black")
        ax.set_xticks(xs)
        ax.set_xticklabels(sub["config_label"], rotation=40, ha="right", fontsize=STYLE["font_tick"])
        ax.set_ylabel(args.metric)
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        if not args.paper_mode:
            ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_title"])
    handles = [plt.Rectangle((0, 0), 1, 1, facecolor=STRATEGY_STYLE[s]["color"],
                             hatch=STRATEGY_STYLE[s]["hatch"], edgecolor="black", linewidth=0.4,
                             label=STRATEGY_LABEL[s]) for s in STRATEGY_ORDER]
    fig.legend(handles=handles, fontsize=STYLE["font_legend"], frameon=False, ncol=4, loc="upper center",
               bbox_to_anchor=(0.5, 1.0))
    _safe_tight_layout(fig, rect=(0, 0, 1, 0.95))
    save_figure(fig, os.path.join(out_dir, "B_config_labels"), args.formats)


def fig_selected(agg, args, out_dir):
    """F: one selected configuration per strategy, plotted per operator, plus the cross-framework
    operator comparison (which also covers job-level-only groups, so it must not be gated on
    operator-level data existing)."""
    best = select_best(agg)
    best = best[best["framework"].isin([f for f in FRAMEWORK_ORDER if f not in JOB_LEVEL_FRAMEWORKS])]
    if best.empty:
        print("  F: no operator-level selected configs for this group (job-level frameworks only "
              "get the cross-framework comparison below, not a per-operator breakdown)")
        _fig_operator_compare_frameworks(agg, args, out_dir)
        return
    rows = []
    for _, r in best.iterrows():
        rows.append(dict(framework=FRAMEWORK_LABEL[r["framework"]], strategy=STRATEGY_LABEL[r["strategy"]],
                         config_id=r["config_id"], metric_mean=r["metric_mean"],
                         mean_inner=r["mean_inner"], sum_inner=r["sum_inner"],
                         max_parallelism=r["max_par"], run_count=int(r["run_count"])))
    summary = pd.DataFrame(rows)
    fws = [f for f in FRAMEWORK_ORDER if f in set(best["framework"])]
    fig, axes = plt.subplots(1, len(fws), figsize=FIGURE_SIZE[args.figure_size], sharey=True, squeeze=False)
    # Grouped bars, not overlapping lines -- lines from 4 strategies on top of each other were
    # unreadable. One bar cluster per operator, one bar per strategy within the cluster.
    # y = parallelism per operator (reverted from a "flat performance" variant that was tried
    # and dropped: with y held constant across operators, the x-axis carried no information and
    # the chart was unreadable -- see commit history / conversation. Overall performance per
    # framework x strategy is covered by E (20_strategy_x_framework) instead.)
    k = len(STRATEGY_ORDER)
    width = 0.8 / k
    for ax, fw in zip(axes[0], fws):
        sub = best[best["framework"] == fw]
        names = None
        for i, s in enumerate(STRATEGY_ORDER):
            d = sub[sub["strategy"] == s]
            if d.empty:
                continue
            r = d.iloc[0]
            if names is None:
                names = list(r["stage_names"])
            if list(r["stage_names"]) != names:
                warnings.warn(f"{fw}: selected configs have different stage templates; not aligned")
                continue
            st = STRATEGY_STYLE[s]
            vals = [int(p.split(":")[1]) for p in r["config_id"].split("|")]
            xs = np.arange(len(vals)) + (i - (k - 1) / 2) * width
            ax.bar(xs, vals, width=width, color=st["color"], hatch=st["hatch"],
                  edgecolor="black", linewidth=0.4, label=STRATEGY_LABEL[s])
        if names:
            ax.set_xticks(np.arange(len(names)))
            ax.set_xticklabels(names, rotation=40, ha="right")
        ax.set_ylabel("Parallelism (per operator)")
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        if not args.paper_mode:
            ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_title"])
        ax.set_ylim(bottom=0)
    # Shared legend above the panels -- per-panel throughput values don't fit one shared legend,
    # so those go in the summary CSV instead (same reason inline loc="best" collided with the
    # rotated operator labels in the narrowest panel).
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=STYLE["font_legend"], frameon=False, ncol=4,
              loc="upper center", bbox_to_anchor=(0.5, 1.08))
    _safe_tight_layout(fig, rect=(0, 0, 1, 0.92))
    save_figure(fig, os.path.join(out_dir, "F_combined_operator_allocation"), args.formats)

    # Individual figures too, one per (framework, strategy), as F asks for alongside the combined view.
    for fw in fws:
        sub = best[best["framework"] == fw]
        for s in STRATEGY_ORDER:
            d = sub[sub["strategy"] == s]
            if d.empty:
                continue
            r = d.iloc[0]
            names = list(r["stage_names"])
            vals = [int(p.split(":")[1]) for p in r["config_id"].split("|")]
            fig_i, ax_i = plt.subplots(figsize=(2.6, 2.2))
            st = STRATEGY_STYLE[s]
            ax_i.plot(np.arange(len(vals)), vals, color=st["color"], marker=st["marker"],
                      linestyle=st["linestyle"], markeredgecolor="black", markeredgewidth=0.4)
            ax_i.set_xticks(np.arange(len(names)))
            ax_i.set_xticklabels(names, rotation=40, ha="right", fontsize=STYLE["font_tick"] - 1)
            ax_i.set_ylabel("Parallelism", fontsize=STYLE["font_axis"] - 1)
            ax_i.set_title(f"{FRAMEWORK_LABEL[fw]} / {STRATEGY_LABEL[s]}", fontsize=STYLE["font_tick"] + 1)
            ax_i.set_ylim(bottom=0)
            ax_i.grid(alpha=0.3, linewidth=0.5)
            _safe_tight_layout(fig_i)
            save_figure(fig_i, os.path.join(out_dir, fw, f"F_{s}"), args.formats)

    table_path = os.path.join(out_dir, "F_selected_summary.csv")
    summary.to_csv(table_path, index=False)
    print(f"  wrote {table_path}")
    print("\n  Selected configuration table (objective = throughput):")
    with pd.option_context("display.max_columns", None, "display.width", 250):
        print(summary.to_string(index=False))

    _fig_operator_compare_frameworks(agg, args, out_dir)


def _fig_operator_compare_frameworks(agg, args, out_dir):
    """Operator-wise parallelism of the SELECTED config, compared across frameworks for one
    strategy at a time -- same framework-block/separator idea as C, but for the operator vector
    instead of performance. Job-level frameworks (Kafka Streams, RisingWave) show their single
    job-level value, never fabricated as a per-operator breakdown."""
    best = select_best(agg)
    for s in STRATEGY_ORDER:
        sub = best[best["strategy"] == s]
        op_fws = [f for f in FRAMEWORK_ORDER if f in FRAMEWORK_GROUPS[args.framework_group]
                 and f not in JOB_LEVEL_FRAMEWORKS and f in set(sub["framework"])]
        job_fws = [f for f in FRAMEWORK_ORDER if f in FRAMEWORK_GROUPS[args.framework_group]
                  and f in JOB_LEVEL_FRAMEWORKS and f in set(sub["framework"])]
        blocks = []
        for fw in op_fws:
            r = sub[sub["framework"] == fw].iloc[0]
            vals = [int(p.split(":")[1]) for p in r["config_id"].split("|")]
            blocks.append((fw, list(r["stage_names"]), vals))
        for fw in job_fws:
            r = sub[sub["framework"] == fw].iloc[0]
            label = "Threads" if fw == "kafkastream" else "JobPar"
            blocks.append((fw, [label], [int(r["effective"])]))
        if not blocks:
            continue
        st = STRATEGY_STYLE[s]
        fig_w = min(9.0, max(3.2, 0.5 * sum(len(b[1]) for b in blocks) + 1.3 * len(blocks)))
        fig, ax = plt.subplots(figsize=(fig_w, 3.0))
        pos, xticks, xticklabels, boundaries = 0, [], [], []
        for fw, names, vals in blocks:
            start = pos
            for n, v in zip(names, vals):
                ax.bar(pos, v, color=st["color"], hatch=st["hatch"], edgecolor="black", linewidth=0.4)
                xticks.append(pos)
                xticklabels.append(n)
                pos += 1
            boundaries.append((fw, start, pos - 1))
            pos += 1
        for fw, start, end in boundaries[:-1]:
            ax.axvline(end + 0.5, color="black", linewidth=0.7, linestyle=":")
        all_single = all(len(n) == 1 for _, n, _ in blocks)
        ax.set_xticks(xticks)
        if all_single:
            ax.set_xticklabels(xticklabels, fontsize=STYLE["font_tick"] - 1)
            fw_y, tick_gap = -0.22, 0.30
        else:
            ax.set_xticklabels(xticklabels, rotation=40, ha="right", fontsize=STYLE["font_tick"] - 1)
            fw_y, tick_gap = -0.42, 0.50
        for fw, start, end in boundaries:
            ax.text((start + end) / 2, fw_y, FRAMEWORK_LABEL[fw], transform=ax.get_xaxis_transform(),
                    ha="center", va="top", fontsize=STYLE["font_axis"] - 1, fontweight="bold")
        ax.set_ylabel("Parallelism", fontsize=STYLE["font_axis"] - 1)
        ax.set_title(f"{STRATEGY_LABEL[s]} — selected-config operator parallelism across frameworks\n"
                    f"{args.query} ({args.query_variant or '-'}) | {args.hw}", fontsize=STYLE["font_tick"] + 1)
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        _safe_tight_layout(fig, rect=(0, tick_gap, 1, 1))
        save_figure(fig, os.path.join(out_dir, f"F_operator_compare_{s}_{args.framework_group}"), args.formats)


def assign_short_ids(sub):
    """Local C1..Cn ids for one (framework, strategy, query, variant, hw, rate) group, sorted
    deterministically (total parallelism, then config_id) so the same inputs always give the
    same ids. C1 in one framework is unrelated to C1 in another -- ids are local, not global."""
    ordered = sub.sort_values(["total_par", "config_id"]).reset_index(drop=True)
    ordered = ordered.copy()
    ordered["short_id"] = [f"C{i + 1}" for i in range(len(ordered))]
    return ordered


def _write_config_mapping(ordered, path, extra_cols=()):
    cols = ["short_id", "config_id", "metric_mean", "metric_std", "run_count",
            "mean_inner", "sum_inner", "max_par", "total_par", *extra_cols]
    cols = [c for c in cols if c in ordered.columns]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    ordered[cols].to_csv(path, index=False)
    print(f"  wrote {path}")


def _mapping_lines(ordered, max_rows=10, abbreviate_stages=False):
    """Ci -> actual configuration, as short readable lines for placing directly on the figure.
    This is the fix for the earlier bug: C1..Cn had no visible meaning except in a separate CSV.
    abbreviate_stages=True truncates each stage name to 4 chars for the header line -- needed
    when this block sits in a narrow side-by-side column (several framework groups in one
    figure) where the full names ("Source-Tokenizer-Counter-Sink") run wider than the column
    and bleed into the next one; the full names are always in the mapping CSV regardless."""
    lines = []
    if len(ordered) and list(ordered.iloc[0].get("stage_names", []) or []):
        names = ordered.iloc[0]["stage_names"]
        if abbreviate_stages:
            names = [n[:4] for n in names]
        lines.append("stages: " + "-".join(names))
    for _, row in ordered.head(max_rows).iterrows():
        if row.get("job_level"):
            lines.append(f"{row['short_id']}: {row['config_id']}")
        else:
            vals = [p.split(":")[1] for p in str(row["config_id"]).split("|")]
            lines.append(f"{row['short_id']}: " + "-".join(vals))
    if len(ordered) > max_rows:
        lines.append(f"... +{len(ordered) - max_rows} more -> mapping csv")
    return lines


def _add_mapping_caption(fig, groups, max_total_lines=18):
    """groups: list of (header_or_None, ordered_df). Places a monospace caption block under the
    figure with Ci -> configuration lines, capped so it never makes the figure unreasonably
    tall, with a pointer to the full mapping CSV when truncated. This is the direct fix for
    C1..Cn having no visible meaning on the figure itself."""
    lines = []
    for header, ordered in groups:
        if header:
            lines.append(f"[{header}]")
        lines.extend(_mapping_lines(ordered, max_rows=8 if len(groups) > 1 else 10))
    truncated = len(lines) > max_total_lines
    if truncated:
        lines = lines[:max_total_lines] + ["... (full mapping in the _mapping.csv next to this figure)"]
    text = "\n".join(lines)
    fig.text(0.02, 0.0, text, ha="left", va="top", fontsize=STYLE["font_annot"] - 1, family="monospace")
    return len(lines)


def _finalize_with_caption(fig, groups, plot_h_in, fig_w_in, max_rows_per_group=None):
    """Resizes the figure to exactly fit a plot_h_in-tall plot plus a caption strip below it.
    Each group gets its OWN column (side by side, left to right, same order as its block in the
    plot above) instead of being stacked one after another -- stacking was the bug: with several
    framework groups (as in C) the old code concatenated every group's lines into one long
    vertical list, so the caption grew with the NUMBER of groups as well as their size and ended
    up taller than the plot itself. Side-by-side columns bound the caption height by the tallest
    single group, which is what actually keeps the legend from eating the figure."""
    if max_rows_per_group is None:
        max_rows_per_group = 10 if len(groups) <= 1 else 6
    ngroups = max(1, len(groups))
    col_lines = []
    for header, ordered in groups:
        lines = []
        if header:
            lines.append(f"[{header}]")
        lines.extend(_mapping_lines(ordered, max_rows=max_rows_per_group, abbreviate_stages=ngroups > 1))
        col_lines.append(lines)
    max_rows = max((len(c) for c in col_lines), default=0)
    line_h_in = 0.125
    cap_h_in = 0.16 + max_rows * line_h_in
    total_h_in = plot_h_in + cap_h_in
    fig.set_size_inches(fig_w_in, total_h_in)
    _safe_tight_layout(fig, rect=(0, cap_h_in / total_h_in, 1, 1))
    y0 = cap_h_in / total_h_in - 0.015
    col_w = 0.98 / ngroups
    for i, lines in enumerate(col_lines):
        fig.text(0.01 + i * col_w, y0, "\n".join(lines), ha="left", va="top",
                 fontsize=STYLE["font_annot"] - 1, family="monospace")


def fig_config_perf_single(agg, args, out_dir):
    """B: ALL configurations of ONE strategy, ONE framework at a time -- x = C1..Cn (local ids),
    y = metric. This is the per-strategy replacement for mean-parallelism plots on Random/DS2/
    Dhalion, where mean parallelism collapses a real heterogeneous vector into one lossy number."""
    if not args.strategy:
        raise SystemExit("config_perf_single requires --strategy")
    sub_all = agg[(agg["strategy"] == args.strategy)
                  & (agg["framework"].isin(FRAMEWORK_GROUPS[args.framework_group]))]
    fws = [f for f in FRAMEWORK_ORDER if f in set(sub_all["framework"])]
    if not fws:
        print(f"  B: no configurations for strategy={args.strategy}; skipping")
        return
    st = STRATEGY_STYLE[args.strategy]
    for fw in fws:
        sub = assign_short_ids(sub_all[sub_all["framework"] == fw])
        fig, ax = plt.subplots(figsize=(max(3.0, 0.42 * len(sub) + 1.2), 2.8))
        x = np.arange(len(sub))
        yerr = sub["metric_std"].fillna(0).values
        ax.bar(x, sub["metric_mean"], yerr=yerr, color=st["color"], hatch=st["hatch"],
               edgecolor="black", linewidth=0.4, capsize=2, error_kw=dict(elinewidth=0.6))
        for xi, v in zip(x, sub["metric_mean"]):
            _label_bar(ax, xi, v, sub["metric_mean"].max(), 0.7)
        ax.set_xticks(x)
        ax.set_xticklabels(sub["short_id"], fontsize=STYLE["font_tick"])
        ax.set_ylabel(METRIC_YLABEL.get(args.metric, args.metric), fontsize=STYLE["font_axis"] - 1)
        ax.set_title(f"{STRATEGY_LABEL[args.strategy]} / {FRAMEWORK_LABEL[fw]} / {args.query} / {args.hw}",
                    fontsize=STYLE["font_tick"] + 1)
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        fig_w = max(3.0, 0.42 * len(sub) + 1.2)
        _finalize_with_caption(fig, [(None, sub)], plot_h_in=2.4, fig_w_in=fig_w)
        base = os.path.join(out_dir, fw)
        save_figure(fig, os.path.join(base, f"B_{args.strategy}"), args.formats)
        _write_config_mapping(sub, os.path.join(base, f"B_{args.strategy}_mapping.csv"))


def fig_config_perf_compare(agg, args, out_dir):
    """C: ALL configurations of ONE strategy, ALL frameworks in the group together in one
    figure -- each framework keeps its own local C1..Cn, with a visible gap and label between
    framework blocks. Not a scatter/selected view -- this is the full per-strategy search space,
    side by side across systems."""
    if not args.strategy:
        raise SystemExit("config_perf_compare requires --strategy")
    sub_all = agg[(agg["strategy"] == args.strategy)
                  & (agg["framework"].isin(FRAMEWORK_GROUPS[args.framework_group]))]
    fws = [f for f in FRAMEWORK_ORDER if f in set(sub_all["framework"])]
    if not fws:
        print(f"  C: no configurations for strategy={args.strategy}; skipping")
        return
    st = STRATEGY_STYLE[args.strategy]
    blocks = [assign_short_ids(sub_all[sub_all["framework"] == fw]) for fw in fws]
    gap = 1
    x, xticks, xticklabels, boundaries = [], [], [], []
    pos = 0
    for fw, block in zip(fws, blocks):
        start = pos
        for _, row in block.iterrows():
            x.append(pos)
            xticks.append(pos)
            xticklabels.append(row["short_id"])
            pos += 1
        boundaries.append((fw, start, pos - 1))
        pos += gap
    all_rows = pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame()
    fig_w = min(9.0, max(FIGURE_SIZE[args.figure_size][0], 0.38 * len(x) + 1.5))
    fig, ax = plt.subplots(figsize=(fig_w, 3.2))
    yerr = all_rows["metric_std"].fillna(0).values
    ax.bar(x, all_rows["metric_mean"], yerr=yerr, color=st["color"], hatch=st["hatch"],
           edgecolor="black", linewidth=0.4, capsize=2, error_kw=dict(elinewidth=0.6))
    top = all_rows["metric_mean"].max() if len(all_rows) else 1.0
    for xi, v in zip(x, all_rows["metric_mean"]):
        _label_bar(ax, xi, v, top, 0.7)
    for fw, start, end in boundaries[:-1]:
        ax.axvline(end + gap / 2, color="black", linewidth=0.7, linestyle=":")
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels, fontsize=STYLE["font_tick"] - 1)
    for fw, start, end in boundaries:
        ax.text((start + end) / 2, -0.14, FRAMEWORK_LABEL[fw], transform=ax.get_xaxis_transform(),
                ha="center", va="top", fontsize=STYLE["font_axis"] - 1, fontweight="bold")
    ax.set_ylabel(METRIC_YLABEL.get(args.metric, args.metric), fontsize=STYLE["font_axis"] - 1)
    ax.set_title(f"{STRATEGY_LABEL[args.strategy]} — all configurations across frameworks\n"
                f"{args.query} ({args.query_variant or '-'}) | {args.hw} | {int(args.rate)} ev/s",
                fontsize=STYLE["font_tick"] + 1)
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    cap_groups = [(FRAMEWORK_LABEL[fw], block) for fw, block in zip(fws, blocks)]
    _finalize_with_caption(fig, cap_groups, plot_h_in=2.8, fig_w_in=fig_w)
    save_figure(fig, os.path.join(out_dir, f"C_{args.strategy}_compare_{args.framework_group}"), args.formats)
    _write_config_mapping(all_rows, os.path.join(out_dir, f"C_{args.strategy}_compare_{args.framework_group}_mapping.csv"),
                          extra_cols=["framework"])


def fig_config_eventrate(df, templates, args, out_dir):
    """Config-ID version of the event-rate families (03/04): x = event rate, one line per
    actual configuration (not a mean-parallelism match against a fixed rate). Valid because
    Random's sampler re-evaluates the SAME configuration at every rate (fixed seed per query),
    confirmed against the raw data before building this."""
    if not args.strategy:
        raise SystemExit("config_eventrate requires --strategy")
    frames = {}
    for rate in RATE_VALUES:
        runs, _ = build_configs(df, args.framework_group, args.query, args.query_variant, args.hw,
                                rate, args.metric, templates)
        a = aggregate_configs(runs)
        frames[rate] = a[a["strategy"] == args.strategy] if not a.empty else a
    fws = [f for f in FRAMEWORK_ORDER if f in FRAMEWORK_GROUPS[args.framework_group]
           and any(not frames[r].empty and f in set(frames[r]["framework"]) for r in RATE_VALUES)]
    if not fws:
        print(f"  config_eventrate: no data for strategy={args.strategy}; skipping")
        return
    for fw in fws:
        parts = [frames[r][frames[r]["framework"] == fw] for r in RATE_VALUES if not frames[r].empty]
        union = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["config_id"]) if parts else pd.DataFrame()
        if union.empty:
            continue
        ordered = assign_short_ids(union)
        fig, ax = plt.subplots(figsize=(4.4, 3.2))
        cmap = plt.get_cmap("tab10")
        plotted = 0
        for i, (_, row) in enumerate(ordered.iterrows()):
            cid = row["config_id"]
            xs, ys = [], []
            for r in RATE_VALUES:
                if frames[r].empty:
                    continue
                d = frames[r][(frames[r]["framework"] == fw) & (frames[r]["config_id"] == cid)]
                if not d.empty:
                    xs.append(r)
                    ys.append(d["metric_mean"].iloc[0])
            if len(xs) < 2:
                continue  # this config wasn't actually re-evaluated at >=2 rates; don't fabricate a line
            ax.plot(xs, ys, marker="o", color=cmap(i % 10), label=row["short_id"],
                    linewidth=1.2, markersize=4, markeredgecolor="black", markeredgewidth=0.3)
            plotted += 1
        if plotted == 0:
            plt.close(fig)
            continue
        ax.set_xticks(RATE_VALUES)
        ax.set_xticklabels([f"{int(r / 1000)}k" for r in RATE_VALUES])
        ax.set_xlabel("Event rate", fontsize=STYLE["font_axis"] - 1)
        ax.set_ylabel(METRIC_YLABEL.get(args.metric, args.metric), fontsize=STYLE["font_axis"] - 1)
        ax.set_title(f"{STRATEGY_LABEL[args.strategy]} / {FRAMEWORK_LABEL[fw]} / {args.query}\n"
                    "series = configuration (see mapping csv)", fontsize=STYLE["font_tick"] + 1)
        ax.legend(fontsize=STYLE["font_annot"], ncol=2, frameon=False, loc="best")
        ax.grid(alpha=0.3, linewidth=0.5)
        _safe_tight_layout(fig)
        base = os.path.join(out_dir, fw)
        save_figure(fig, os.path.join(base, f"eventrate_by_config_{args.strategy}"), args.formats)
        _write_config_mapping(ordered, os.path.join(base, f"eventrate_by_config_{args.strategy}_mapping.csv"))


OPERATOR_METRICS_CSV = os.path.join(HERE, "operator_level_metrics.csv")
OWN_METRIC_YLABEL = {
    "throughput_per_sec": "Operator throughput (events/s)",
    "selectivity": "Operator selectivity",
    "backpressure_ratio": "Operator backpressure ratio",
    "busy_ratio": "Operator busy ratio",
}


def fig_operator_own_metrics(df, args, out_dir, own_metric):
    """19G: x = operator (the engine's own raw names, in its own reported order), y = that
    operator's OWN metric (throughput_per_sec / selectivity / backpressure_ratio / busy_ratio,
    from 11_extract_operator_metrics.py), series = strategy. Uses the same selected/best
    objective as E (highest mean throughput_50), picked directly from query_performance_data.csv
    so it works for Kafka Streams and RisingWave too -- their CONFIGURATION is job-level, but
    these per-operator PERFORMANCE metrics are real and vary by operator for them as well."""
    if not os.path.exists(OPERATOR_METRICS_CSV):
        print(f"  19G: {OPERATOR_METRICS_CSV} not found -- run 11_extract_operator_metrics.py first")
        return
    op_df = pd.read_csv(OPERATOR_METRICS_CSV, low_memory=False)
    op_df["query_variant"] = op_df["query_variant"].fillna("")

    cell = df[(df["query"] == args.query) & (df["query_variant"] == args.query_variant)
             & (df["hardware_type"] == args.hw) & (df["producer_rate"] == args.rate)
             & (df["framework"].isin(FRAMEWORK_GROUPS[args.framework_group]))
             & (df["enumeration_strategy"].isin(STRATEGY_ORDER))]
    if cell.empty:
        print("  19G: no rows for this selection; skipping")
        return
    best = (cell.groupby(["framework", "enumeration_strategy", "parallelism"])["throughput_50"]
            .mean().reset_index()
            .sort_values("throughput_50", ascending=False)
            .groupby(["framework", "enumeration_strategy"]).head(1))

    fws = [f for f in FRAMEWORK_ORDER if f in FRAMEWORK_GROUPS[args.framework_group]
           and f in set(best["framework"])]
    if not fws:
        print("  19G: no selected configs; skipping")
        return
    fig, axes = plt.subplots(1, len(fws), figsize=(max(3.2, 2.0 * len(fws)), 3.0), squeeze=False)
    plotted_any = False
    for ax, fw in zip(axes[0], fws):
        names = None
        for s in STRATEGY_ORDER:
            row = best[(best["framework"] == fw) & (best["enumeration_strategy"] == s)]
            if row.empty:
                continue
            par = row.iloc[0]["parallelism"]
            opsub = op_df[(op_df["framework"] == fw) & (op_df["strategy"] == s)
                         & (op_df["query"] == args.query) & (op_df["query_variant"] == args.query_variant)
                         & (op_df["hardware_type"] == args.hw) & (op_df["producer_rate"] == args.rate)
                         & (op_df["parallelism"] == par)]
            if opsub.empty:
                continue
            opsub = opsub.groupby(["operator_index", "operator_raw_name"], as_index=False)[own_metric].mean()
            opsub = opsub.sort_values("operator_index")
            opsub = opsub[opsub[own_metric].notna()]
            if opsub.empty:
                continue
            if names is None:
                names = opsub["operator_raw_name"].tolist()
            st = STRATEGY_STYLE[s]
            ax.plot(np.arange(len(opsub)), opsub[own_metric], color=st["color"], marker=st["marker"],
                    linestyle=st["linestyle"], markeredgecolor="black", markeredgewidth=0.4,
                    label=STRATEGY_LABEL[s])
            plotted_any = True
        if names:
            ax.set_xticks(np.arange(len(names)))
            ax.set_xticklabels([n.title() for n in names], rotation=40, ha="right", fontsize=STYLE["font_tick"] - 1)
        else:
            ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        ax.set_ylabel(OWN_METRIC_YLABEL.get(own_metric, own_metric), fontsize=STYLE["font_axis"] - 1)
        ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_tick"] + 1)
        ax.grid(alpha=0.3, linewidth=0.5)
    if not plotted_any:
        plt.close(fig)
        print(f"  19G ({own_metric}): no operator data found for any framework; skipping")
        return
    handles, labels = axes[0, 0].get_legend_handles_labels()
    if not handles:
        for ax in axes[0]:
            handles, labels = ax.get_legend_handles_labels()
            if handles:
                break
    fig.legend(handles, labels, fontsize=STYLE["font_legend"], frameon=False, ncol=4,
              loc="upper center", bbox_to_anchor=(0.5, 1.1))
    _safe_tight_layout(fig, rect=(0, 0, 1, 0.88))
    save_figure(fig, os.path.join(out_dir, f"G_operator_own_{own_metric}"), args.formats)


def fig_framework_compare(agg, args, out_dir, layout):
    """E -- Framework x Strategy: Selected/Best Configuration.
    D1 (x = framework, grouped by strategy) or D2 (facets per framework). ONE selected
    configuration per (framework, strategy); this is not the search space (see B/C)."""
    best = select_best(agg)
    fws = [f for f in FRAMEWORK_ORDER if f in FRAMEWORK_GROUPS[args.framework_group] and f in set(best["framework"])]
    if not fws:
        print("  D: no frameworks with data; skipping")
        return
    if layout == "D1":
        width_in = max(FIGURE_SIZE[args.figure_size][0], 0.9 * len(fws) + 2.2)
        fig, ax = plt.subplots(figsize=(width_in, 3.0))
        width = 0.8 / len(STRATEGY_ORDER)
        xs = np.arange(len(fws))
        for i, s in enumerate(STRATEGY_ORDER):
            st = STRATEGY_STYLE[s]
            for j, fw in enumerate(fws):
                row = best[(best["framework"] == fw) & (best["strategy"] == s)]
                x = xs[j] + (i - (len(STRATEGY_ORDER) - 1) / 2) * width
                if row.empty:
                    ax.text(x, 0, "N/A", ha="center", va="bottom", fontsize=STYLE["font_annot"], rotation=90)
                    continue
                ax.bar(x, row["metric_mean"].iloc[0], width=width, color=st["color"], hatch=st["hatch"],
                       edgecolor="black", linewidth=0.4, label=STRATEGY_LABEL[s] if j == 0 else None)
                value = row["metric_mean"].iloc[0]
                top = best["metric_mean"].max()
                if value >= 0.12 * top:
                    ax.text(x, value / 2, _short_value(value), rotation=90, ha="center", va="center",
                            fontsize=STYLE["font_annot"], fontweight="bold", color="white")
                else:
                    ax.text(x, value, _short_value(value), rotation=90, ha="center", va="bottom",
                            fontsize=STYLE["font_annot"], fontweight="bold", color="black")
        ax.set_xticks(xs)
        ax.set_xticklabels([FRAMEWORK_LABEL[f] for f in fws], rotation=30, ha="right")
        ax.set_xlabel("Framework")
        ax.set_ylabel(f"{args.metric} (selected config)")
        ax.legend(fontsize=STYLE["font_legend"], frameon=False, ncol=len(STRATEGY_ORDER),
                  loc="lower center", bbox_to_anchor=(0.5, 1.0))
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        fig.suptitle("Framework × Strategy — Selected/Best Configuration", fontsize=STYLE["font_title"], y=1.14)
        _safe_tight_layout(fig)
        save_figure(fig, os.path.join(out_dir, "D1_framework_grouped_by_strategy"), args.formats)
    else:
        fig, axes = plt.subplots(1, len(fws), figsize=FIGURE_SIZE[args.figure_size], sharey=True, squeeze=False)
        for ax, fw in zip(axes[0], fws):
            for k, s in enumerate(STRATEGY_ORDER):
                st = STRATEGY_STYLE[s]
                row = best[(best["framework"] == fw) & (best["strategy"] == s)]
                if row.empty:
                    ax.text(k, 0, "N/A", ha="center", va="bottom", fontsize=STYLE["font_annot"], rotation=90)
                    continue
                ax.bar(k, row["metric_mean"].iloc[0], color=st["color"], hatch=st["hatch"],
                       edgecolor="black", linewidth=0.4)
            ax.set_xticks(np.arange(len(STRATEGY_ORDER)))
            ax.set_xticklabels([STRATEGY_LABEL[s] for s in STRATEGY_ORDER], rotation=40, ha="right")
            ax.grid(axis="y", alpha=0.3, linewidth=0.5)
            if not args.paper_mode:
                ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_title"])
        axes[0, 0].set_ylabel(f"{args.metric} (selected config)")
        fig.suptitle("Framework × Strategy — Selected/Best Configuration", fontsize=STYLE["font_title"], y=1.08)
        _safe_tight_layout(fig)
        save_figure(fig, os.path.join(out_dir, "D2_framework_facets"), args.formats)


def fig_job_level(agg, args, out_dir):
    """E: job-level comparison, only where semantic equivalence is recorded."""
    fws = [f for f in ["kafkastream", "risingwave"] if f in set(agg["framework"])]
    pairs_ok = len(fws) == 2 and any(
        {k[0], k[1]} == {"kafkastream", "risingwave"} and k[2] == args.query
        for k in SEMANTIC_EQUIVALENT
    )
    if not pairs_ok:
        print(f"  E: no semantic equivalence recorded for {args.query} across Kafka Streams and RisingWave; skipping")
        return
    best = select_best(agg)
    fig, ax = plt.subplots(figsize=FIGURE_SIZE[args.figure_size])
    width = 0.38
    xs = np.arange(len(STRATEGY_ORDER))
    for j, fw in enumerate(fws):
        for k, s in enumerate(STRATEGY_ORDER):
            row = best[(best["framework"] == fw) & (best["strategy"] == s)]
            x = xs[k] + (j - 0.5) * width
            if row.empty:
                ax.text(x, 0, "N/A", ha="center", va="bottom", fontsize=STYLE["font_annot"], rotation=90)
                continue
            r = row.iloc[0]
            ax.bar(x, r["metric_mean"], width=width, color=FRAMEWORK_COLOR[fw], edgecolor="black", linewidth=0.4,
                   label=FRAMEWORK_LABEL[fw] if k == 0 else None)
            ax.text(x, r["metric_mean"], r["config_label"], ha="center", va="bottom", fontsize=STYLE["font_annot"])
    ax.set_xticks(xs)
    ax.set_xticklabels([STRATEGY_LABEL[s] for s in STRATEGY_ORDER])
    ax.set_xlabel("Enumeration strategy (selected config; label = job-level value)")
    ax.set_ylabel(f"{args.metric} (selected config)")
    ax.legend(fontsize=STYLE["font_legend"], frameon=False, loc="upper right")
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    _safe_tight_layout(fig)
    save_figure(fig, os.path.join(out_dir, "E_job_level_kafka_vs_risingwave"), args.formats)
    print("  semantic check:", SEMANTIC_EQUIVALENT[("kafkastream", "risingwave", args.query)])


def fig_regression(df, args, out_dir):
    """F: Uniform-only regression against the baseline 3_ scaling helper."""
    spec = importlib.util.spec_from_file_location("baseline3", BASELINE_PATH)
    base = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base)
    sel = df[(df["framework"] == args.regression_framework) & (df["query"] == args.query)
             & (df["query_variant"] == args.query_variant) & (df["hardware_type"] == args.hw)
             & (df["producer_rate"] == args.rate) & (df["enumeration_strategy"] == "uniform")].copy()
    if sel.empty:
        print("  F: no uniform rows for regression selection; skipping")
        return
    new = sel.groupby("parallelism_avg")[args.metric].agg("mean").sort_index()
    old_raw = base._agg_series(sel.groupby("parallelism_avg")[args.metric], "mean")
    old = old_raw.set_index("parallelism_avg")[args.metric].sort_index()
    same = (list(new.index) == list(old.index)) and np.allclose(new.values, old.values, equal_nan=True)
    print(f"  F regression: uniform-only rows={len(sel)}, points={len(new)}, match={'PASS' if same else 'FAIL'}")
    os.makedirs(out_dir, exist_ok=True)
    baseline_png = os.path.join(out_dir, "F_baseline_plot_scaling_single_framework")
    base.plot_scaling_single_framework(sel, args.regression_framework, args.metric, agg="mean",
                                       title=None, save_path=baseline_png + ".png", show=False)
    print(f"  wrote {baseline_png}.png (baseline script output, filtered to uniform)")
    fig, ax = plt.subplots(figsize=FIGURE_SIZE[args.figure_size])
    ax.plot(new.index.astype(float), new.values, marker="s", color=STRATEGY_STYLE["uniform"]["color"],
            linestyle="--", markeredgecolor="black", markeredgewidth=0.4,
            label="new script (Uniform only)")
    ax.set_xlabel("Mean inner-operator parallelism (baseline: parallelism_avg)")
    ax.set_ylabel(args.metric)
    ax.legend(fontsize=STYLE["font_legend"], frameon=False)
    ax.grid(alpha=0.3, linewidth=0.5)
    _safe_tight_layout(fig)
    save_figure(fig, os.path.join(out_dir, "F_regression_uniform_new"), args.formats)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
RATE_VALUES = [100000.0, 500000.0, 1000000.0]
HW_VALUES = ["m510", "c6525-25g", "c6525-100g"]
SELECTED_LABEL = {"uniform": "Uniform-best", "random": "Random-best", "ds2": "DS2", "dhalion": "Dhalion"}
METRIC_YLABEL = {
    "throughput_50": "Throughput (events/s)",
    "endtoend_latency": "End-to-end latency (ms)",
    "cpu_50": "CPU utilisation (%)",
    "memory_50": "Memory (%)",
    "network_50": "Network (MB/s)",
    "throughput_per_sum_inner": "Throughput / sum inner parallelism",
}


def _num(v):
    return None if v is None or pd.isna(v) else float(v)


def selected_for(df, templates, group, query, variant, hw, rate, metric):
    """Selected configuration per (framework, strategy) under the throughput objective.
    `metric` is read from that same configuration (mean over its repetitions)."""
    tp_runs, _ = build_configs(df, group, query, variant, hw, rate, "throughput_50", templates)
    best = select_best(aggregate_configs(tp_runs))
    if best.empty:
        return best
    if metric == "throughput_per_sum_inner":
        return best.assign(value=best["metric_mean"] / best["sum_inner"])
    if metric == "throughput_50":
        return best.assign(value=best["metric_mean"])
    m_runs, _ = build_configs(df, group, query, variant, hw, rate, metric, templates)
    m_agg = aggregate_configs(m_runs).reindex(columns=["framework", "strategy", "config_id", "metric_mean"])
    merged = best.merge(m_agg, on=["framework", "strategy", "config_id"], how="left", suffixes=("", "_m"))
    return merged.assign(value=merged["metric_mean_m"])


def _label_bar(ax, x, v, top, width):
    if v >= 0.12 * top:
        ax.text(x, v / 2, _short_value(v), rotation=90, ha="center", va="center",
                fontsize=STYLE["font_annot"], fontweight="bold", color="white")
    else:
        ax.text(x, v, _short_value(v), rotation=90, ha="center", va="bottom",
                fontsize=STYLE["font_annot"], fontweight="bold", color="black")


def strategy_legend(fig, y=1.02):
    from matplotlib.patches import Patch
    handles = [Patch(facecolor=STRATEGY_STYLE[s]["color"], hatch=STRATEGY_STYLE[s]["hatch"],
                     edgecolor="black", linewidth=0.4, label=SELECTED_LABEL.get(s, STRATEGY_LABEL[s]))
               for s in STRATEGY_ORDER]
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.5, y),
               fontsize=STYLE["font_legend"])


def draw_strategy_bars(ax, labels, series):
    """Grouped bars: one group per label, one bar per strategy. Missing values -> N/A, never zero."""
    k, n = len(STRATEGY_ORDER), len(labels)
    width = 0.8 / k
    top = max([v for vals in series.values() for v in vals if v is not None] or [1.0])
    for i, s in enumerate(STRATEGY_ORDER):
        st = STRATEGY_STYLE[s]
        for j, v in enumerate(series.get(s, [None] * n)):
            x = j + (i - (k - 1) / 2) * width
            if v is None:
                ax.text(x, 0, "N/A", ha="center", va="bottom", fontsize=STYLE["font_annot"] - 1, rotation=90)
                continue
            ax.bar(x, v, width=width, color=st["color"], hatch=st["hatch"], edgecolor="black", linewidth=0.4)
            _label_bar(ax, x, v, top, width)
    ax.set_xticks(np.arange(n))
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=STYLE["font_tick"])
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)


def strategy_panels(panels, labels, ylabel, args, out_base):
    """One panel per framework (or per title); x = labels, bars = strategies."""
    n = len(panels)
    fig, axes = plt.subplots(1, n, figsize=(max(FIGURE_SIZE[args.figure_size][0], 1.6 * n * len(labels) ** 0.5 + 1.2), 3.2),
                             sharey=True, squeeze=False)
    for ax, (title, series) in zip(axes[0], panels):
        draw_strategy_bars(ax, labels, series)
        ax.set_title(title, fontsize=STYLE["font_tick"] + 1, pad=3)
    axes[0, 0].set_ylabel(ylabel, fontsize=STYLE["font_axis"] - 1)
    strategy_legend(fig)
    _safe_tight_layout(fig, rect=(0, 0, 1, 0.9))
    save_figure(fig, out_base, args.formats)


def fig_selected_performance(df, templates, args, out):
    """Selected-plan performance: x = Uniform-best | Random-best | DS2 | Dhalion, y = metric."""
    best = selected_for(df, templates, args.framework_group, args.query, args.query_variant,
                        args.hw, args.rate, args.metric)
    if best.empty:
        print("  no selected configurations; skipping")
        return
    fws = [f for f in FRAMEWORK_ORDER if f in set(best["framework"])]
    fig, axes = plt.subplots(1, len(fws), figsize=(max(FIGURE_SIZE[args.figure_size][0], 1.9 * len(fws) + 0.8), 3.2),
                             sharey=True, squeeze=False)
    for ax, fw in zip(axes[0], fws):
        sub = best[best["framework"] == fw].set_index("strategy")
        vals = [_num(sub.loc[s, "value"]) if s in sub.index else None for s in STRATEGY_ORDER]
        top = max([v for v in vals if v is not None] or [1.0])
        for i, (s, v) in enumerate(zip(STRATEGY_ORDER, vals)):
            st = STRATEGY_STYLE[s]
            if v is None:
                ax.text(i, 0, "N/A", ha="center", va="bottom", fontsize=STYLE["font_annot"] - 1, rotation=90)
                continue
            ax.bar(i, v, width=0.7, color=st["color"], hatch=st["hatch"], edgecolor="black", linewidth=0.4)
            _label_bar(ax, i, v, top, 0.7)
        ax.set_xticks(np.arange(len(STRATEGY_ORDER)))
        ax.set_xticklabels([SELECTED_LABEL[s] for s in STRATEGY_ORDER], rotation=30, ha="right",
                           fontsize=STYLE["font_tick"])
        ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_tick"] + 1, pad=3)
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    axes[0, 0].set_ylabel(METRIC_YLABEL.get(args.metric, args.metric), fontsize=STYLE["font_axis"] - 1)
    _safe_tight_layout(fig)
    save_figure(fig, os.path.join(out, "selected_performance"), args.formats)


def fig_strategy_by_rate(df, templates, args, out):
    """x = event rate (100k, 500k, 1M), series = strategy, one panel per framework."""
    per_fw = {}
    for r in RATE_VALUES:
        best = selected_for(df, templates, args.framework_group, args.query, args.query_variant,
                            args.hw, r, args.metric)
        for _, row in best.iterrows():
            series = per_fw.setdefault(row["framework"], {})
            series.setdefault(row["strategy"], [None] * len(RATE_VALUES))[RATE_VALUES.index(r)] = _num(row["value"])
    fws = [f for f in FRAMEWORK_ORDER if f in per_fw]
    if not fws:
        print("  no data across event rates; skipping")
        return
    panels = [(FRAMEWORK_LABEL[f], per_fw[f]) for f in fws]
    strategy_panels(panels, [f"{int(r / 1000)}k" for r in RATE_VALUES], METRIC_YLABEL.get(args.metric, args.metric),
                    args, os.path.join(out, "strategy_by_event_rate"))


def fig_strategy_by_hardware(df, templates, args, out):
    """x = hardware (m510, c6525-25g, c6525-100g), series = strategy, one panel per framework."""
    per_fw = {}
    for hw in HW_VALUES:
        best = selected_for(df, templates, args.framework_group, args.query, args.query_variant,
                            hw, args.rate, args.metric)
        for _, row in best.iterrows():
            series = per_fw.setdefault(row["framework"], {})
            series.setdefault(row["strategy"], [None] * len(HW_VALUES))[HW_VALUES.index(hw)] = _num(row["value"])
    fws = [f for f in FRAMEWORK_ORDER if f in per_fw]
    if not fws:
        print("  no data across hardware; skipping")
        return
    panels = [(FRAMEWORK_LABEL[f], per_fw[f]) for f in fws]
    strategy_panels(panels, HW_VALUES, METRIC_YLABEL.get(args.metric, args.metric), args,
                    os.path.join(out, "strategy_by_hardware"))


def fig_strategy_by_workload(df, templates, args, out):
    """x = workload (query + variant), series = strategy, one panel per framework."""
    frameworks = FRAMEWORK_GROUPS[args.framework_group]
    cells = (df[(df["framework"].isin(frameworks)) & (df["hardware_type"] == args.hw)
                & (df["producer_rate"].astype(float) == args.rate)]
             [["query", "query_variant"]].drop_duplicates().sort_values(["query", "query_variant"]))
    workloads = [(q, v) for q, v in cells.itertuples(index=False)]
    per_fw = {}
    for idx, (q, v) in enumerate(workloads):
        best = selected_for(df, templates, args.framework_group, q, v, args.hw, args.rate, args.metric)
        for _, row in best.iterrows():
            series = per_fw.setdefault(row["framework"], {})
            series.setdefault(row["strategy"], [None] * len(workloads))[idx] = _num(row["value"])
    fws = [f for f in FRAMEWORK_ORDER if f in per_fw]
    if not fws:
        print("  no data across workloads; skipping")
        return
    # Keep only workloads with at least one value for this group (no empty N/A columns).
    keep = [i for i in range(len(workloads))
            if any(s[i] is not None for fw in per_fw.values() for s in fw.values())]
    labels = [f"{workloads[i][0]} {workloads[i][1]}".strip() if workloads[i][1] else workloads[i][0] for i in keep]
    fig_w = min(7.0, max(FIGURE_SIZE[args.figure_size][0], 0.42 * len(labels) + 1.6))
    fig, axes = plt.subplots(len(fws), 1, figsize=(fig_w, 2.4 * len(fws) + 0.8), sharex=True, squeeze=False)
    for ax, fw in zip(axes[:, 0], fws):
        series = {s: [vals[i] for i in keep] for s, vals in per_fw[fw].items()}
        draw_strategy_bars(ax, labels, series)
        ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_tick"] + 1, pad=3, loc="left")
        ax.set_ylabel(METRIC_YLABEL.get(args.metric, args.metric), fontsize=STYLE["font_axis"] - 1)
    strategy_legend(fig)
    _safe_tight_layout(fig, rect=(0, 0, 1, 0.94))
    save_figure(fig, os.path.join(out, "strategy_by_workload"), args.formats)


def fig_tp_latency(df, templates, args, out):
    """Throughput vs end-to-end latency, every unique configuration, colour/marker = strategy."""
    tp_runs, _ = build_configs(df, args.framework_group, args.query, args.query_variant, args.hw, args.rate,
                               "throughput_50", templates)
    lat_runs, _ = build_configs(df, args.framework_group, args.query, args.query_variant, args.hw, args.rate,
                                "endtoend_latency", templates)
    tp = aggregate_configs(tp_runs)
    if tp.empty:
        print("  no configurations with throughput data; skipping")
        return
    lat = aggregate_configs(lat_runs).reindex(columns=["framework", "strategy", "config_id", "metric_mean"])
    merged = tp.merge(lat, on=["framework", "strategy", "config_id"], how="inner", suffixes=("", "_lat"))
    fws = [f for f in FRAMEWORK_ORDER if f in set(merged["framework"])]
    if not fws:
        print("  no configurations with both throughput and latency; skipping")
        return
    fig, axes = plt.subplots(1, len(fws), figsize=(max(FIGURE_SIZE[args.figure_size][0], 1.9 * len(fws) + 0.8), 3.2),
                             sharey=True, squeeze=False)
    for ax, fw in zip(axes[0], fws):
        sub = merged[merged["framework"] == fw]
        for s in STRATEGY_ORDER:
            d = sub[sub["strategy"] == s]
            if d.empty:
                continue
            st = STRATEGY_STYLE[s]
            ax.scatter(d["metric_mean"] / 1000, d["metric_mean_lat"], marker=st["marker"], color=st["color"],
                       edgecolor="black", linewidths=0.4, s=22, label=STRATEGY_LABEL[s])
        ax.set_yscale("log")
        ax.set_xlabel("Throughput (k events/s)", fontsize=STYLE["font_axis"] - 1)
        ax.set_title(FRAMEWORK_LABEL[fw], fontsize=STYLE["font_tick"] + 1, pad=3)
        ax.grid(alpha=0.3, linewidth=0.5)
    axes[0, 0].set_ylabel("End-to-end latency (ms, log)", fontsize=STYLE["font_axis"] - 1)
    strategy_legend(fig)
    _safe_tight_layout(fig, rect=(0, 0, 1, 0.9))
    save_figure(fig, os.path.join(out, "throughput_vs_latency"), args.formats)


NEW_MODES = {
    "selected_performance": fig_selected_performance,
    "tp_latency": fig_tp_latency,
    "strategy_rate": fig_strategy_by_rate,
    "strategy_hardware": fig_strategy_by_hardware,
    "strategy_workload": fig_strategy_by_workload,
    "tp_latency": fig_tp_latency,
}


NEW_MODES.update({
    "selected_performance": fig_selected_performance,
    "strategy_rate": fig_strategy_by_rate,
    "strategy_hardware": fig_strategy_by_hardware,
    "strategy_workload": fig_strategy_by_workload,
    "tp_latency": fig_tp_latency,
    "config_eventrate": fig_config_eventrate,
})

# Numbered output families (match the plot list: 15-27 for enumeration analysis).
FAMILY = {
    "searchspace_mean": "15_searchspace_mean_parallelism",
    "searchspace_sum": "16_searchspace_sum_parallelism",
    "config_labels": "17_configuration_label_performance",
    "config_perf_single": "17B_configuration_performance_single",
    "config_perf_compare": "17C_configuration_performance_compare",
    "config_eventrate": "03B_eventrate_by_configuration",
    "selected_performance": "18_selected_plan_performance",
    "selected": "19_selected_plan_operator_parallelism",
    "strategy_framework": "20_strategy_x_framework",
    "strategy_workload": "21_strategy_x_workload",
    "strategy_hardware": "22_strategy_x_hardware",
    "strategy_rate": "23_strategy_x_event_rate",
    "tp_latency": "25_throughput_vs_latency",
    "selected_efficiency": "26_selected_plan_efficiency",
    "job_level": "27_job_level_kafka_vs_risingwave",
    "framework_compare": "20_strategy_x_framework",
    "regression": "regression_check",
    "searchspace_mean_fallback": "15_searchspace_mean_parallelism",
}


def family_for(args):
    if args.mode == "searchspace":
        return FAMILY["searchspace_mean" if args.parallelism_summary == "mean" else "searchspace_sum"]
    if args.mode == "selected_performance" and args.metric == "throughput_per_sum_inner":
        return FAMILY["selected_efficiency"]
    if args.mode == "selected" or args.mode == "config_labels":
        return FAMILY[args.mode]
    return FAMILY.get(args.mode, args.mode)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Enumeration-strategy analysis plots (validation stage).")
    p.add_argument("--mode", choices=["searchspace", "config_labels", "selected", "framework_compare",
                                      "strategy_framework", "selected_performance", "strategy_rate",
                                      "strategy_hardware", "strategy_workload", "tp_latency",
                                      "config_perf_single", "config_perf_compare", "config_eventrate",
                                      "operator_own_metrics",
                                      "job_level", "regression", "coverage"])
    p.add_argument("--print-group", metavar="NAME", help="Print members of a framework group and exit.")
    p.add_argument("--framework-group", default="all_5", choices=list(FRAMEWORK_GROUPS))
    p.add_argument("--own-metric", default="throughput_per_sec",
                   choices=["throughput_per_sec", "selectivity", "backpressure_ratio", "busy_ratio"],
                   help="19G only: which per-operator metric to plot (from operator_level_metrics.csv).")
    p.add_argument("--strategy", choices=STRATEGY_ORDER,
                   help="Single strategy (required for config_perf_single / config_perf_compare: "
                        "mean parallelism is not a valid shared x-axis for Random/DS2/Dhalion, so "
                        "those modes plot one strategy's actual configurations at a time.)")
    p.add_argument("--query", default="Click Analytics")
    p.add_argument("--query-variant", default="time-based")
    p.add_argument("--hw", default="m510")
    p.add_argument("--rate", type=float, default=500000)
    p.add_argument("--metric", default="throughput_50")
    p.add_argument("--parallelism-summary", choices=["mean", "sum"], default="mean")
    p.add_argument("--selection-objective", choices=["throughput"], default="throughput")
    p.add_argument("--framework-layout", choices=["D1", "D2"], default="D1")
    p.add_argument("--regression-framework", default="flink")
    p.add_argument("--figure-size", choices=list(FIGURE_SIZE), default="single")
    p.add_argument("--paper-mode", action="store_true", help="Omit in-figure titles.")
    p.add_argument("--formats", default="png,pdf,svg")
    args = p.parse_args(argv)
    args.formats = [f.strip() for f in args.formats.split(",") if f.strip()]
    return args


def main(argv=None):
    args = parse_args(argv)
    if args.print_group:
        if args.print_group not in FRAMEWORK_GROUPS:
            sys.exit(f"unknown framework group: {args.print_group}")
        print(" ".join(FRAMEWORK_GROUPS[args.print_group]))
        return
    if not args.mode:
        sys.exit("--mode is required")
    apply_style(args.paper_mode)
    df = load_csv()
    templates = load_templates()
    excluded = count_excluded_uniform_1()
    print(f"uniform_1 evaluation files excluded (legacy, not an enumeration strategy): {excluded}")

    if args.mode == "regression":
        out_dir = os.path.join(OUT_ROOT, "regression")
        fig_regression(df, args, out_dir)
        return

    if args.mode == "operator_own_metrics":
        qslug = args.query.lower().replace(" ", "-")
        tail = os.path.join(qslug, args.query_variant or "novariant", args.hw, str(int(args.rate)))
        g_dir = os.path.join(PLOTS_TODATE_ROOT, "comparison", args.framework_group,
                             "19G_operator_own_metrics", tail)
        fig_operator_own_metrics(df, args, g_dir, args.own_metric)
        return

    if args.mode in NEW_MODES:
        qslug = args.query.lower().replace(" ", "-")
        base = os.path.join(OUT_ROOT, family_for(args), args.framework_group, qslug,
                            args.query_variant or "novariant", args.hw, str(int(args.rate)), args.metric)
        NEW_MODES[args.mode](df, templates, args, base)
        return

    runs, skipped = build_configs(df, args.framework_group, args.query, args.query_variant,
                                  args.hw, args.rate, args.metric, templates)
    agg = aggregate_configs(runs)
    print_coverage(runs, agg, skipped, args.framework_group, args.query, args.query_variant, args.hw, args.rate)
    if args.mode == "coverage":
        return
    if agg.empty:
        print("  no configurations matched this selection (query/variant/hardware/rate/group); skipping")
        return

    qslug = args.query.lower().replace(" ", "-")
    base_dir = os.path.join(OUT_ROOT, family_for(args), args.framework_group, qslug,
                            args.query_variant or "novariant", args.hw, str(int(args.rate)), args.metric)
    tail = os.path.join(qslug, args.query_variant or "novariant", args.hw, str(int(args.rate)), args.metric)

    if args.mode == "strategy_framework":
        ok, reason = semantics_ok(args.framework_group, args.query)
        print(f"  semantic gate: {reason}")
        if not ok:
            print("  SKIPPED: not generated (no forced comparison)")
            return
        # E spans all 4 strategies at once -- it belongs with the other cross-strategy output
        # in plots_todate/comparison/<group>/, next to the baseline's own framework comparisons,
        # not under one strategy's folder.
        e_dir = os.path.join(PLOTS_TODATE_ROOT, "comparison", args.framework_group,
                             "00_selected_best_comparison", tail)
        fig_framework_compare(agg, args, e_dir, "D1")
        return
    if args.mode == "searchspace":
        fig_searchspace(agg, args, base_dir)
    elif args.mode == "config_labels":
        fig_config_labels(agg, args, base_dir)
    elif args.mode == "selected":
        # F also spans all 4 strategies (combined view + cross-framework compare), so it lives
        # alongside E rather than under one strategy's plots_todate/<strategy>/ folder.
        f_dir = os.path.join(PLOTS_TODATE_ROOT, "comparison", args.framework_group,
                             "operator_parallelism", tail)
        fig_selected(agg, args, f_dir)
    elif args.mode == "framework_compare":
        fig_framework_compare(agg, args, base_dir, args.framework_layout)
    elif args.mode == "job_level":
        fig_job_level(agg, args, base_dir)
    elif args.mode == "config_perf_single":
        # B is per-FRAMEWORK, not per-group: Flink's own Random configs don't change depending
        # on whether the --framework-group is all_5 or flink_spark_storm, so the group is never
        # part of this path -- generating it once per (strategy, framework) and reusing it for
        # every group it appears in avoids regenerating byte-identical files under each group.
        b_dir = os.path.join(PLOTS_TODATE_ROOT, args.strategy, "01_scaling_single_framework", tail)
        fig_config_perf_single(agg, args, b_dir)
    elif args.mode == "config_perf_compare":
        c_dir = os.path.join(PLOTS_TODATE_ROOT, args.strategy, "02_scaling_compare_frameworks",
                             args.framework_group, tail)
        fig_config_perf_compare(agg, args, c_dir)


if __name__ == "__main__":
    main()
