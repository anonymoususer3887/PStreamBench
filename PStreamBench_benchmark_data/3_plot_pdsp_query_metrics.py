#!/usr/bin/env python3

import argparse
import os
import json
import re
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.container import BarContainer
from matplotlib.lines import Line2D
from matplotlib.colors import to_rgb, to_hex

# -----------------------------
# Metric & styling configuration
# -----------------------------
METRICS = [
    "event_rate_50", "event_rate_95", "event_rate_98",
    "throughput_50", "throughput_95", "throughput_98",
    "endtoend_latency",
    "cpu_50", "cpu_95", "cpu_98",
    "memory_50", "memory_95", "memory_98",
    "network_50", "network_95", "network_98",
    "cpu_efficiency",
    "network_efficiency",
]

FRAMEWORK_ALPHA = {"flink": 1.0, "storm": 0.55, "spark": 0.80, "kafkastream": 0.80, "risingwave": 0.80}

# Used by LOW/MED/HIGH across HW plots (two legends)
FRAMEWORK_ALPHA_LMH = {"flink": 1.0, "storm": 0.90, "spark": 0.75, "kafkastream": 0.75, "risingwave": 0.75}
FRAMEWORK_HATCH_LMH = {"flink": "", "storm": "xx", "spark": "..", "kafkastream": "//", "risingwave": "\\\\"}

# (Dual-axis) framework colors/linestyles
FRAMEWORK_COLORS = {
    "flink": "#1f77b4",
    "storm": "#ff7f0e",
    "spark": "#2ca02c",
    "kafkastream": "#d62728",
    "risingwave": "#9467bd",
}
FRAMEWORK_LINESTYLE = {
    "flink": "-",
    "storm": "--",
    "spark": ":",
    "kafkastream": "-.",
    "risingwave": (0, (3, 1, 1, 1)),
}

FRAMEWORK_MARKER = {
    "flink": "o",
    "storm": "s",
    "spark": "^",
    "kafkastream": "D",
    "risingwave": "v",
}

def darken_color(color: str, factor: float = 0.65) -> str:
    """
    Return a darker version of a color.
    factor < 1 => darker (e.g., 0.65 = 35% darker).
    """
    r, g, b = to_rgb(color)
    r *= factor
    g *= factor
    b *= factor
    return to_hex((r, g, b))

QUERY_COLORS = {
    "Smart Grid": "#1b9e77",
    "Word Count": "#d95f02",
    "Word Count Shuffle": "#7570b3",
    "Ad Analytics": "#e7298a",
    "Google Cloud Monitoring": "#66a61e",
    "Sentiment Analysis": "#e6ab02",
    "Spike Detection": "#a6761d",
    "Log Processing": "#1f78b4",
    "Trending Topics": "#b2df8a",
    "Bargain Index": "#fb9a99",
    "Click Analytics": "#cab2d6",
    "Click Analytics Count-based": "#fdbf6f",
    "Machine Outlier": "#a6cee3",
    "Linear Road": "#ff7f00",
    "TPCH": "#6a3d9a",
    "TPCH Window": "#b15928",
    "Traffic Monitoring": "#8dd3c7",
}

QUERY_HATCHES = {
    "Smart Grid": "",
    "Word Count": "",
    "Word Count Shuffle": "",
    "Ad Analytics": "",
    "Google Cloud Monitoring": "",
    "Sentiment Analysis": "",
    "Spike Detection": "",
    "Log Processing": "",
    "Trending Topics": "",
    "Bargain Index": "",
    "Click Analytics": "",
    "Click Analytics Count-based": "",
    "Machine Outlier": "",
    "Linear Road": "",
    "TPCH": "",
    "TPCH Window": "",
    "Traffic Monitoring": "",
}

# Hardware palettes (used across plots that color by hardware_type)
HW_PALETTES: Dict[str, Dict[str, str]] = {
    "modern": {
        "m510": "#10B981",
        "c6525-25g": "#F59E0B",
        "c6525-100g": "#6366F1",
    },
    "tableau": {
        "m510": "#4E79A7",
        "c6525-25g": "#F28E2B",
        "c6525-100g": "#59A14F",
    },
    "okabeito": {
        "m510": "#009E73",
        "c6525-25g": "#E69F00",
        "c6525-100g": "#0072B2",
    },
    "muted": {
        "m510": "#2A9D8F",
        "c6525-25g": "#E9C46A",
        "c6525-100g": "#264653",
    },
}
DEFAULT_HW_COLOR = "#6B7280"

# Will be overwritten in main() based on --hw-palette
HARDWARE_COLORS = HW_PALETTES["modern"]

HARDWARE_ORDER = ["m510", "c6525-25g", "c6525-100g"]

PARALLELISM_WHITELIST = {
    "m510": [2, 8, 16, 32, 40],
    "c6525-25g": [2, 8, 16, 32, 40, 80],
    "c6525-100g": [2, 8, 16, 32, 40, 96],
}

MAX_REL_ERR = 0.15


# -----------------------------
# job_query_number helpers
# -----------------------------
_QID_SUFFIX_RE = re.compile(r"^(?P<base>.*)_(?P<num>\d+)$")


def add_query_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      - query_base: original df['query'] as string
      - query_id:   query_base + "_" + job_query_number (if present), else just query_base
    """
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


def get_base_query_name(qid: str) -> str:
    """
    Best-effort base name extraction from a query_id string.
    If qid ends with _<digits>, strip it; else return qid.
    (When job_query_number exists, we also have df['query_base'] which is preferred.)
    """
    m = _QID_SUFFIX_RE.match(str(qid))
    return m.group("base") if m else str(qid)


def slugify(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s)).strip("_")


# -----------------------------
# Global aesthetics (SciencePlots optional)
# -----------------------------
def configure_style(mpl_style: str = "science", latex_text: bool = False, fig_w: float = 5, fig_h: float = 3):
    """
    mpl_style: science|ieee|nature|default
    latex_text: True uses LaTeX text rendering (requires LaTeX installed). If False, uses no-latex.
    """
    if mpl_style != "default":
        try:
            import scienceplots  # noqa: F401
            styles = ["science"]
            if mpl_style in ("ieee", "nature"):
                styles.insert(1, mpl_style)
            if not latex_text:
                styles.append("no-latex")
            plt.style.use(styles)
        except Exception as e:
            warnings.warn(f"SciencePlots not available/failed ({e}). Using Matplotlib default style.")
            plt.style.use("default")
    else:
        plt.style.use("default")

    plt.rcParams.update({
        "figure.figsize": (fig_w, fig_h),
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.labelsize": 12,
        "legend.fontsize": 10,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.autolayout": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "hatch.linewidth": 0.8,
    })


def add_value_labels(ax, spacing: int = 3, fontsize: int = 9, rotation: int = 90, max_bars: int = 40):
    """
    Annotate bar charts with their heights.
    Robust to matplotlib containers that are NOT bars (e.g., ErrorbarContainer).
    """
    bars = []
    for c in getattr(ax, "containers", []):
        if isinstance(c, BarContainer):
            bars.extend(list(c.patches))

    if len(bars) > max_bars:
        return

    for bar in bars:
        if bar is None or not hasattr(bar, "get_height"):
            continue

        height = bar.get_height()
        if height is None:
            continue
        try:
            if not np.isfinite(height) or height == 0:
                continue
        except Exception:
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


def order_hardware(hws: List[str]) -> List[str]:
    present = [h for h in HARDWARE_ORDER if h in hws]
    others = sorted([h for h in hws if h not in HARDWARE_ORDER])
    return present + others


# -----------------------------
# Axis scaling/limits + AUTO log ymin
# -----------------------------
def apply_y_scale(ax, y_scale: str):
    if y_scale == "linear":
        ax.set_yscale("linear")
        return

    ymin, _ymax = ax.get_ylim()
    if ymin <= 0:
        warnings.warn("Log scale requested but non-positive values exist. Using symlog instead.")
        ax.set_yscale("symlog", linthresh=1e-9)
    else:
        ax.set_yscale("log")


def apply_axis_limits(ax, ymin=None, ymax=None):
    if ymin is None and ymax is None:
        return

    cur_min, cur_max = ax.get_ylim()
    new_min = cur_min if ymin is None else float(ymin)
    new_max = cur_max if ymax is None else float(ymax)

    yscale = ax.get_yscale()
    if yscale in ("log", "symlog") and new_min <= 0:
        warnings.warn("ymin <= 0 is invalid for log scale; ignoring ymin.")
        new_min = cur_min

    if new_max <= new_min:
        warnings.warn(f"Invalid y-limits (ymin={new_min}, ymax={new_max}); ignoring.")
        return

    ax.set_ylim(new_min, new_max)


def compute_safe_log_ymin(values, factor: float = 10.0) -> Optional[float]:
    vals = np.asarray(values, dtype=float)
    vals = vals[np.isfinite(vals)]
    vals = vals[vals > 0]
    if len(vals) == 0:
        return None
    return float(np.min(vals)) / factor


def apply_auto_log_ymin(ax, y_scale: str, ymin: Optional[float], plotted_values):
    """
    If y_scale=='log' and ymin not provided, set:
        ymin = min_positive(plotted_values)/10
    """
    if y_scale != "log":
        return
    if ymin is not None:
        return

    auto_ymin = compute_safe_log_ymin(plotted_values, factor=10.0)
    if auto_ymin is None:
        warnings.warn("Log scale requested but no positive plotted values exist; skipping auto-ymin.")
        return

    _cur_min, cur_max = ax.get_ylim()
    ax.set_ylim(auto_ymin, cur_max)


# -----------------------------
# Helpers: errors, efficiency
# -----------------------------
def compute_capped_errors(values: np.ndarray, std: np.ndarray, max_rel: float = MAX_REL_ERR) -> np.ndarray:
    vals = np.asarray(values, dtype=float)
    stds = np.asarray(std, dtype=float)
    stds = np.where(np.isnan(stds), 0.0, stds)
    caps = np.abs(vals) * max_rel
    errs = np.minimum(stds, caps)
    errs = np.where(np.isnan(errs), 0.0, errs)
    return errs


def add_efficiency_metrics(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["throughput_50", "cpu_50", "network_50"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return df

    cpu_den = df["cpu_50"].replace(0, pd.NA)
    net_den = df["network_50"].replace(0, pd.NA)

    df = df.copy()
    df["cpu_efficiency"] = df["throughput_50"] / cpu_den
    df["network_efficiency"] = df["throughput_50"] / net_den
    return df


def apply_parallelism_whitelist(df: pd.DataFrame) -> pd.DataFrame:
    if "hardware_type" not in df.columns or "parallelism_avg" not in df.columns:
        return df

    mask_keep = pd.Series(True, index=df.index)
    for hw, allowed in PARALLELISM_WHITELIST.items():
        hw_mask = df["hardware_type"] == hw
        if not hw_mask.any():
            continue
        allowed_mask = df["parallelism_avg"].isin(allowed)
        mask_keep &= (~hw_mask) | allowed_mask

    return df[mask_keep]


# -----------------------------
# Data loading & filtering
# -----------------------------
def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "framework" in df.columns:
        df["framework"] = df["framework"].astype(str).str.lower()
    return df


def filter_data(
    df: pd.DataFrame,
    query: Optional[str] = None,
    hardware_type: Optional[str] = None,
    frameworks: Optional[List[str]] = None,
    producer_rate: Optional[int] = None,
    parallelism_avg: Optional[int] = None,
) -> pd.DataFrame:
    sub = df.copy()

    # query filtering: accept query_id or base query name
    if query is not None:
        if "query_id" in sub.columns and (sub["query_id"].astype(str) == str(query)).any():
            sub = sub[sub["query_id"].astype(str) == str(query)]
        elif "query_base" in sub.columns:
            sub = sub[sub["query_base"].astype(str) == str(query)]
        else:
            sub = sub[sub["query"].astype(str) == str(query)]

    if hardware_type is not None and hardware_type != "":
        sub = sub[sub["hardware_type"] == hardware_type]

    if frameworks:
        frameworks = [f.lower() for f in frameworks]
        sub = sub[sub["framework"].isin(frameworks)]

    if producer_rate is not None:
        sub = sub[sub["producer_rate"] == producer_rate]

    if parallelism_avg is not None and "parallelism_avg" in sub.columns:
        sub = sub[sub["parallelism_avg"] == parallelism_avg]

    return sub


def limit_points_df(df: pd.DataFrame, x_col: str, max_points: Optional[int], strategy: str = "even") -> pd.DataFrame:
    """
    Reduce number of distinct x points for dense plots.
    Works on the filtered DF BEFORE aggregation.
    """
    if max_points is None or max_points <= 0 or df.empty:
        return df
    if x_col not in df.columns:
        return df

    xs = sorted(df[x_col].dropna().unique().tolist())
    if len(xs) <= max_points:
        return df

    if strategy == "head":
        keep = xs[:max_points]
    elif strategy == "tail":
        keep = xs[-max_points:]
    else:
        idx = np.linspace(0, len(xs) - 1, num=max_points)
        idx = np.unique(np.round(idx).astype(int))
        keep = [xs[i] for i in idx]

    return df[df[x_col].isin(keep)]


# -----------------------------
# File helpers
# -----------------------------
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

    # bbox_inches="tight" expands the saved canvas to fit everything
    # (title, legend, labels) instead of clipping at the figure's nominal
    # size -- fixes titles/legends getting cut off at the edges.
    fig.savefig(root + ".png", dpi=300, bbox_inches="tight")
    if all_formats:
        fig.savefig(root + ".pdf", bbox_inches="tight")
        fig.savefig(root + ".svg", bbox_inches="tight")


# -----------------------------
# Plot primitives
# -----------------------------
def plot_series(
    ax,
    x_labels: List[str],
    y_values: np.ndarray,
    chart: str,
    label: Optional[str] = None,
    color: Optional[str] = None,
    alpha: float = 1.0,
    hatch: str = "",
    y_err: Optional[np.ndarray] = None,
    linestyle: str = "-",
):
    x = np.arange(len(x_labels))

    if chart == "bar":
        bars = ax.bar(
            x,
            y_values,
            yerr=y_err,
            capsize=3 if y_err is not None else 0,
            label=label,
            color=color,
            edgecolor="black",
            hatch=hatch,
            alpha=alpha,
        )
        return x, bars

    # line
    if y_err is not None:
        ax.errorbar(
            x, y_values,
            yerr=y_err,
            marker="o",
            linestyle=linestyle,
            label=label,
            alpha=alpha,
            color=color,
            capsize=3,
        )
    else:
        ax.plot(
            x, y_values,
            marker="o",
            linestyle=linestyle,
            label=label,
            alpha=alpha,
            color=color,
        )
    return x, None


# -----------------------------
# Aggregation helpers
# -----------------------------
def _agg_series(grouped: pd.core.groupby.generic.SeriesGroupBy, agg: str) -> pd.DataFrame:
    if agg == "mean":
        return grouped.mean().reset_index()
    if agg == "median":
        return grouped.median().reset_index()
    if agg == "max":
        return grouped.max().reset_index()
    if agg == "min":
        return grouped.min().reset_index()
    raise ValueError(f"Unsupported agg='{agg}'")


def _agg_stats(grouped: pd.core.groupby.generic.SeriesGroupBy, agg: str) -> Tuple[pd.DataFrame, str]:
    if agg == "mean":
        stats = grouped.agg(["mean", "std"]).reset_index()
        return stats, "mean"
    if agg == "median":
        stats = grouped.agg(["median", "std"]).reset_index()
        return stats, "median"
    if agg == "max":
        stats = grouped.agg(["max", "std"]).reset_index()
        return stats, "max"
    if agg == "min":
        stats = grouped.agg(["min", "std"]).reset_index()
        return stats, "min"
    raise ValueError(f"Unsupported agg='{agg}'")


# =============================================================================
# DUAL-AXIS TP+LATENCY (MERGED)
# =============================================================================
def plot_dual_axis_tp_latency_compare_frameworks(
    df: pd.DataFrame,
    x_dim: str,
    throughput_metric: str = "throughput_50",
    latency_metric: str = "endtoend_latency",
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    y2_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
    y2min: Optional[float] = None,
    y2max: Optional[float] = None,
):
    if df.empty:
        raise ValueError("No data to plot (df is empty).")

    if x_dim not in {"parallelism_avg", "producer_rate", "hardware_type"}:
        raise ValueError("x_dim must be one of: parallelism_avg | producer_rate | hardware_type")

    for c in ["framework", x_dim]:
        if c not in df.columns:
            raise ValueError(f"DataFrame must contain '{c}' for dual-axis plot.")
    for m in [throughput_metric, latency_metric]:
        if m not in df.columns:
            raise ValueError(f"Metric '{m}' not found in DataFrame.")

    tp_stats, tp_valcol = _agg_stats(df.groupby(["framework", x_dim])[throughput_metric], agg)
    lat_stats, lat_valcol = _agg_stats(df.groupby(["framework", x_dim])[latency_metric], agg)

    tp_pivot = tp_stats.pivot(index=x_dim, columns="framework", values=tp_valcol)
    tp_std = tp_stats.pivot(index=x_dim, columns="framework", values="std").reindex(tp_pivot.index)

    lat_pivot = lat_stats.pivot(index=x_dim, columns="framework", values=lat_valcol).reindex(tp_pivot.index)
    lat_std = lat_stats.pivot(index=x_dim, columns="framework", values="std").reindex(tp_pivot.index)

    # X ordering
    if x_dim == "hardware_type":
        x_vals = order_hardware([str(x) for x in tp_pivot.index.tolist()])
        tp_pivot = tp_pivot.reindex(x_vals)
        tp_std = tp_std.reindex(x_vals)
        lat_pivot = lat_pivot.reindex(x_vals)
        lat_std = lat_std.reindex(x_vals)
        x_labels = x_vals
    else:
        x_vals = sorted(tp_pivot.index.tolist())
        tp_pivot = tp_pivot.reindex(x_vals)
        tp_std = tp_std.reindex(x_vals)
        lat_pivot = lat_pivot.reindex(x_vals)
        lat_std = lat_std.reindex(x_vals)
        x_labels = [str(x) for x in x_vals]

    frameworks = [str(fw).lower() for fw in tp_pivot.columns.tolist()]
    # stable ordering: flink, storm, then others
    frameworks = [fw for fw in ["flink", "storm"] if fw in frameworks] + [fw for fw in frameworks if fw not in ("flink", "storm")]
    if not frameworks:
        raise ValueError("No frameworks found after aggregation.")

    x = np.arange(len(x_labels))
    fig, ax1 = plt.subplots(figsize=(5, 3))

    qid = None
    qbase = None
    if "query_id" in df.columns and df["query_id"].notna().any():
        uniq = df["query_id"].dropna().astype(str).unique().tolist()
        qid = uniq[0] if len(uniq) == 1 else uniq[0]
    if "query_base" in df.columns and df["query_base"].notna().any():
        qbase = df["query_base"].dropna().astype(str).iloc[0]
    else:
        qbase = get_base_query_name(qid) if qid else None

    base_color = QUERY_COLORS.get(qbase, "#333333") if qbase else "#333333"
    line_color = darken_color(base_color, factor=0.60)  # darker for visibility

    ax2 = ax1.twinx()

    num_fw = len(frameworks)
    bar_width = 0.8 / max(1, num_fw)

    plotted_tp_all = []
    plotted_lat_all = []

    for i, fw in enumerate(frameworks):
        if fw not in tp_pivot.columns:
            continue

        # Bars: query color + framework alpha/hatch -- alpha alone collides
        # once >2 frameworks share a value (e.g. spark/kafkastream/
        # risingwave all default to 0.80), so hatch is the real
        # per-framework visual differentiator here (color still encodes
        # the query, matching this file's convention everywhere else).
        bar_color = base_color
        alpha = FRAMEWORK_ALPHA.get(fw, 0.85)
        fw_hatch = FRAMEWORK_HATCH_LMH.get(fw, "")

        # Lines: darker query color + framework linestyle/marker
        ls = FRAMEWORK_LINESTYLE.get(fw, "-")
        mk = FRAMEWORK_MARKER.get(fw, "o")

        tp_vals = tp_pivot[fw].to_numpy(dtype=float)
        tp_errs = compute_capped_errors(tp_vals, tp_std[fw].to_numpy(dtype=float))
        plotted_tp_all.append(tp_vals)

        x_pos = x + (i - (num_fw - 1) / 2) * bar_width
        ax1.bar(
            x_pos,
            tp_vals,
            width=bar_width,
            yerr=tp_errs,
            capsize=3,
            color=bar_color,
            edgecolor="black",
            hatch=fw_hatch,
            alpha=alpha,
            label=None,
        )

        lat_vals = lat_pivot[fw].to_numpy(dtype=float)
        lat_errs = compute_capped_errors(lat_vals, lat_std[fw].to_numpy(dtype=float))
        plotted_lat_all.append(lat_vals)

        ax2.errorbar(
            x,
            lat_vals,
            yerr=lat_errs,
            marker=mk,
            linestyle=ls,
            color=line_color,
            alpha=alpha,                 # keep your framework alpha differentiation
            linewidth=2.2,
            markersize=6,
            markerfacecolor=line_color,
            markeredgecolor="black",
            markeredgewidth=0.6,
            capsize=3,
            label=None,
            zorder=5,
        )

    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels)

    ax1.set_xlabel(x_dim)
    ax1.set_ylabel(throughput_metric)
    ax2.set_ylabel(latency_metric)

    q_label = None
    if "query_id" in df.columns and df["query_id"].notna().any():
        uniq = df["query_id"].dropna().astype(str).unique().tolist()
        q_label = uniq[0] if len(uniq) == 1 else f"{get_base_query_name(uniq[0])} (multiple jobs)"

    if title is None:
        prefix = f"{q_label} | " if q_label else ""
        title = f"{prefix}{throughput_metric} (bars) + {latency_metric} (lines) vs {x_dim} ({agg})"
        #title = f"{prefix}"
    # Pushes the title above the two legend boxes -- the legend boxes are
    # anchored (via bbox_to_anchor) at y=1.02 with loc="lower left"/"lower
    # right", meaning that's their BOTTOM edge, so the box itself extends
    # upward past y=1.25; the title needs to clear that extent, not just
    # the anchor point.
    ax1.set_title(title, y=1.55)

    ax1.grid(axis="y", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax2.spines["top"].set_visible(False)

    # Two legends: throughput bars (left), latency lines (right)
    bar_handles = [
        Patch(
            facecolor=base_color,
            edgecolor="black",
            hatch=FRAMEWORK_HATCH_LMH.get(fw, ""),
            alpha=FRAMEWORK_ALPHA.get(fw, 0.85),
            label=fw,
        )
        for fw in frameworks
    ]

    line_handles = [
        Line2D(
            [0], [0],
            color=line_color,
            linestyle=FRAMEWORK_LINESTYLE.get(fw, "-"),
            marker=FRAMEWORK_MARKER.get(fw, "o"),
            linewidth=2.2,
            markersize=6,
            markerfacecolor=line_color,
            markeredgecolor="black",
            markeredgewidth=0.6,
            alpha=FRAMEWORK_ALPHA.get(fw, 0.85),
            label=fw,
        )
        for fw in frameworks
    ]
    # Place both legends above the axes (not "upper left"/"upper right"
    # *inside* the axes) -- with dense bar/line data spanning the full
    # plot width, an in-axes legend box sits directly on top of the middle
    # data points and hides them.
    leg1 = ax1.legend(handles=bar_handles, title="Throughput (bars)",
                       loc="lower left", bbox_to_anchor=(0.0, 1.02), frameon=True)
    ax1.add_artist(leg1)
    ax2.legend(handles=line_handles, title="Latency (lines)",
               loc="lower right", bbox_to_anchor=(1.0, 1.02), frameon=True)

    if show_values:
        add_value_labels(ax1, spacing=3, fontsize=8, rotation=90, max_bars=250)

    tp_all = np.concatenate(plotted_tp_all) if plotted_tp_all else np.array([])
    lat_all = np.concatenate(plotted_lat_all) if plotted_lat_all else np.array([])

    apply_y_scale(ax1, y_scale)
    apply_auto_log_ymin(ax1, y_scale, ymin, tp_all)
    apply_axis_limits(ax1, ymin=ymin, ymax=ymax)

    apply_y_scale(ax2, y2_scale)
    apply_auto_log_ymin(ax2, y2_scale, y2min, lat_all)
    apply_axis_limits(ax2, ymin=y2min, ymax=y2max)

    # rect reserves the top ~30% of the figure for the title + the two
    # above-axes legends -- plain tight_layout() doesn't know about
    # artists placed outside the axes via bbox_to_anchor and will
    # re-expand the axes to fill that space, pushing the title back up
    # into the legends.
    plt.tight_layout(rect=[0, 0, 1, 0.70])
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# =============================================================================
# LOW/MED/HIGH across hardware (NEW family)
# =============================================================================
def parse_level_map_json(s: Optional[str], name: str) -> Dict[str, Dict[str, int]]:
    """
    Accepts:
      {"m510":[low,med,high], ...} OR {"m510":{"low":..,"medium":..,"high":..}, ...}
    Legacy:
      {"m510":[low,high]} OR {"m510":{"low":..,"high":..}}
    Returns:
      { hw: {"low":int, "high":int, "medium":int(optional)} }
    """
    if not s:
        return {}

    try:
        obj = json.loads(s)
    except Exception as e:
        raise SystemExit(f"Failed to parse {name}: {e}")

    out: Dict[str, Dict[str, int]] = {}
    for hw, v in obj.items():
        if isinstance(v, (list, tuple)):
            if len(v) == 3:
                out[hw] = {"low": int(v[0]), "medium": int(v[1]), "high": int(v[2])}
            elif len(v) == 2:
                out[hw] = {"low": int(v[0]), "high": int(v[1])}
            else:
                raise SystemExit(f"{name}: '{hw}' must be [low,medium,high] or [low,high]. Got: {v}")
        elif isinstance(v, dict):
            if "low" not in v or "high" not in v:
                raise SystemExit(f"{name}: '{hw}' dict must have low/high. Got: {v}")
            out[hw] = {"low": int(v["low"]), "high": int(v["high"])}
            if "medium" in v:
                out[hw]["medium"] = int(v["medium"])
        else:
            raise SystemExit(f"{name}: '{hw}' must map to list/tuple or dict. Got: {v}")
    return out


def autofill_medium_from_df(
    df: pd.DataFrame,
    hardware_types: List[str],
    level_map: Dict[str, Dict[str, int]],
    col: str,
) -> Dict[str, Tuple[int, int, int]]:
    """
    Ensures each hardware has (low, medium, high) values for the specified column (col).
    If medium missing:
      - prefer values strictly between low and high (median)
      - else closest to midpoint among available
      - else midpoint rounded
    """
    out: Dict[str, Tuple[int, int, int]] = {}

    for hw in hardware_types:
        if hw not in level_map:
            raise ValueError(f"Hardware '{hw}' missing from mapping for '{col}'")

        low = int(level_map[hw]["low"])
        high = int(level_map[hw]["high"])

        if "medium" in level_map[hw]:
            med = int(level_map[hw]["medium"])
            out[hw] = (low, med, high)
            continue

        vals = (
            df.loc[df["hardware_type"] == hw, col]
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        vals = sorted(set(vals))
        midpoint = (low + high) / 2.0

        between = [v for v in vals if min(low, high) < v < max(low, high)]
        if between:
            med = int(np.median(between))
        elif vals:
            med = min(vals, key=lambda v: abs(v - midpoint))
        else:
            med = int(round(midpoint))

        out[hw] = (low, med, high)

    return out


def _aggregate_metric_lmh(df_pts: pd.DataFrame, metric: str, agg: str) -> pd.DataFrame:
    group_cols = ["hardware_type", "framework", "x_value"]
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
    return stats


def _plot_low_med_high_generic(
    stats: pd.DataFrame,
    metric: str,
    frameworks: List[str],
    hardware_types: List[str],
    level_map: Dict[str, Tuple[int, int, int]],
    xlabel: str,
    title: str,
    save_path: Optional[str],
    show: bool,
    all_formats: bool,
    show_values: bool,
    show_group_values: bool,
):
    x = np.arange(3)  # LOW, MED, HIGH
    x_groups = ["LOW", "MED", "HIGH"]

    series = [(fw, hw) for fw in frameworks for hw in hardware_types]
    bars_per_group = len(series)
    bar_width = 0.8 / max(1, bars_per_group)

    fig, ax = plt.subplots(figsize=(5, 3))

    for s_idx, (fw, hw) in enumerate(series):
        offset = (s_idx - (bars_per_group - 1) / 2) * bar_width
        low_v, med_v, high_v = level_map[hw]

        def _get_val(v: int) -> float:
            row = stats[
                (stats["hardware_type"] == hw) &
                (stats["framework"] == fw) &
                (stats["x_value"] == v)
            ]
            return float(row["val"].iloc[0]) if not row.empty else np.nan

        vals = np.array([_get_val(low_v), _get_val(med_v), _get_val(high_v)], dtype=float)

        color = HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR)
        alpha = FRAMEWORK_ALPHA_LMH.get(fw, 0.9)
        hatch = FRAMEWORK_HATCH_LMH.get(fw, "")

        bars = ax.bar(
            x + offset,
            np.nan_to_num(vals, nan=0.0),
            width=bar_width,
            color=color,
            alpha=alpha,
            edgecolor="#111827",
            hatch=hatch,
        )
        for b, v in zip(bars, vals):
            if np.isnan(v):
                b.set_alpha(0.15)

    ax.set_xticks(x)
    if show_group_values:
        low_desc = ", ".join([f"{hw}:{level_map[hw][0]}" for hw in hardware_types])
        med_desc = ", ".join([f"{hw}:{level_map[hw][1]}" for hw in hardware_types])
        high_desc = ", ".join([f"{hw}:{level_map[hw][2]}" for hw in hardware_types])
        ax.set_xticklabels([f"LOW\n({low_desc})", f"MED\n({med_desc})", f"HIGH\n({high_desc})"])
    else:
        ax.set_xticklabels(x_groups)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(metric)
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.25)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Two legends: hardware (color) and framework (hatch)
    hw_handles = [
        Patch(facecolor=HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR), edgecolor="#111827", label=hw, hatch="")
        for hw in hardware_types
    ]
    fw_handles = [
        Patch(facecolor="white", edgecolor="#111827", label=fw, hatch=FRAMEWORK_HATCH_LMH.get(fw, ""))
        for fw in frameworks
    ]

    leg1 = ax.legend(handles=hw_handles, title="Hardware (color)", loc="upper left", frameon=True)
    ax.add_artist(leg1)
    ax.legend(handles=fw_handles, title="Framework (hatch)", loc="upper right", frameon=True)

    if show_values:
        add_value_labels(ax, spacing=3, fontsize=11, rotation=90, max_bars=300)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# A) Parallelism-grouped (one plot per producer_rate)
def plot_low_med_high_parallelism_across_hardware(
    df: pd.DataFrame,
    query: str,  # query_id
    metric: str,
    producer_rate: int,
    frameworks: List[str],
    hardware_types: List[str],
    par_map: Dict[str, Tuple[int, int, int]],
    agg: str,
    save_path: Optional[str],
    show: bool,
    all_formats: bool,
    show_values: bool,
    show_group_values: bool,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    df_sel = df[
        (df["query_id"] == query) &
        (df["producer_rate"] == producer_rate) &
        (df["framework"].isin(frameworks)) &
        (df["hardware_type"].isin(hardware_types))
    ].copy()

    pieces = []
    for hw in hardware_types:
        low, med, high = par_map[hw]
        df_hw = df_sel[
            (df_sel["hardware_type"] == hw) &
            (df_sel["parallelism_avg"].isin([low, med, high]))
        ].copy()
        if not df_hw.empty:
            df_hw["x_value"] = df_hw["parallelism_avg"].astype(int)
            pieces.append(df_hw)

    if not pieces:
        raise ValueError("No rows matched low/med/high parallelism selections.")

    df_pts = pd.concat(pieces, ignore_index=True)
    stats = _aggregate_metric_lmh(df_pts, metric, agg)

    #title = f"{query} | rate={producer_rate} | {metric} | LOW/MED/HIGH"
    title = f"{query} | rate={producer_rate}"
    _plot_low_med_high_generic(
        stats=stats,
        metric=metric,
        frameworks=frameworks,
        hardware_types=hardware_types,
        level_map=par_map,
        xlabel="parallelism group",
        title=title,
        save_path=save_path,
        show=show,
        all_formats=all_formats,
        show_values=show_values,
        show_group_values=show_group_values,
    )


def batch_low_med_high_parallelism_across_hw(
    df: pd.DataFrame,
    frameworks: List[str],
    hardware_types: List[str],
    par_map: Dict[str, Tuple[int, int, int]],
    out_dir: str,
    agg: str,
    show: bool,
    all_formats: bool,
    show_values: bool,
    show_group_values: bool,
    metrics: Optional[List[str]] = None,
):
    metrics = metrics or METRICS
    queries = sorted(df["query_id"].dropna().unique().tolist())

    for q in queries:
        df_q = df[df["query_id"] == q]
        rates = sorted(df_q["producer_rate"].dropna().unique().tolist())
        for metric in metrics:
            if metric not in df.columns:
                continue
            for r in rates:
                save_base = os.path.join(out_dir, "low_med_high_parallelism_across_hw", slugify(q), metric, f"rate{int(r)}")
                print(f"[INFO] {q} | {metric} | rate={int(r)} -> {save_base}")
                try:
                    plot_low_med_high_parallelism_across_hardware(
                        df=df, query=q, metric=metric, producer_rate=int(r),
                        frameworks=frameworks, hardware_types=hardware_types, par_map=par_map,
                        agg=agg, save_path=save_base, show=show, all_formats=all_formats,
                        show_values=show_values, show_group_values=show_group_values,
                    )
                except Exception as e:
                    print(f"[WARN] Skipping {q} | {metric} | rate={int(r)}: {e}")


# B) Event-rate-grouped (one plot per parallelism_avg)
def plot_low_med_high_eventrate_across_hardware(
    df: pd.DataFrame,
    query: str,  # query_id
    metric: str,
    parallelism: int,
    frameworks: List[str],
    hardware_types: List[str],
    rate_map: Dict[str, Tuple[int, int, int]],
    agg: str,
    save_path: Optional[str],
    show: bool,
    all_formats: bool,
    show_values: bool,
    show_group_values: bool,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    df_sel = df[
        (df["query_id"] == query) &
        (df["parallelism_avg"] == parallelism) &
        (df["framework"].isin(frameworks)) &
        (df["hardware_type"].isin(hardware_types))
    ].copy()

    if df_sel.empty:
        raise ValueError(f"No data for query={query}, par={parallelism}")

    pieces = []
    for hw in hardware_types:
        low, med, high = rate_map[hw]
        df_hw = df_sel[
            (df_sel["hardware_type"] == hw) &
            (df_sel["producer_rate"].isin([low, med, high]))
        ].copy()
        if not df_hw.empty:
            df_hw["x_value"] = df_hw["producer_rate"].astype(int)
            pieces.append(df_hw)

    if not pieces:
        raise ValueError("No rows matched low/med/high event-rate selections.")

    df_pts = pd.concat(pieces, ignore_index=True)
    stats = _aggregate_metric_lmh(df_pts, metric, agg)

    title = f"{query} | par={parallelism} | {metric}"
    _plot_low_med_high_generic(
        stats=stats,
        metric=metric,
        frameworks=frameworks,
        hardware_types=hardware_types,
        level_map=rate_map,
        xlabel="event-rate group (producer_rate)",
        title=title,
        save_path=save_path,
        show=show,
        all_formats=all_formats,
        show_values=show_values,
        show_group_values=show_group_values,
    )


def batch_low_med_high_eventrate_across_hw(
    df: pd.DataFrame,
    frameworks: List[str],
    hardware_types: List[str],
    rate_map: Dict[str, Tuple[int, int, int]],
    out_dir: str,
    agg: str,
    show: bool,
    all_formats: bool,
    show_values: bool,
    show_group_values: bool,
    metrics: Optional[List[str]] = None,
):
    metrics = metrics or METRICS
    queries = sorted(df["query_id"].dropna().unique().tolist())

    for q in queries:
        df_q = df[df["query_id"] == q]
        pars = sorted(df_q["parallelism_avg"].dropna().unique().tolist())
        for metric in metrics:
            if metric not in df.columns:
                continue
            for p in pars:
                save_base = os.path.join(out_dir, "low_med_high_eventrate_across_hw", slugify(q), metric, f"par{int(p)}")
                print(f"[INFO] {q} | {metric} | par={int(p)} -> {save_base}")
                try:
                    plot_low_med_high_eventrate_across_hardware(
                        df=df, query=q, metric=metric, parallelism=int(p),
                        frameworks=frameworks, hardware_types=hardware_types, rate_map=rate_map,
                        agg=agg, save_path=save_base, show=show, all_formats=all_formats,
                        show_values=show_values, show_group_values=show_group_values,
                    )
                except Exception as e:
                    print(f"[WARN] Skipping {q} | {metric} | par={int(p)}: {e}")


# =============================================================================
# plot functions to use query_id + query_base
# =============================================================================
def plot_scaling_single_framework(
    df: pd.DataFrame,
    framework: str,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    df_fw = df[df["framework"] == framework]
    if df_fw.empty:
        raise ValueError(f"No data for framework={framework} after filtering.")
    if metric not in df_fw.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")

    grouped = _agg_series(df_fw.groupby("parallelism_avg")[metric], agg).sort_values("parallelism_avg")

    fig, ax = plt.subplots()

    qid = df_fw["query_id"].iloc[0] if "query_id" in df_fw.columns and not df_fw["query_id"].isna().all() else None
    qbase = df_fw["query_base"].iloc[0] if "query_base" in df_fw.columns and not df_fw["query_base"].isna().all() else (get_base_query_name(qid) if qid else None)

    color = QUERY_COLORS.get(qbase, "#333333") if qbase else "#333333"
    hatch = QUERY_HATCHES.get(qbase, "") if qbase else ""

    x_labels = grouped["parallelism_avg"].astype(str).tolist()
    y_vals = grouped[metric].to_numpy(dtype=float)

    plot_series(
        ax, x_labels=x_labels, y_values=y_vals, chart=chart,
        label=framework if chart == "line" else None,
        color=color, alpha=1.0, hatch=hatch, y_err=None,
    )

    ax.set_xlabel("parallelism_avg")
    ax.set_ylabel(metric)
    ax.set_xticks(np.arange(len(x_labels)))
    ax.set_xticklabels(x_labels)

    if title is None:
        title = f"{(qid + ' | ') if qid else ''}{framework}: {metric} vs parallelism_avg ({agg})"
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)

    if chart == "bar" and show_values:
        add_value_labels(ax, spacing=3, fontsize=9, max_bars=40)

    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, y_vals)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    if chart == "line":
        ax.legend(title="series")

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_scaling_compare_frameworks(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in DataFrame.")
    if "framework" not in df.columns or "parallelism_avg" not in df.columns:
        raise ValueError("DataFrame must contain 'framework' and 'parallelism_avg'.")

    stats, value_col = _agg_stats(df.groupby(["framework", "parallelism_avg"])[metric], agg)
    pivot_val = stats.pivot(index="parallelism_avg", columns="framework", values=value_col).sort_index()
    pivot_std = stats.pivot(index="parallelism_avg", columns="framework", values="std").loc[pivot_val.index]

    par_values = list(pivot_val.index)
    frameworks = list(pivot_val.columns)

    # Explicit, wider figure -- the rcParams default (5,3) clips the
    # auto-generated title once there are 3 frameworks in the legend text
    # instead of 2 (confirmed visually: title text got cut off at top).
    fig, ax = plt.subplots(figsize=(7, 4))

    qid = df["query_id"].iloc[0] if "query_id" in df.columns and not df["query_id"].isna().all() else None
    qbase = df["query_base"].iloc[0] if "query_base" in df.columns and not df["query_base"].isna().all() else (get_base_query_name(qid) if qid else None)

    base_color = QUERY_COLORS.get(qbase, "#333333") if qbase else "#333333"
    hatch = QUERY_HATCHES.get(qbase, "") if qbase else ""

    x_labels = [str(p) for p in par_values]
    x = np.arange(len(x_labels))

    plotted_vals_all = []

    if chart == "bar":
        num_fw = len(frameworks)
        bar_width = 0.8 / max(1, num_fw)

        for i, fw in enumerate(frameworks):
            vals = pivot_val[fw].values.astype(float)
            stds = pivot_std[fw].values.astype(float)
            errs = compute_capped_errors(vals, stds)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            fw_hatch = FRAMEWORK_HATCH_LMH.get(str(fw).lower(), "")
            x_pos = x + (i - (num_fw - 1) / 2) * bar_width

            ax.bar(
                x_pos, vals, width=bar_width, yerr=errs, capsize=3,
                label=str(fw), color=base_color, edgecolor="black", hatch=fw_hatch, alpha=alpha
            )

        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)

        if show_values:
            add_value_labels(ax, spacing=3, fontsize=9, max_bars=40)

    else:
        linestyle_map = {"flink": "-", "storm": "--"}
        for fw in frameworks:
            vals = pivot_val[fw].values.astype(float)
            stds = pivot_std[fw].values.astype(float)
            errs = compute_capped_errors(vals, stds)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            ls = FRAMEWORK_LINESTYLE.get(str(fw).lower(), linestyle_map.get(str(fw).lower(), "-"))

            plot_series(
                ax, x_labels=x_labels, y_values=vals, chart="line",
                label=str(fw), color=base_color, alpha=alpha, hatch="",
                y_err=errs, linestyle=ls
            )

        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)

    ax.set_xlabel("parallelism_avg")
    ax.set_ylabel(metric)

    if title is None:
        fw_label = " vs ".join(str(fw).capitalize() for fw in frameworks)
        title = f"{(qid + ': ') if qid else ''}{metric} vs parallelism_avg ({fw_label}, {agg})"
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="framework")

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_eventrate_single_framework(
    df: pd.DataFrame,
    framework: str,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    df_fw = df[df["framework"] == framework]
    if df_fw.empty:
        raise ValueError(f"No data for framework={framework} after filtering.")
    if metric not in df_fw.columns:
        raise ValueError(f"Metric '{metric}' not found.")
    if "producer_rate" not in df_fw.columns:
        raise ValueError("DataFrame must contain 'producer_rate'.")

    grouped = _agg_series(df_fw.groupby("producer_rate")[metric], agg).sort_values("producer_rate")

    fig, ax = plt.subplots()

    qid = df_fw["query_id"].iloc[0] if "query_id" in df_fw.columns and not df_fw["query_id"].isna().all() else None
    qbase = df_fw["query_base"].iloc[0] if "query_base" in df_fw.columns and not df_fw["query_base"].isna().all() else (get_base_query_name(qid) if qid else None)

    color = QUERY_COLORS.get(qbase, "#333333") if qbase else "#333333"
    hatch = QUERY_HATCHES.get(qbase, "") if qbase else ""

    x_labels = grouped["producer_rate"].astype(str).tolist()
    y_vals = grouped[metric].to_numpy(dtype=float)

    plot_series(
        ax, x_labels=x_labels, y_values=y_vals, chart=chart,
        label=framework if chart == "line" else None,
        color=color, alpha=1.0, hatch=hatch, y_err=None,
    )

    ax.set_xlabel("producer_rate")
    ax.set_ylabel(metric)

    hw = df_fw["hardware_type"].iloc[0] if "hardware_type" in df_fw.columns and not df_fw["hardware_type"].isna().all() else None
    par = df_fw["parallelism_avg"].iloc[0] if "parallelism_avg" in df_fw.columns and not df_fw["parallelism_avg"].isna().all() else None

    if title is None:
        parts = []
        if qid: parts.append(qid)
        if hw: parts.append(hw)
        if par is not None: parts.append(f"par={par}")
        parts.append(f"{framework}: {metric} vs event rate ({agg})")
        title = " | ".join(parts)
    ax.set_title(title)

    ax.set_xticks(np.arange(len(x_labels)))
    ax.set_xticklabels(x_labels)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    if chart == "bar" and show_values:
        add_value_labels(ax, spacing=3, fontsize=9, max_bars=40)

    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, y_vals)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    if chart == "line":
        ax.legend(title="series")

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_eventrate_compare_frameworks(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found.")
    if "framework" not in df.columns or "producer_rate" not in df.columns:
        raise ValueError("DataFrame must contain 'framework' and 'producer_rate'.")

    stats, value_col = _agg_stats(df.groupby(["framework", "producer_rate"])[metric], agg)
    pivot_val = stats.pivot(index="producer_rate", columns="framework", values=value_col).sort_index()
    pivot_std = stats.pivot(index="producer_rate", columns="framework", values="std").loc[pivot_val.index]

    rate_values = list(pivot_val.index)
    frameworks = list(pivot_val.columns)

    fig, ax = plt.subplots()

    qid = df["query_id"].iloc[0] if "query_id" in df.columns and not df["query_id"].isna().all() else None
    qbase = df["query_base"].iloc[0] if "query_base" in df.columns and not df["query_base"].isna().all() else (get_base_query_name(qid) if qid else None)

    hw = df["hardware_type"].iloc[0] if "hardware_type" in df.columns and not df["hardware_type"].isna().all() else None
    par = df["parallelism_avg"].iloc[0] if "parallelism_avg" in df.columns and not df["parallelism_avg"].isna().all() else None

    base_color = QUERY_COLORS.get(qbase, "#333333") if qbase else "#333333"
    hatch = QUERY_HATCHES.get(qbase, "") if qbase else ""

    x_labels = [str(r) for r in rate_values]
    x = np.arange(len(x_labels))

    plotted_vals_all = []

    if chart == "bar":
        num_fw = len(frameworks)
        bar_width = 0.8 / max(1, num_fw)

        for i, fw in enumerate(frameworks):
            vals = pivot_val[fw].values.astype(float)
            stds = pivot_std[fw].values.astype(float)
            errs = compute_capped_errors(vals, stds)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            fw_hatch = FRAMEWORK_HATCH_LMH.get(str(fw).lower(), "")
            x_pos = x + (i - (num_fw - 1) / 2) * bar_width

            ax.bar(
                x_pos, vals, width=bar_width, yerr=errs, capsize=3,
                label=str(fw), color=base_color, edgecolor="black", hatch=fw_hatch, alpha=alpha
            )

        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)

        if show_values:
            add_value_labels(ax, spacing=3, fontsize=9, max_bars=40)

    else:
        linestyle_map = {"flink": "-", "storm": "--"}
        for fw in frameworks:
            vals = pivot_val[fw].values.astype(float)
            stds = pivot_std[fw].values.astype(float)
            errs = compute_capped_errors(vals, stds)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            ls = FRAMEWORK_LINESTYLE.get(str(fw).lower(), linestyle_map.get(str(fw).lower(), "-"))

            plot_series(
                ax, x_labels=x_labels, y_values=vals, chart="line",
                label=str(fw), color=base_color, alpha=alpha, hatch="",
                y_err=errs, linestyle=ls
            )

        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)

    ax.set_xlabel("producer_rate")
    ax.set_ylabel(metric)

    if title is None:
        parts = []
        if qid: parts.append(qid)
        if hw: parts.append(hw)
        if par is not None: parts.append(f"par={par}")
        parts.append(f"{metric} vs event rate (Flink vs Storm, {agg})")
        title = " | ".join(parts)
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="framework")

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_queries_single_framework(
    df: pd.DataFrame,
    framework: str,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    df_fw = df[df["framework"] == framework]
    if df_fw.empty:
        raise ValueError(f"No data for framework={framework} after filtering.")
    if metric not in df_fw.columns:
        raise ValueError(f"Metric '{metric}' not found.")

    # group by query_id (NOT base)
    grouped = _agg_series(df_fw.groupby("query_id")[metric], agg).sort_values("query_id")

    # mapping query_id -> base query for consistent colors
    qid_to_base = {}
    if "query_id" in df_fw.columns and "query_base" in df_fw.columns:
        qid_to_base = dict(df_fw[["query_id", "query_base"]].drop_duplicates().values)

    fig, ax = plt.subplots(figsize=(9, 4))

    x_labels = grouped["query_id"].tolist()
    y_vals = grouped[metric].to_numpy(dtype=float)
    x = np.arange(len(x_labels))

    if chart == "bar":
        bars = ax.bar(x, y_vals)
        for bar, qid in zip(bars, x_labels):
            qbase = qid_to_base.get(qid, get_base_query_name(qid))
            bar.set_edgecolor("black")
            bar.set_color(QUERY_COLORS.get(qbase, "#333333"))
            bar.set_hatch(QUERY_HATCHES.get(qbase, ""))
        if show_values:
            add_value_labels(ax, spacing=2, fontsize=7, max_bars=60)
    else:
        ax.plot(x, y_vals, marker="o", linestyle="-", label=framework)
        ax.legend(title="framework")

    ax.set_xlabel("query_id")
    ax.set_ylabel(metric)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha="right")

    if title is None:
        title = f"{framework}: {metric} by query_id ({agg})"
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, y_vals)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_queries_compare_frameworks(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found.")
    if "framework" not in df.columns or "query_id" not in df.columns:
        raise ValueError("DataFrame must contain 'framework' and 'query_id'.")

    grouped = _agg_series(df.groupby(["query_id", "framework"])[metric], agg)
    pivot = grouped.pivot(index="query_id", columns="framework", values=metric).sort_index()

    queries = list(pivot.index)         # query_id list
    frameworks = list(pivot.columns)

    # mapping query_id -> base
    qid_to_base = {}
    if "query_base" in df.columns:
        qid_to_base = dict(df[["query_id", "query_base"]].drop_duplicates().values)

    fig, ax = plt.subplots(figsize=(9, 4))
    x_labels = queries
    x = np.arange(len(x_labels))

    plotted_vals_all = []

    if chart == "bar":
        num_fw = len(frameworks)
        bar_width = 0.8 / max(1, num_fw)

        for i, fw in enumerate(frameworks):
            vals = pivot[fw].values.astype(float)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            fw_hatch = FRAMEWORK_HATCH_LMH.get(str(fw).lower(), "")
            x_pos = x + (i - (num_fw - 1) / 2) * bar_width
            bars = ax.bar(x_pos, vals, width=bar_width, label=str(fw), alpha=alpha)

            for bar, qid in zip(bars, queries):
                qbase = qid_to_base.get(qid, get_base_query_name(qid))
                bar.set_edgecolor("black")
                bar.set_color(QUERY_COLORS.get(qbase, "#333333"))
                # framework hatch (not query hatch, which is empty for
                # every query -- see QUERY_HATCHES) is the real
                # differentiator here since color already encodes query
                bar.set_hatch(fw_hatch)

        if show_values:
            add_value_labels(ax, spacing=2, fontsize=7, max_bars=120)

        ax.legend(title="framework")

    else:
        linestyle_map = {"flink": "-", "storm": "--"}
        for fw in frameworks:
            vals = pivot[fw].values.astype(float)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            ls = FRAMEWORK_LINESTYLE.get(str(fw).lower(), linestyle_map.get(str(fw).lower(), "-"))
            mk = FRAMEWORK_MARKER.get(str(fw).lower(), "o")
            ax.plot(x, vals, marker=mk, linestyle=ls, label=str(fw), alpha=alpha)

        ax.legend(title="framework")

    ax.set_xlabel("query_id")
    ax.set_ylabel(metric)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45, ha="right")

    if title is None:
        title = f"{metric} by query_id (framework comparison, {agg})"
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# -----------------------------
# Batch functions (script-3) - updated to iterate query_id
# -----------------------------
def batch_compare_frameworks_over_event_rates(
    df: pd.DataFrame,
    metric: str,
    agg: str,
    out_dir: str,
    show: bool,
    all_formats: bool,
    chart: str,
    y_scale: str,
    show_values: bool,
    ymin: Optional[float],
    ymax: Optional[float],
):
    if df.empty:
        raise ValueError("No data left for batch plotting.")

    query_values = df["query_id"].dropna().unique().tolist()
    hw_values = df["hardware_type"].dropna().unique().tolist()

    for q in query_values:
        for hw in hw_values:
            df_q_hw = df[(df["query_id"] == q) & (df["hardware_type"] == hw)]
            if df_q_hw.empty:
                continue

            producer_rates = sorted(df_q_hw["producer_rate"].dropna().unique().tolist())
            for pr in producer_rates:
                df_q_hw_pr = df_q_hw[df_q_hw["producer_rate"] == pr]
                if df_q_hw_pr["framework"].nunique() < 2:
                    print(f"[WARN] Only one framework for {q}, {hw}, pr={pr}, skipping.")
                    continue

                title = f"{q} | {hw} | rate={pr} | {metric}"
                save_base = os.path.join(out_dir, "compare_flink_storm", slugify(q), hw, f"{pr}_{metric}")
                print(f"[INFO] Plotting {title} -> {save_base}")

                plot_scaling_compare_frameworks(
                    df_q_hw_pr,
                    metric=metric, agg=agg, title=title, save_path=save_base,
                    show=show, all_formats=all_formats,
                    chart=chart, y_scale=y_scale, show_values=show_values,
                    ymin=ymin, ymax=ymax
                )


def batch_compare_frameworks_over_all_metrics_and_event_rates(
    df: pd.DataFrame,
    agg: str,
    out_dir: str,
    show: bool,
    all_formats: bool,
    chart: str,
    y_scale: str,
    show_values: bool,
    ymin: Optional[float],
    ymax: Optional[float],
):
    if df.empty:
        raise ValueError("No data left for batch plotting.")

    query_values = df["query_id"].dropna().unique().tolist()
    hw_values = df["hardware_type"].dropna().unique().tolist()

    for q in query_values:
        for hw in hw_values:
            df_q_hw = df[(df["query_id"] == q) & (df["hardware_type"] == hw)]
            if df_q_hw.empty:
                continue

            producer_rates = sorted(df_q_hw["producer_rate"].dropna().unique().tolist())
            for metric in METRICS:
                if metric not in df_q_hw.columns:
                    print(f"[WARN] Metric '{metric}' missing for {q}, {hw}, skipping.")
                    continue

                for pr in producer_rates:
                    df_slice = df_q_hw[df_q_hw["producer_rate"] == pr]
                    if df_slice["framework"].nunique() < 2:
                        print(f"[WARN] Only one framework for {q}, {hw}, rate={pr}, metric={metric}, skipping.")
                        continue

                    title = f"{q} | {hw} | rate={pr} | {metric}"
                    save_base = os.path.join(out_dir, "compare_flink_storm_all_metrics", slugify(q), hw, metric, f"{pr}")
                    print(f"[INFO] Plotting {title} -> {save_base}")

                    plot_scaling_compare_frameworks(
                        df_slice,
                        metric=metric, agg=agg, title=title, save_path=save_base,
                        show=show, all_formats=all_formats,
                        chart=chart, y_scale=y_scale, show_values=show_values,
                        ymin=ymin, ymax=ymax
                    )


def batch_compare_low_high_parallelism(
    df: pd.DataFrame,
    low_par: int,
    high_par: int,
    agg: str,
    out_dir: str,
    show: bool,
    all_formats: bool,
    chart: str,
    y_scale: str,
    show_values: bool,
    ymin: Optional[float],
    ymax: Optional[float],
):
    if df.empty:
        raise ValueError("No data left for batch plotting.")

    query_values = df["query_id"].dropna().unique().tolist()
    hw_values = df["hardware_type"].dropna().unique().tolist()

    for q in query_values:
        for hw in hw_values:
            df_q_hw = df[(df["query_id"] == q) & (df["hardware_type"] == hw)]
            if df_q_hw.empty:
                continue

            producer_rates = sorted(df_q_hw["producer_rate"].dropna().unique().tolist())

            for metric in METRICS:
                if metric not in df_q_hw.columns:
                    print(f"[WARN] Metric '{metric}' missing for {q}, {hw}, skipping.")
                    continue

                for pr in producer_rates:
                    df_slice = df_q_hw[
                        (df_q_hw["producer_rate"] == pr) &
                        (df_q_hw["parallelism_avg"].isin([low_par, high_par]))
                    ]

                    if df_slice["framework"].nunique() < 2:
                        print(f"[WARN] Only one framework for {q}, {hw}, rate={pr}, metric={metric}, skipping.")
                        continue

                    if not set([low_par, high_par]).issubset(set(df_slice["parallelism_avg"].unique())):
                        print(f"[WARN] Missing low/high parallelism for {q}, {hw}, rate={pr}, metric={metric}, skipping.")
                        continue

                    title = f"{q} | {hw} | rate={pr} | par={low_par} vs {high_par} | {metric}"
                    save_base = os.path.join(out_dir, "compare_low_high_parallelism", slugify(q), hw, metric, f"par{low_par}-{high_par}_rate{pr}")
                    print(f"[INFO] Plotting {title} -> {save_base}")

                    plot_scaling_compare_frameworks(
                        df_slice,
                        metric=metric, agg=agg, title=title, save_path=save_base,
                        show=show, all_formats=all_formats,
                        chart=chart, y_scale=y_scale, show_values=show_values,
                        ymin=ymin, ymax=ymax
                    )


# -----------------------------
# App-level plots (all HW + all FW)
# -----------------------------
def plot_app_scaling_all_hw(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found.")
    required = {"framework", "hardware_type", "parallelism_avg"}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame must contain {required}.")

    qid = df["query_id"].iloc[0] if "query_id" in df.columns and not df["query_id"].isna().all() else None

    hw_values = order_hardware(sorted(df["hardware_type"].dropna().unique().tolist()))
    fw_values = sorted(df["framework"].dropna().unique().tolist())
    if not hw_values or not fw_values:
        raise ValueError("No hardware/frameworks found in filtered DataFrame.")

    par_values = sorted(df["parallelism_avg"].dropna().unique().tolist())
    x_labels = [str(p) for p in par_values]
    x = np.arange(len(x_labels))

    stats, value_col = _agg_stats(df.groupby(["hardware_type", "framework", "parallelism_avg"])[metric], agg)
    val_map, std_map = {}, {}
    for _, row in stats.iterrows():
        key = (row["hardware_type"], row["framework"], row["parallelism_avg"])
        val_map[key] = float(row[value_col])
        std_map[key] = float(row["std"]) if not pd.isna(row["std"]) else 0.0

    fig, ax = plt.subplots(figsize=(8, 4))

    plotted_vals_all = []

    if chart == "bar":
        num_groups = len(hw_values) * len(fw_values)
        bar_width = 0.8 / max(1, num_groups)
        group_index = 0

        for hw in hw_values:
            for fw in fw_values:
                vals, stds = [], []
                for par in par_values:
                    key = (hw, fw, par)
                    vals.append(val_map.get(key, 0.0))
                    stds.append(std_map.get(key, 0.0))

                vals = np.array(vals, dtype=float)
                stds = np.array(stds, dtype=float)
                errs = compute_capped_errors(vals, stds)
                plotted_vals_all.append(vals)

                offset = (group_index - (num_groups - 1) / 2) * bar_width
                color = HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR)
                alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
                fw_hatch = FRAMEWORK_HATCH_LMH.get(str(fw).lower(), "")

                ax.bar(
                    x + offset, vals, width=bar_width, yerr=errs, capsize=3,
                    label=f"{hw} | {fw}", color=color, edgecolor="black", hatch=fw_hatch, alpha=alpha
                )
                group_index += 1

        if show_values:
            add_value_labels(ax, spacing=3, fontsize=7, max_bars=120)

    else:
        linestyle_map = {"flink": "-", "storm": "--"}
        for hw in hw_values:
            color = HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR)
            for fw in fw_values:
                vals, stds = [], []
                for par in par_values:
                    key = (hw, fw, par)
                    vals.append(val_map.get(key, np.nan))
                    stds.append(std_map.get(key, 0.0))
                vals = np.array(vals, dtype=float)
                stds = np.array(stds, dtype=float)
                errs = compute_capped_errors(np.nan_to_num(vals, nan=0.0), stds)
                plotted_vals_all.append(np.nan_to_num(vals, nan=np.nan))

                alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
                ls = FRAMEWORK_LINESTYLE.get(str(fw).lower(), linestyle_map.get(str(fw).lower(), "-"))
                mk = FRAMEWORK_MARKER.get(str(fw).lower(), "o")

                ax.errorbar(
                    x, vals, yerr=errs, marker=mk, linestyle=ls,
                    color=color, alpha=alpha, capsize=3, label=f"{hw} | {fw}"
                )

    ax.set_xlabel("parallelism_avg")
    ax.set_ylabel(metric)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)

    if title is None:
        title = f"{(qid + ' | ') if qid else ''}{metric} vs parallelism (all hardware, {agg})"
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="hardware | framework", fontsize=8, ncol=2)

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_app_eventrate_all_hw(
    df: pd.DataFrame,
    metric: str,
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found.")
    required = {"framework", "hardware_type", "producer_rate"}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame must contain {required}.")

    qid = df["query_id"].iloc[0] if "query_id" in df.columns and not df["query_id"].isna().all() else None

    hw_values = order_hardware(sorted(df["hardware_type"].dropna().unique().tolist()))
    fw_values = sorted(df["framework"].dropna().unique().tolist())
    if not hw_values or not fw_values:
        raise ValueError("No hardware/frameworks found in filtered DataFrame.")

    rate_values = sorted(df["producer_rate"].dropna().unique().tolist())
    x_labels = [str(r) for r in rate_values]
    x = np.arange(len(x_labels))

    stats, value_col = _agg_stats(df.groupby(["hardware_type", "framework", "producer_rate"])[metric], agg)
    val_map, std_map = {}, {}
    for _, row in stats.iterrows():
        key = (row["hardware_type"], row["framework"], row["producer_rate"])
        val_map[key] = float(row[value_col])
        std_map[key] = float(row["std"]) if not pd.isna(row["std"]) else 0.0

    fig, ax = plt.subplots(figsize=(8, 4))

    plotted_vals_all = []

    if chart == "bar":
        num_groups = len(hw_values) * len(fw_values)
        bar_width = 0.8 / max(1, num_groups)
        group_index = 0

        for hw in hw_values:
            for fw in fw_values:
                vals, stds = [], []
                for r in rate_values:
                    key = (hw, fw, r)
                    vals.append(val_map.get(key, 0.0))
                    stds.append(std_map.get(key, 0.0))

                vals = np.array(vals, dtype=float)
                stds = np.array(stds, dtype=float)
                errs = compute_capped_errors(vals, stds)
                plotted_vals_all.append(vals)

                offset = (group_index - (num_groups - 1) / 2) * bar_width
                color = HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR)
                alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
                fw_hatch = FRAMEWORK_HATCH_LMH.get(str(fw).lower(), "")

                ax.bar(
                    x + offset, vals, width=bar_width, yerr=errs, capsize=3,
                    label=f"{hw} | {fw}", color=color, edgecolor="black", hatch=fw_hatch, alpha=alpha
                )
                group_index += 1

        if show_values:
            add_value_labels(ax, spacing=3, fontsize=7, max_bars=120)

    else:
        linestyle_map = {"flink": "-", "storm": "--"}
        for hw in hw_values:
            color = HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR)
            for fw in fw_values:
                vals, stds = [], []
                for r in rate_values:
                    key = (hw, fw, r)
                    vals.append(val_map.get(key, np.nan))
                    stds.append(std_map.get(key, 0.0))

                vals = np.array(vals, dtype=float)
                stds = np.array(stds, dtype=float)
                errs = compute_capped_errors(np.nan_to_num(vals, nan=0.0), stds)
                plotted_vals_all.append(np.nan_to_num(vals, nan=np.nan))

                alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
                ls = FRAMEWORK_LINESTYLE.get(str(fw).lower(), linestyle_map.get(str(fw).lower(), "-"))
                mk = FRAMEWORK_MARKER.get(str(fw).lower(), "o")

                ax.errorbar(
                    x, vals, yerr=errs, marker=mk, linestyle=ls,
                    color=color, alpha=alpha, capsize=3, label=f"{hw} | {fw}"
                )

    ax.set_xlabel("producer_rate")
    ax.set_ylabel(metric)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)

    if title is None:
        title = f"{(qid + ' | ') if qid else ''}{metric} vs event rate (all hardware, {agg})"
    ax.set_title(title)

    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="hardware | framework", fontsize=8, ncol=2)

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# -----------------------------
# Efficiency plot across hardware (unchanged logic; query_id-aware title)
# -----------------------------
def plot_cpu_efficiency_across_hardware(
    df: pd.DataFrame,
    query: str,  # query_id OR base (filtering supports base matching if needed)
    frameworks: List[str],
    par_map: dict,
    producer_rate: int,
    metric: str = "cpu_efficiency",
    agg: str = "mean",
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    chart: str = "bar",
    y_scale: str = "linear",
    show_values: bool = False,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if metric not in df.columns:
        raise ValueError(f"Metric {metric} not found. Did you compute efficiency metrics?")

    # query can be base or qid
    if "query_id" in df.columns and (df["query_id"].astype(str) == str(query)).any():
        df_q = df[(df["query_id"] == query) & (df["producer_rate"] == producer_rate)]
        query_label = query
    else:
        base = str(query)
        df_q = df[(df["query_base"] == base) & (df["producer_rate"] == producer_rate)]
        query_label = base

    if df_q.empty:
        raise ValueError(f"No data for query={query} + rate={producer_rate}")

    mask = pd.Series(False, index=df_q.index)
    for hw, par in par_map.items():
        mask |= ((df_q["hardware_type"] == hw) & (df_q["parallelism_avg"] == par))
    df_sel = df_q[mask]
    if df_sel.empty:
        raise ValueError(f"No rows match par_map for query={query_label}, rate={producer_rate}")

    stats, value_col = _agg_stats(df_sel.groupby(["hardware_type", "framework"])[metric], agg)
    if frameworks:
        frameworks = [f.lower() for f in frameworks]
        stats["framework"] = stats["framework"].astype(str).str.lower()
        stats = stats[stats["framework"].isin(frameworks)]
    if stats.empty:
        raise ValueError("No data left after restricting frameworks.")

    hw_list = order_hardware(sorted(stats["hardware_type"].unique().tolist()))
    fw_list = sorted(stats["framework"].unique().tolist())

    val_pivot = stats.pivot(index="hardware_type", columns="framework", values=value_col).loc[hw_list]
    std_pivot = stats.pivot(index="hardware_type", columns="framework", values="std").loc[hw_list]

    x_labels = hw_list
    x = np.arange(len(x_labels))

    fig, ax = plt.subplots()
    plotted_vals_all = []

    if chart == "bar":
        num_fw = len(fw_list)
        bar_width = 0.8 / max(1, num_fw)

        for i, fw in enumerate(fw_list):
            vals = val_pivot[fw].values.astype(float)
            stds = std_pivot[fw].values.astype(float)
            errs = compute_capped_errors(vals, stds)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            fw_hatch = FRAMEWORK_HATCH_LMH.get(str(fw).lower(), "")
            fw_color = FRAMEWORK_COLORS.get(str(fw).lower())
            x_pos = x + (i - (num_fw - 1) / 2) * bar_width

            ax.bar(
                x_pos, vals, width=bar_width, yerr=errs, capsize=3,
                label=str(fw), color=fw_color, edgecolor="black", hatch=fw_hatch, alpha=alpha
            )

        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)

        if show_values:
            add_value_labels(ax, spacing=4, max_bars=40)

    else:
        linestyle_map = {"flink": "-", "storm": "--"}
        for fw in fw_list:
            vals = val_pivot[fw].values.astype(float)
            stds = std_pivot[fw].values.astype(float)
            errs = compute_capped_errors(vals, stds)
            plotted_vals_all.append(vals)

            alpha = FRAMEWORK_ALPHA.get(str(fw).lower(), 0.8)
            ls = FRAMEWORK_LINESTYLE.get(str(fw).lower(), linestyle_map.get(str(fw).lower(), "-"))
            mk = FRAMEWORK_MARKER.get(str(fw).lower(), "o")
            fw_color = FRAMEWORK_COLORS.get(str(fw).lower())
            ax.errorbar(x, vals, yerr=errs, marker=mk, linestyle=ls, color=fw_color, alpha=alpha, capsize=3, label=str(fw))

        ax.set_xticks(x)
        ax.set_xticklabels(x_labels)

    ax.set_xlabel("Hardware")
    ax.set_ylabel(metric)
    if title is None:
        title = f"{query_label}: {metric} across hardware (rate={producer_rate})"
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="Framework")

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# -----------------------------
# Intro plots (accept query_id or base; base expands to all qids)
# -----------------------------
def _expand_intro_queries(df: pd.DataFrame, queries: List[str]) -> List[str]:
    """
    If user passes base query names for intro plots, expand to all matching query_id.
    If query already matches a query_id, keep it as-is.
    """
    if "query_id" not in df.columns or "query_base" not in df.columns:
        return queries

    all_qids = set(df["query_id"].dropna().astype(str).unique().tolist())
    out = []
    for q in queries:
        q = str(q)
        if q in all_qids:
            out.append(q)
        else:
            matches = df.loc[df["query_base"].astype(str) == q, "query_id"].dropna().astype(str).unique().tolist()
            out.extend(matches)
    # stable unique
    seen = set()
    res = []
    for qid in out:
        if qid not in seen:
            res.append(qid)
            seen.add(qid)
    return res


def plot_intro_hw_scaling_multiqueries(
    df: pd.DataFrame,
    queries: List[str],   # query_id list (expanded)
    framework: str,
    metric: str,
    producer_rate: int,
    agg: str = "mean",
    par_levels: Optional[List[int]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if par_levels is None:
        par_levels = [8, 16, 32]

    df_fw = df[(df["framework"] == framework) & (df["producer_rate"] == producer_rate)]
    if df_fw.empty:
        raise ValueError(f"No data for fw={framework}, rate={producer_rate}")

    queries = _expand_intro_queries(df_fw, queries)
    queries_present = [q for q in queries if q in df_fw["query_id"].astype(str).unique()]
    if not queries_present:
        raise ValueError("None of the requested queries exist in df")

    n = len(queries_present)
    fig, axes = plt.subplots(1, n, figsize=(4.2 * n, 3.5), sharey=False)
    if n == 1:
        axes = [axes]

    for ax, qid in zip(axes, queries_present):
        df_q = df_fw[(df_fw["query_id"] == qid) & (df_fw["parallelism_avg"].isin(par_levels))]
        if df_q.empty:
            ax.set_title(qid)
            ax.text(0.5, 0.5, "no data", ha="center", va="center")
            continue

        grouped = _agg_series(df_q.groupby(["hardware_type", "parallelism_avg"])[metric], agg).rename(columns={metric: "val"})

        for hw in order_hardware(sorted(grouped["hardware_type"].unique())):
            sub = grouped[grouped["hardware_type"] == hw].sort_values("parallelism_avg")
            ax.plot(
                sub["parallelism_avg"],
                sub["val"],
                marker="o",
                linestyle="-",
                label=hw,
                color=HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR),
            )

        ax.set_title(qid)
        ax.set_xlabel("Parallelism")
        ax.grid(axis="y", linestyle="--", alpha=0.4)

        vals_here = grouped["val"].to_numpy(dtype=float)
        apply_y_scale(ax, y_scale)
        apply_auto_log_ymin(ax, y_scale, ymin, vals_here)
        apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    axes[0].set_ylabel(metric)
    fig.suptitle(title or f"{framework}: {metric} scaling across HW", fontsize=13)

    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=len(h), title="Hardware")

    plt.tight_layout(rect=[0, 0, 1, 0.85])
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_intro_hw_scaling_multi_fw(
    df: pd.DataFrame,
    queries: List[str],   # query_id list (expanded)
    frameworks: List[str],
    metric: str,
    producer_rate: int,
    agg: str = "mean",
    par_levels: Optional[List[int]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if par_levels is None:
        par_levels = [8, 16, 32]

    frameworks = [fw.lower() for fw in frameworks]
    df = df.copy()
    queries = _expand_intro_queries(df, queries)

    df_sel = df[
        (df["producer_rate"] == producer_rate) &
        (df["framework"].str.lower().isin(frameworks)) &
        (df["query_id"].isin(queries)) &
        (df["parallelism_avg"].isin(par_levels))
    ]
    if df_sel.empty:
        raise ValueError("No data for requested intro-multi selection.")

    group_cols = ["query_id", "framework", "hardware_type", "parallelism_avg"]
    grouped = _agg_series(df_sel.groupby(group_cols)[metric], agg).rename(columns={metric: "val"})

    queries_present = [q for q in queries if q in grouped["query_id"].unique()]
    frameworks_present = [fw for fw in frameworks if fw in grouped["framework"].unique()]
    if not queries_present or not frameworks_present:
        raise ValueError("No data left after filtering intro-multi.")

    n_rows = len(frameworks_present)
    n_cols = len(queries_present)

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(4.2 * n_cols, 3.0 * n_rows + 0.5),
        sharex=True,
    )
    if n_rows == 1 and n_cols == 1:
        axes = np.array([[axes]])
    elif n_rows == 1:
        axes = np.array([axes])
    elif n_cols == 1:
        axes = axes.reshape(n_rows, 1)

    for i, fw in enumerate(frameworks_present):
        for j, qid in enumerate(queries_present):
            ax = axes[i, j]
            sub = grouped[(grouped["framework"] == fw) & (grouped["query_id"] == qid)]
            if sub.empty:
                ax.set_title(qid)
                ax.text(0.5, 0.5, "no data", ha="center", va="center")
                continue

            for hw in order_hardware(sorted(sub["hardware_type"].unique())):
                sub_hw = sub[sub["hardware_type"] == hw].sort_values("parallelism_avg")
                ax.plot(
                    sub_hw["parallelism_avg"],
                    sub_hw["val"],
                    marker="o",
                    linestyle="-",
                    label=hw,
                    color=HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR),
                )

            if i == 0:
                ax.set_title(qid)
            if i == n_rows - 1:
                ax.set_xlabel("Parallelism")

            ax.grid(axis="y", linestyle="--", alpha=0.4)
            if j == 0:
                ax.set_ylabel(f"{fw.capitalize()} — {metric}")

            vals_here = sub["val"].to_numpy(dtype=float)
            apply_y_scale(ax, y_scale)
            apply_auto_log_ymin(ax, y_scale, ymin, vals_here)
            apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(handles), title="Hardware")
    fig.suptitle(title or f"{metric} vs parallelism | rate={producer_rate}", fontsize=13)

    plt.tight_layout(rect=[0, 0, 1, 0.85])
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


def plot_workload_fw_hw_parallelism(
    df: pd.DataFrame,
    query: str,  # query_id
    frameworks: List[str],
    metric: str,
    producer_rate: int,
    agg: str = "mean",
    par_levels: Optional[List[int]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
    show: bool = True,
    all_formats: bool = False,
    y_scale: str = "linear",
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
):
    if par_levels is None:
        par_levels = [2, 8, 16, 32, 40]

    frameworks = [fw.lower() for fw in frameworks]
    hw_fixed = ["m510", "c6525-25g", "c6525-100g"]

    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found.")

    df_sel = df[
        (df["query_id"] == query)
        & (df["producer_rate"] == producer_rate)
        & (df["framework"].str.lower().isin(frameworks))
        & (df["hardware_type"].isin(hw_fixed))
        & (df["parallelism_avg"].isin(par_levels))
    ]
    if df_sel.empty:
        raise ValueError("No data for workload_fw_hw_parallelism selection.")

    grouped = _agg_series(df_sel.groupby(["framework", "hardware_type", "parallelism_avg"])[metric], agg).rename(columns={metric: "val"})
    par_values = sorted(grouped["parallelism_avg"].unique())

    fig, ax = plt.subplots(figsize=(7, 4))
    linestyle_map = {"flink": "-", "storm": "--"}

    plotted_vals_all = []

    for fw in sorted(grouped["framework"].unique()):
        fw_lc = str(fw).lower()
        ls = FRAMEWORK_LINESTYLE.get(fw_lc, linestyle_map.get(fw_lc, "-"))
        mk = FRAMEWORK_MARKER.get(fw_lc, "o")
        alpha = FRAMEWORK_ALPHA.get(fw_lc, 0.9)

        for hw in hw_fixed:
            sub = grouped[(grouped["framework"] == fw) & (grouped["hardware_type"] == hw)].sort_values("parallelism_avg")
            if sub.empty:
                continue

            plotted_vals_all.append(sub["val"].to_numpy(dtype=float))

            ax.plot(
                sub["parallelism_avg"],
                sub["val"],
                marker=mk,
                linestyle=ls,
                label=f"{fw.capitalize()} | {hw}",
                color=HARDWARE_COLORS.get(hw, DEFAULT_HW_COLOR),
                alpha=alpha,
            )

    ax.set_xlabel("Parallelism")
    ax.set_ylabel(metric)
    ax.set_xticks(par_values)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    if title is None:
        title = f"{query}: {metric} vs parallelism | rate={producer_rate}"
    ax.set_title(title)

    ax.legend(title="Framework | Hardware", ncol=2, fontsize=9)

    plotted_vals_all = np.concatenate(plotted_vals_all) if plotted_vals_all else np.array([])
    apply_y_scale(ax, y_scale)
    apply_auto_log_ymin(ax, y_scale, ymin, plotted_vals_all)
    apply_axis_limits(ax, ymin=ymin, ymax=ymax)

    plt.tight_layout()
    if save_path:
        _save_figure_multi(fig, save_path, all_formats)
    plt.show() if show else plt.close(fig)


# =============================================================================
# CLI
# =============================================================================
def parse_args():
    parser = argparse.ArgumentParser(description="PDSP_Bench unified plotting utility (job_query_number-aware).")

    parser.add_argument("--csv", type=str, default="query_performance_data.csv", help="Path to query_performance_data.csv (default: query_performance_data.csv)")

    # Style / palette
    parser.add_argument("--mpl-style", type=str, default="science", choices=["science", "ieee", "nature", "default"])
    parser.add_argument("--latex-text", action="store_true", help="Use LaTeX text rendering (requires LaTeX).")
    parser.add_argument("--hw-palette", type=str, default="modern", choices=list(HW_PALETTES.keys()))
    parser.add_argument("--list-palettes", action="store_true", help="Print hardware palettes and exit.")

    parser.add_argument(
        "--plot",
        type=str,
        required=True,
        choices=[
            # NEW: low/med/high across HW
            "low_med_high_parallelism_across_hw",
            "batch_low_med_high_parallelism_across_hw",
            "low_med_high_eventrate_across_hw",
            "batch_low_med_high_eventrate_across_hw",

            # NEW: dual-axis
            "dual_axis_tp_latency",

            # script-3 modes
            "scaling_single",
            "scaling_compare_fw",
            "queries_single_framework",
            "queries_compare_frameworks",
            "batch_compare_eventrates",
            "batch_all_metrics_eventrates",
            "eventrate_single",
            "eventrate_compare_fw",
            "batch_low_high_parallelism",
            "app_scaling_all_hw",
            "app_eventrate_all_hw",
            "cpu_efficiency_across_hw",
            "intro_hw_scaling_multi",
            "intro_hw_scaling_multi_fw",
            "workload_fw_hw_parallelism",
        ],
        help="Type of plot to generate.",
    )

    parser.add_argument("--framework", type=str, default=None, help="Framework name for single-fw plots.")
    parser.add_argument("--frameworks", type=str, nargs="*", default=None, help="Frameworks to include (e.g. flink storm).")

    # IMPORTANT:
    # --query now accepts either query_id (e.g. "Ad Analytics_2") OR base query (e.g. "Ad Analytics")
    parser.add_argument("--query", type=str, default=None, help="Filter: query_id OR base query name (exact).")
    parser.add_argument("--queries", type=str, nargs="*", default=None, help="List of queries (query_id or base) for intro plots.")

    parser.add_argument("--hardware_type", type=str, default=None, help="Filter: hardware_type (e.g. c6525-100g).")
    parser.add_argument("--hardware-types", type=str, nargs="*", default=None, help="(LMH) Hardware types to include.")
    parser.add_argument("--producer_rate", type=int, default=None, help="Filter: producer_rate (event rate).")
    parser.add_argument("--parallelism_avg", type=int, default=None, help="Filter: parallelism_avg.")
    parser.add_argument("--low_parallelism", type=int, default=None, help="Low parallelism for batch_low_high_parallelism.")
    parser.add_argument("--high_parallelism", type=int, default=None, help="High parallelism for batch_low_high_parallelism.")

    parser.add_argument("--metric", type=str, default=None, help="Metric to plot (required for most non-batch-all-metrics modes).")
    parser.add_argument("--agg", type=str, default="mean", choices=["mean", "median", "max", "min"], help="Aggregation (default: mean).")

    parser.add_argument("--save", type=str, default=None, help="Save base path (extension ignored; PNG always).")
    parser.add_argument("--out_dir", type=str, default="plots", help="Output directory for batch modes.")
    parser.add_argument("--no-show", action="store_true", help="Do not display figures.")
    parser.add_argument("--all-formats", action="store_true", help="Also save PDF+SVG.")
    parser.add_argument("--title", type=str, default=None, help="Custom title.")
    parser.add_argument("--par_map", type=str, default=None, help="(script-3) JSON dict mapping hardware -> single parallelism.")

    # chart + scaling
    parser.add_argument("--chart", type=str, default="bar", choices=["bar", "line"], help="Chart type (default: bar).")
    parser.add_argument("--y-scale", type=str, default="linear", choices=["linear", "log"], help="Y scale (default: linear).")
    parser.add_argument("--show-values", action="store_true", help="Annotate values (bars only).")

    # axis control + point limiting
    parser.add_argument("--ymin", type=float, default=None, help="Explicit y-axis min.")
    parser.add_argument("--ymax", type=float, default=None, help="Explicit y-axis max.")
    parser.add_argument("--max-points", type=int, default=None, help="Limit distinct x points before aggregation.")
    parser.add_argument("--point-strategy", type=str, default="even", choices=["even", "head", "tail"], help="Point limiting strategy.")

    # LMH specific
    parser.add_argument("--par-map-json", type=str, default=None,
                        help="(LMH) JSON mapping hw -> [low,med,high] parallelism or {low,medium,high}. Legacy [low,high] ok.")
    parser.add_argument("--rate-map-json", type=str, default=None,
                        help="(LMH) JSON mapping hw -> [low,med,high] producer_rate or {low,medium,high}. Legacy [low,high] ok.")
    parser.add_argument("--parallelism", type=int, default=None, help="(LMH eventrate single) Fixed parallelism_avg for x=rate LMH plots.")
    parser.add_argument("--metrics", nargs="*", default=None, help="(LMH batch) Override metrics list.")
    parser.add_argument("--show-group-values", action="store_true", help="(LMH) Show per-hardware mapping inside xticks.")

    # Dual-axis specific
    parser.add_argument("--x-dim", type=str, default="parallelism_avg", choices=["parallelism_avg", "producer_rate", "hardware_type"],
                        help="(dual_axis_tp_latency) X dimension.")
    parser.add_argument("--tp-metric", type=str, default="throughput_50", help="(dual_axis_tp_latency) Throughput metric for bars.")
    parser.add_argument("--lat-metric", type=str, default="endtoend_latency", help="(dual_axis_tp_latency) Latency metric for lines.")
    parser.add_argument("--y2-scale", type=str, default="linear", choices=["linear", "log"], help="(dual_axis_tp_latency) Right Y scale.")
    parser.add_argument("--y2min", type=float, default=None, help="(dual_axis_tp_latency) Right Y min.")
    parser.add_argument("--y2max", type=float, default=None, help="(dual_axis_tp_latency) Right Y max.")

    return parser.parse_args()


def main():
    global HARDWARE_COLORS

    args = parse_args()

    if args.list_palettes:
        for name, pal in HW_PALETTES.items():
            print(f"{name}: {pal}")
        return

    HARDWARE_COLORS = HW_PALETTES.get(args.hw_palette, HW_PALETTES["modern"])
    configure_style(mpl_style=args.mpl_style, latex_text=args.latex_text)

    chart = args.chart
    y_scale = args.y_scale
    show_values = args.show_values
    ymin = args.ymin
    ymax = args.ymax
    max_points = args.max_points
    point_strategy = args.point_strategy

    df = load_data(args.csv)
    df = add_query_id_columns(df)
    df = add_efficiency_metrics(df)
    df = apply_parallelism_whitelist(df)

    show = not args.no_show
    all_formats = args.all_formats

    # If a dimension is used as X axis, do not filter it down to a single value.
    producer_rate_filter = args.producer_rate
    parallelism_filter = args.parallelism_avg
    hardware_filter = args.hardware_type

    # existing: producer_rate is X in these plots
    if args.plot in {
        "batch_compare_eventrates",
        "batch_all_metrics_eventrates",
        "eventrate_single",
        "eventrate_compare_fw",
        "batch_low_high_parallelism",
        "app_eventrate_all_hw",
        "batch_low_med_high_eventrate_across_hw",
        "low_med_high_eventrate_across_hw",
    }:
        producer_rate_filter = None

    # dual-axis: ignore whichever dimension is the X axis
    if args.plot == "dual_axis_tp_latency":
        if args.x_dim == "producer_rate":
            producer_rate_filter = None
        if args.x_dim == "parallelism_avg":
            parallelism_filter = None
        if args.x_dim == "hardware_type":
            hardware_filter = None

    df_filtered = filter_data(
        df,
        query=args.query,
        hardware_type=hardware_filter,
        frameworks=args.frameworks,
        producer_rate=producer_rate_filter,
        parallelism_avg=parallelism_filter,
    )

    # Optional: limit number of x points BEFORE aggregation
    if args.plot in {"scaling_single", "scaling_compare_fw", "app_scaling_all_hw"}:
        df_filtered = limit_points_df(df_filtered, "parallelism_avg", max_points, point_strategy)
    elif args.plot in {"eventrate_single", "eventrate_compare_fw", "app_eventrate_all_hw"}:
        df_filtered = limit_points_df(df_filtered, "producer_rate", max_points, point_strategy)
    elif args.plot in {"queries_single_framework", "queries_compare_frameworks"}:
        df_filtered = limit_points_df(df_filtered, "query_id", max_points, point_strategy)
    elif args.plot == "dual_axis_tp_latency":
        df_filtered = limit_points_df(df_filtered, args.x_dim, max_points, point_strategy)

    # Some modes operate on original df; but most should use df_filtered
    if args.plot not in {"intro_hw_scaling_multi", "intro_hw_scaling_multi_fw"} and df_filtered.empty:
        raise SystemExit("No data left after filtering. Check filters / max-points.")

    # -------------------------
    # NEW: LOW/MED/HIGH modes
    # -------------------------
    frameworks = args.frameworks if args.frameworks else ["flink", "storm"]
    frameworks = [f.lower() for f in frameworks]

    if args.hardware_types and len(args.hardware_types) > 0:
        hardware_types = order_hardware(list(args.hardware_types))
    else:
        if "hardware_type" in df.columns:
            hardware_types = order_hardware(sorted(df["hardware_type"].dropna().unique().tolist()))
        else:
            hardware_types = order_hardware(HARDWARE_ORDER[:])

    if args.plot in {"low_med_high_parallelism_across_hw", "batch_low_med_high_parallelism_across_hw"}:
        if args.par_map_json is None:
            raise SystemExit("LMH parallelism plots require --par-map-json")

        par_map_raw = parse_level_map_json(args.par_map_json, "--par-map-json")
        missing = [hw for hw in hardware_types if hw not in par_map_raw]
        if missing:
            raise SystemExit("Missing parallelism mapping for hardware: " + ", ".join(missing))

        par_map = autofill_medium_from_df(df, hardware_types, par_map_raw, col="parallelism_avg")

        if args.plot == "low_med_high_parallelism_across_hw":
            if args.query is None or args.metric is None or args.producer_rate is None:
                raise SystemExit("Requires --query --metric --producer_rate (query should be query_id for per-job plots).")
            if "query_id" in df_filtered.columns and df_filtered["query_id"].nunique() != 1:
                raise SystemExit("low_med_high_parallelism_across_hw expects a single query_id. Pass --query '<base>_<job#>'.")
            qid = df_filtered["query_id"].iloc[0]
            plot_low_med_high_parallelism_across_hardware(
                df=df,
                query=str(qid),
                metric=args.metric,
                producer_rate=int(args.producer_rate),
                frameworks=frameworks,
                hardware_types=hardware_types,
                par_map=par_map,
                agg=args.agg,
                save_path=args.save,
                show=show,
                all_formats=all_formats,
                show_values=show_values,
                show_group_values=args.show_group_values,
            )
            return

        metrics = args.metrics if args.metrics else METRICS
        batch_low_med_high_parallelism_across_hw(
            df=df,
            frameworks=frameworks,
            hardware_types=hardware_types,
            par_map=par_map,
            out_dir=args.out_dir,
            agg=args.agg,
            show=show,
            all_formats=all_formats,
            show_values=show_values,
            show_group_values=args.show_group_values,
            metrics=metrics,
        )
        return

    if args.plot in {"low_med_high_eventrate_across_hw", "batch_low_med_high_eventrate_across_hw"}:
        if args.rate_map_json is None:
            raise SystemExit("LMH event-rate plots require --rate-map-json")

        rate_map_raw = parse_level_map_json(args.rate_map_json, "--rate-map-json")
        missing = [hw for hw in hardware_types if hw not in rate_map_raw]
        if missing:
            raise SystemExit("Missing event-rate mapping for hardware: " + ", ".join(missing))

        rate_map = autofill_medium_from_df(df, hardware_types, rate_map_raw, col="producer_rate")

        if args.plot == "low_med_high_eventrate_across_hw":
            if args.query is None or args.metric is None or args.parallelism is None:
                raise SystemExit("Requires --query --metric --parallelism (query should be query_id for per-job plots).")
            if "query_id" in df_filtered.columns and df_filtered["query_id"].nunique() != 1:
                raise SystemExit("low_med_high_eventrate_across_hw expects a single query_id. Pass --query '<base>_<job#>'.")
            qid = df_filtered["query_id"].iloc[0]
            plot_low_med_high_eventrate_across_hardware(
                df=df,
                query=str(qid),
                metric=args.metric,
                parallelism=int(args.parallelism),
                frameworks=frameworks,
                hardware_types=hardware_types,
                rate_map=rate_map,
                agg=args.agg,
                save_path=args.save,
                show=show,
                all_formats=all_formats,
                show_values=show_values,
                show_group_values=args.show_group_values,
            )
            return

        metrics = args.metrics if args.metrics else METRICS
        batch_low_med_high_eventrate_across_hw(
            df=df,
            frameworks=frameworks,
            hardware_types=hardware_types,
            rate_map=rate_map,
            out_dir=args.out_dir,
            agg=args.agg,
            show=show,
            all_formats=all_formats,
            show_values=show_values,
            show_group_values=args.show_group_values,
            metrics=metrics,
        )
        return

    # -------------------------
    # NEW: dual-axis plot mode
    # -------------------------
    if args.plot == "dual_axis_tp_latency":
        # Uses --tp-metric and --lat-metric, so --metric is NOT required
        plot_dual_axis_tp_latency_compare_frameworks(
            df_filtered,
            x_dim=args.x_dim,
            throughput_metric=args.tp_metric,
            latency_metric=args.lat_metric,
            agg=args.agg,
            title=args.title,
            save_path=args.save,
            show=show,
            all_formats=all_formats,
            y_scale=args.y_scale,
            y2_scale=args.y2_scale,
            show_values=show_values,
            ymin=args.ymin,
            ymax=args.ymax,
            y2min=args.y2min,
            y2max=args.y2max,
        )
        return

    # -------------------------
    # SCRIPT-3 modes
    # -------------------------
    if args.plot in {
        "scaling_single",
        "scaling_compare_fw",
        "queries_single_framework",
        "queries_compare_frameworks",
        "batch_compare_eventrates",
        "eventrate_single",
        "eventrate_compare_fw",
        "batch_low_high_parallelism",
        "app_scaling_all_hw",
        "app_eventrate_all_hw",
        "cpu_efficiency_across_hw",
        "intro_hw_scaling_multi",
        "intro_hw_scaling_multi_fw",
        "workload_fw_hw_parallelism",
    } and args.metric is None:
        raise SystemExit("--metric is required for this plot mode.")

    if args.plot == "scaling_single":
        if args.framework is None:
            raise SystemExit("You must provide --framework for scaling_single.")
        plot_scaling_single_framework(
            df_filtered, framework=args.framework, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "scaling_compare_fw":
        plot_scaling_compare_frameworks(
            df_filtered, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "eventrate_single":
        if args.framework is None:
            raise SystemExit("You must provide --framework for eventrate_single.")
        plot_eventrate_single_framework(
            df_filtered, framework=args.framework, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "eventrate_compare_fw":
        plot_eventrate_compare_frameworks(
            df_filtered, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "queries_single_framework":
        if args.framework is None:
            raise SystemExit("You must provide --framework for queries_single_framework.")
        plot_queries_single_framework(
            df_filtered, framework=args.framework, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "queries_compare_frameworks":
        plot_queries_compare_frameworks(
            df_filtered, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "batch_compare_eventrates":
        batch_compare_frameworks_over_event_rates(
            df_filtered, metric=args.metric, agg=args.agg, out_dir=args.out_dir,
            show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "batch_all_metrics_eventrates":
        batch_compare_frameworks_over_all_metrics_and_event_rates(
            df_filtered, agg=args.agg, out_dir=args.out_dir,
            show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "batch_low_high_parallelism":
        if args.low_parallelism is None or args.high_parallelism is None:
            raise SystemExit("You must provide --low_parallelism and --high_parallelism.")
        batch_compare_low_high_parallelism(
            df_filtered, low_par=args.low_parallelism, high_par=args.high_parallelism, agg=args.agg,
            out_dir=args.out_dir, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "app_scaling_all_hw":
        plot_app_scaling_all_hw(
            df_filtered, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "app_eventrate_all_hw":
        plot_app_eventrate_all_hw(
            df_filtered, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "cpu_efficiency_across_hw":
        if args.par_map is not None:
            try:
                par_map = json.loads(args.par_map)
            except Exception as e:
                raise SystemExit(f"Failed to parse --par_map JSON: {e}")
        else:
            par_map = {"m510": 40, "c6525-25g": 80, "c6525-100g": 96}

        if not args.frameworks:
            raise SystemExit("cpu_efficiency_across_hw requires --frameworks.")
        if args.query is None:
            raise SystemExit("cpu_efficiency_across_hw requires --query (query_id or base).")
        if args.producer_rate is None:
            raise SystemExit("cpu_efficiency_across_hw requires --producer_rate.")

        plot_cpu_efficiency_across_hardware(
            df, query=args.query, frameworks=args.frameworks, par_map=par_map,
            producer_rate=args.producer_rate, metric=args.metric, agg=args.agg,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            chart=chart, y_scale=y_scale, show_values=show_values, ymin=ymin, ymax=ymax
        )

    elif args.plot == "intro_hw_scaling_multi":
        if args.framework is None:
            raise SystemExit("intro_hw_scaling_multi requires --framework.")
        if args.producer_rate is None:
            raise SystemExit("intro_hw_scaling_multi requires --producer_rate.")

        intro_queries = args.queries if args.queries else ([args.query] if args.query else None)
        if not intro_queries:
            raise SystemExit("intro_hw_scaling_multi requires --queries or --query.")

        plot_intro_hw_scaling_multiqueries(
            df, queries=intro_queries, framework=args.framework, metric=args.metric,
            producer_rate=args.producer_rate, agg=args.agg, par_levels=None,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            y_scale=y_scale, ymin=ymin, ymax=ymax
        )

    elif args.plot == "intro_hw_scaling_multi_fw":
        frameworks = args.frameworks if args.frameworks else ["flink", "storm"]
        if args.producer_rate is None:
            raise SystemExit("intro_hw_scaling_multi_fw requires --producer_rate.")

        intro_queries = args.queries if args.queries else ([args.query] if args.query else None)
        if not intro_queries:
            raise SystemExit("intro_hw_scaling_multi_fw requires --queries or --query.")

        plot_intro_hw_scaling_multi_fw(
            df, queries=intro_queries, frameworks=frameworks, metric=args.metric,
            producer_rate=args.producer_rate, agg=args.agg, par_levels=None,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            y_scale=y_scale, ymin=ymin, ymax=ymax
        )

    elif args.plot == "workload_fw_hw_parallelism":
        if args.query is None:
            raise SystemExit("workload_fw_hw_parallelism requires --query (must be query_id for per-job plots).")
        if args.producer_rate is None:
            raise SystemExit("workload_fw_hw_parallelism requires --producer_rate.")

        if "query_id" in df_filtered.columns and df_filtered["query_id"].nunique() != 1:
            raise SystemExit("workload_fw_hw_parallelism expects a single query_id. Pass --query '<base>_<job#>'.")

        qid = df_filtered["query_id"].iloc[0]
        frameworks = args.frameworks if args.frameworks else ["flink", "storm"]
        plot_workload_fw_hw_parallelism(
            df, query=str(qid), frameworks=frameworks, metric=args.metric,
            producer_rate=args.producer_rate, agg=args.agg, par_levels=None,
            title=args.title, save_path=args.save, show=show, all_formats=all_formats,
            y_scale=y_scale, ymin=ymin, ymax=ymax
        )

    else:
        raise SystemExit(f"Unhandled plot mode: {args.plot}")


if __name__ == "__main__":
    main()
