#!/usr/bin/env python3

import argparse
import os
import re
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


# -----------------------------
# Framework styling
# -----------------------------
FRAMEWORKS_DEFAULT = ["flink", "storm"]
FRAMEWORK_HATCH = {"flink": "", "storm": "xx"}
FRAMEWORK_ALPHA = {"flink": 1.0, "storm": 0.70}


def configure_style(mpl_style: str = "default", latex_text: bool = False, fig_w: float = 12.0, fig_h: float = 5.0):
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
        "legend.fontsize": 9,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.autolayout": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "hatch.linewidth": 0.8,
    })


def slugify(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s)).strip("_")


def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def save_figure(fig, save_base: str, all_formats: bool):
    ensure_dir(save_base)
    fig.savefig(save_base + ".png", dpi=300)
    if all_formats:
        fig.savefig(save_base + ".pdf")
        fig.savefig(save_base + ".svg")


# -----------------------------
# Loading
# -----------------------------
def load_operator_wide(operator_dir: str) -> pd.DataFrame:
    files = []
    for root, _dirs, fs in os.walk(operator_dir):
        for f in fs:
            if f.endswith(".csv") and "operator_metrics_" in f:
                files.append(os.path.join(root, f))
    if not files:
        raise SystemExit(f"No operator metrics CSVs found under: {operator_dir}")

    dfs = []
    for fp in sorted(files):
        try:
            df = pd.read_csv(fp)
            df["_source_file"] = os.path.basename(fp)
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Failed reading {fp}: {e}")

    if not dfs:
        raise SystemExit("No readable operator metrics CSVs were found.")

    out = pd.concat(dfs, ignore_index=True)

    # normalize framework
    if "framework" in out.columns:
        out["framework"] = out["framework"].astype(str).str.lower()
    else:
        raise SystemExit("Missing required column: framework")

    # ensure query + job_query_number -> query_id
    if "query" not in out.columns:
        out["query"] = ""
    if "job_query_number" not in out.columns:
        out["job_query_number"] = 1
    out["job_query_number"] = pd.to_numeric(out["job_query_number"], errors="coerce").fillna(1).astype(int)

    out["query_base"] = out["query"].astype(str)
    out["query_id"] = out["query_base"] + "_" + out["job_query_number"].astype(str)

    # ensure numeric parallelism_avg / producer_rate (best effort)
    if "parallelism_avg" in out.columns:
        out["parallelism_avg"] = pd.to_numeric(out["parallelism_avg"], errors="coerce")
    if "producer_rate" in out.columns:
        out["producer_rate"] = pd.to_numeric(out["producer_rate"], errors="coerce")

    return out


def filter_df(
    df: pd.DataFrame,
    query: Optional[str] = None,           # query_id or base query
    hardware_type: Optional[str] = None,
    producer_rate: Optional[int] = None,
    frameworks: Optional[List[str]] = None,
) -> pd.DataFrame:
    sub = df.copy()

    if query is not None:
        q = str(query)
        if (sub["query_id"].astype(str) == q).any():
            sub = sub[sub["query_id"].astype(str) == q]
        else:
            sub = sub[sub["query_base"].astype(str) == q]

    if hardware_type is not None:
        sub = sub[sub["hardware_type"].astype(str) == str(hardware_type)]

    if producer_rate is not None and "producer_rate" in sub.columns:
        sub = sub[pd.to_numeric(sub["producer_rate"], errors="coerce") == int(producer_rate)]

    if frameworks:
        fws = [f.lower() for f in frameworks]
        sub = sub[sub["framework"].astype(str).str.lower().isin(fws)]

    return sub


# -----------------------------
# Operator discovery (by index)
# -----------------------------
_OP_METRIC_RE = re.compile(r"^op(?P<i>\d+)_(?P<m>selectivity|latency_50|latency_95|latency_99)$")


def discover_operator_indices(df: pd.DataFrame) -> List[int]:
    ops = set()
    for c in df.columns:
        m = _OP_METRIC_RE.match(str(c))
        if m:
            ops.add(int(m.group("i")))
    return sorted(ops)


def operator_label(df_slice: pd.DataFrame, op_idx: int) -> str:
    """
    Nice label per operator index. Uses whatever names exist.
    Example: "op3: ParseBolt | Map"
    """
    c_f = f"op{op_idx}_name_flink"
    c_s = f"op{op_idx}_name_storm"
    name_f = None
    name_s = None
    if c_f in df_slice.columns:
        vals = df_slice[c_f].dropna().astype(str).unique().tolist()
        if vals:
            name_f = vals[0]
    if c_s in df_slice.columns:
        vals = df_slice[c_s].dropna().astype(str).unique().tolist()
        if vals:
            name_s = vals[0]

    if name_f and name_s and name_f != name_s:
        return f"op{op_idx}: {name_s} | {name_f}"
    if name_s:
        return f"op{op_idx}: {name_s}"
    if name_f:
        return f"op{op_idx}: {name_f}"
    return f"op{op_idx}"


# -----------------------------
# Plotting
# -----------------------------
def agg_mean(df_slice: pd.DataFrame, framework: str, par: float, col: str) -> float:
    d = df_slice[(df_slice["framework"] == framework) & (df_slice["parallelism_avg"] == par)]
    if d.empty or col not in d.columns:
        return np.nan
    vals = pd.to_numeric(d[col], errors="coerce").dropna()
    return float(vals.mean()) if not vals.empty else np.nan


def plot_operator_metric_over_parallelism(
    df: pd.DataFrame,
    query_id: str,
    hardware_type: str,
    producer_rate: int,
    metric: str,                        # selectivity|latency_50|latency_95|latency_99
    frameworks: List[str],
    out_base: Optional[str],
    show: bool,
    all_formats: bool,
    y_scale: str,
):
    df_slice = filter_df(
        df,
        query=query_id,
        hardware_type=hardware_type,
        producer_rate=producer_rate,
        frameworks=frameworks,
    )

    if df_slice.empty:
        raise ValueError("No data after filtering this slice.")

    if "parallelism_avg" not in df_slice.columns:
        raise ValueError("Missing parallelism_avg in operator-wide CSV.")

    # parallelism values
    pars = pd.to_numeric(df_slice["parallelism_avg"], errors="coerce").dropna()
    pars = sorted(pars.unique().tolist())
    if not pars:
        raise ValueError("No parallelism_avg values found.")

    # operators (by index)
    op_indices = discover_operator_indices(df_slice)
    if not op_indices:
        raise ValueError("No operator columns found (op{i}_selectivity / op{i}_latency_*)")

    # frameworks present
    frameworks = [f.lower() for f in frameworks]
    frameworks_present = [f for f in frameworks if (df_slice["framework"] == f).any()]
    if not frameworks_present:
        raise ValueError("None of the requested frameworks are present in this slice.")

    # prepare colors per operator
    cmap = plt.get_cmap("tab20")
    op_colors = {op: cmap((op - 1) % 20) for op in op_indices}

    # bar geometry: per parallelism cluster: operators grouped, each operator has 2 bars (fw)
    x = np.arange(len(pars))
    total_width = 0.82
    n_ops = len(op_indices)
    n_fw = len(frameworks_present)

    op_group_w = total_width / max(1, n_ops)
    fw_bar_w = op_group_w / max(1, n_fw)

    fig, ax = plt.subplots(figsize=(max(12, 1.5 + 0.9 * len(pars)), 5.2))

    # plot bars
    for oi, op in enumerate(op_indices):
        col = f"op{op}_{metric}"
        for fi, fw in enumerate(frameworks_present):
            vals = []
            for par in pars:
                vals.append(agg_mean(df_slice, fw, par, col))
            vals = np.asarray(vals, dtype=float)

            # offset within each x cluster
            # cluster left edge = x - total_width/2
            left = x - total_width / 2
            op_left = left + oi * op_group_w
            fw_left = op_left + fi * fw_bar_w
            xpos = fw_left + fw_bar_w / 2

            ax.bar(
                xpos,
                vals,
                width=fw_bar_w * 0.98,
                color=op_colors[op],
                edgecolor="black",
                hatch=FRAMEWORK_HATCH.get(fw, ""),
                alpha=FRAMEWORK_ALPHA.get(fw, 0.9),
                linewidth=0.8,
            )

    # axes
    ax.set_xticks(x)
    ax.set_xticklabels([str(int(p)) if float(p).is_integer() else str(p) for p in pars])
    ax.set_xlabel("parallelism_avg")

    ylabel = metric
    if metric == "selectivity":
        ylabel = "selectivity"
    elif metric.startswith("latency_"):
        pct = metric.split("_")[1]
        ylabel = f"operator latency p{pct}"
    ax.set_ylabel(ylabel)

    ax.grid(axis="y", linestyle="--", alpha=0.35)

    title = f"{query_id} | {hardware_type} | rate={producer_rate} | {ylabel}"
    ax.set_title(title)

    # y-scale
    if y_scale == "log":
        ax.set_yscale("log")

    # legends: one for frameworks (hatch), one for operators (color)
    fw_handles = []
    for fw in frameworks_present:
        fw_handles.append(
            mpatches.Patch(
                facecolor="white",
                edgecolor="black",
                hatch=FRAMEWORK_HATCH.get(fw, ""),
                label=fw,
                alpha=FRAMEWORK_ALPHA.get(fw, 0.9),
            )
        )
    leg1 = ax.legend(handles=fw_handles, title="framework", loc="upper left", bbox_to_anchor=(1.01, 1.0))
    ax.add_artist(leg1)

    op_handles = []
    for op in op_indices:
        op_handles.append(
            mpatches.Patch(
                facecolor=op_colors[op],
                edgecolor="black",
                label=operator_label(df_slice, op),
            )
        )
    ax.legend(handles=op_handles, title="operator (storm | flink)", loc="upper left", bbox_to_anchor=(1.01, 0.40))

    plt.tight_layout()

    if out_base:
        save_figure(fig, out_base, all_formats)

    if show:
        plt.show()
    else:
        plt.close(fig)


# -----------------------------
# Batch
# -----------------------------
DEFAULT_METRICS = ["selectivity", "latency_50", "latency_95", "latency_99"]


def batch_all(
    df: pd.DataFrame,
    out_dir: str,
    frameworks: List[str],
    metrics: List[str],
    show: bool,
    all_formats: bool,
    y_scale: str,
    query: Optional[str] = None,
    hardware_type: Optional[str] = None,
    metric_allow_regex: Optional[str] = None,
):
    df0 = filter_df(df, query=query, hardware_type=hardware_type, frameworks=frameworks)
    if df0.empty:
        raise SystemExit("No data after applying filters for batch.")

    # choose metrics (optionally filtered)
    metrics2 = metrics[:]
    if metric_allow_regex:
        rx = re.compile(metric_allow_regex)
        metrics2 = [m for m in metrics2 if rx.search(m)]
    if not metrics2:
        raise SystemExit("No metrics left after applying --metric-allow-regex.")

    qids = sorted(df0["query_id"].dropna().astype(str).unique().tolist())
    hws = sorted(df0["hardware_type"].dropna().astype(str).unique().tolist())

    for qid in qids:
        for hw in hws:
            df_qh = df0[(df0["query_id"].astype(str) == qid) & (df0["hardware_type"].astype(str) == hw)]
            if df_qh.empty:
                continue

            rates = sorted(pd.to_numeric(df_qh["producer_rate"], errors="coerce").dropna().astype(int).unique().tolist())
            for r in rates:
                for m in metrics2:
                    save_base = os.path.join(
                        out_dir,
                        "operator_over_parallelism",
                        slugify(qid),
                        slugify(hw),
                        slugify(m),
                        f"rate{int(r)}",
                    )
                    print(f"[INFO] {qid} | {hw} | rate={r} | metric={m} -> {save_base}")
                    try:
                        plot_operator_metric_over_parallelism(
                            df=df,
                            query_id=qid,
                            hardware_type=hw,
                            producer_rate=int(r),
                            metric=m,
                            frameworks=frameworks,
                            out_base=save_base,
                            show=show,
                            all_formats=all_formats,
                            y_scale=y_scale,
                        )
                    except Exception as e:
                        print(f"[WARN] Skipping: {qid} | {hw} | rate={r} | {m}: {e}")


# -----------------------------
# CLI
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="PDSP-like operator plots over parallelism_avg (wide operator CSVs).")
    p.add_argument("--operator-dir", type=str, default="operator_metrics_wide",
                   help="Directory containing operator_metrics_*.csv (default: operator_metrics_wide)")
    p.add_argument("--out-dir", type=str, default="plots", help="Output directory (default: plots)")

    p.add_argument("--plot", required=True, choices=["single", "batch_all"])
    p.add_argument("--mpl-style", type=str, default="default", choices=["science", "ieee", "nature", "default"])
    p.add_argument("--latex-text", action="store_true")
    p.add_argument("--frameworks", nargs="*", default=FRAMEWORKS_DEFAULT)

    p.add_argument("--metrics", nargs="*", default=DEFAULT_METRICS,
                   help="Metrics to plot (default: selectivity latency_50 latency_95 latency_99)")
    p.add_argument("--metric-allow-regex", type=str, default=None,
                   help="Filter metrics by regex in batch_all (e.g. 'latency|selectivity')")

    p.add_argument("--y-scale", type=str, default="linear", choices=["linear", "log"])
    p.add_argument("--all-formats", action="store_true")
    p.add_argument("--no-show", action="store_true")

    # filters
    p.add_argument("--query", type=str, default=None, help="query_id like 'Ad Analytics_2' OR base query 'Ad Analytics'")
    p.add_argument("--hardware-type", type=str, default=None)
    p.add_argument("--producer-rate", type=int, default=None)

    # single-only
    p.add_argument("--metric", type=str, default=None, help="single plot metric: selectivity|latency_50|latency_95|latency_99")

    return p.parse_args()


def main():
    args = parse_args()
    configure_style(args.mpl_style, args.latex_text)

    df = load_operator_wide(args.operator_dir)
    frameworks = [f.lower() for f in (args.frameworks or FRAMEWORKS_DEFAULT)]
    show = not args.no_show

    if args.plot == "single":
        if args.query is None or args.hardware_type is None or args.producer_rate is None or args.metric is None:
            raise SystemExit("single requires: --query --hardware-type --producer-rate --metric")

        # enforce one query_id
        df_one = filter_df(df, query=args.query)
        qids = sorted(df_one["query_id"].dropna().astype(str).unique().tolist())
        if len(qids) != 1:
            raise SystemExit(f"--query matched {qids}. For single, pass an exact query_id like 'Ad Analytics_2'.")
        qid = qids[0]

        save_base = os.path.join(
            args.out_dir,
            "operator_over_parallelism",
            slugify(qid),
            slugify(args.hardware_type),
            slugify(args.metric),
            f"rate{int(args.producer_rate)}",
        )

        plot_operator_metric_over_parallelism(
            df=df,
            query_id=qid,
            hardware_type=args.hardware_type,
            producer_rate=int(args.producer_rate),
            metric=args.metric,
            frameworks=frameworks,
            out_base=save_base,
            show=show,
            all_formats=args.all_formats,
            y_scale=args.y_scale,
        )
        return

    if args.plot == "batch_all":
        batch_all(
            df=df,
            out_dir=args.out_dir,
            frameworks=frameworks,
            metrics=[m for m in (args.metrics or DEFAULT_METRICS)],
            show=show,
            all_formats=args.all_formats,
            y_scale=args.y_scale,
            query=args.query,
            hardware_type=args.hardware_type,
            metric_allow_regex=args.metric_allow_regex,
        )
        return


if __name__ == "__main__":
    main()
