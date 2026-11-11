#!/usr/bin/env python3
"""Operator-level metrics for all five engines (Flink, Storm, Spark, Kafka Streams, RisingWave).

Walks artifacts/ (same layout as 1_data_extractor.py) and writes
  data_20251201.csv                         run-level metrics (one row per run)
  operator_metrics_wide/operator_metrics_<query>.csv   one row per run, operator metrics as op<i>_* columns
Run-level values for Spark, Kafka Streams and RisingWave, and the strategy/query-variant parsing, are taken from
1_data_extractor.py so both scripts read every engine the same way. uniform_1 folders are skipped (as in step 1).
Operator columns: op<i>_name_<framework>, op<i>_selectivity, op<i>_latency_50/95/99 (Flink/Storm only),
op<i>_throughput_per_sec, op<i>_busy_ratio, op<i>_backpressure_ratio (where the engine reports them).
Operator index i follows each engine's own operator order, so op<i> can be a different operator per engine."""

import importlib.util
import os
import json
import re
import statistics
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("data_extractor_step1", os.path.join(_HERE, "1_data_extractor.py"))
step1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(step1)


# ============================================================
# Helpers
# ============================================================

def to_float(v: Any) -> Optional[float]:
    if isinstance(v, list):
        if not v:
            return None
        v = v[0]
    try:
        return float(v)
    except Exception:
        return None


def safe_query_name(q: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", q).strip("_")


def flatten_json(obj: Any, prefix: str = "meta", sep: str = "__") -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    def _rec(x: Any, key_prefix: str):
        if isinstance(x, dict):
            for k, v in x.items():
                _rec(v, f"{key_prefix}{sep}{k}")
        elif isinstance(x, list):
            out[key_prefix] = json.dumps(x)
        else:
            out[key_prefix] = x

    if isinstance(obj, dict):
        for k, v in obj.items():
            _rec(v, f"{prefix}{sep}{k}")
    else:
        _rec(obj, prefix)

    return out


# ============================================================
# Parallelism & class helpers
# ============================================================

def compute_parallelism_avg(par_str: str) -> float:
    nums = [float(v) for v in str(par_str).split(",") if str(v).strip() != ""]
    if not nums:
        return 0.0
    if len(nums) <= 2:
        return statistics.fmean(nums)
    middle = nums[1:-1]
    return statistics.fmean(middle) if middle else statistics.fmean(nums)


def get_class(n: float):
    n = float(n)
    if n <= 5:
        return ("XS", 1, 1)
    elif n <= 10:
        return ("S", 2, 2)
    elif n <= 15:
        return ("S", 2, 3)
    elif n <= 20:
        return ("M", 3, 4)
    elif n <= 25:
        return ("M", 3, 5)
    elif n <= 30:
        return ("L", 4, 6)
    elif n <= 35:
        return ("L", 4, 7)
    elif n <= 40:
        return ("XL", 5, 8)
    elif n <= 45:
        return ("XL", 5, 9)
    elif n <= 50:
        return ("XL", 5, 10)
    elif n <= 55:
        return ("XL", 5, 11)
    elif n <= 60:
        return ("XXL", 6, 12)
    elif n <= 65:
        return ("XXL", 6, 13)
    elif n <= 70:
        return ("XXL", 6, 14)
    elif n <= 75:
        return ("XXL", 6, 15)
    elif n <= 80:
        return ("XXL", 6, 16)
    else:
        return ("XXL", 6, 16)


# ============================================================
# Query mapping
# ============================================================

def get_query_from_directory(directory: str) -> str:
    if "word-count" == directory:
        return "Word Count"
    if "word-count-shuffle" == directory:
        return "Word Count Shuffle"
    elif "smart-grid" == directory:
        return "Smart Grid"
    elif "ad-analytics" == directory:
        return "Ad Analytics"
    elif "google-cloud-monitoring" == directory:
        return "Google Cloud Monitoring"
    elif "sentiment-analysis" == directory:
        return "Sentiment Analysis"
    elif "spike-detection" == directory:
        return "Spike Detection"
    elif "log-processing" == directory:
        return "Log Processing"
    elif "trending-topics" == directory:
        return "Trending Topics"
    elif "bargain-index" == directory:
        return "Bargain Index"
    elif "click-analytics" == directory:
        return "Click Analytics"
    elif "click-analytics-1" == directory:
        return "Click Analytics"
    elif "click-analytics-2" == directory:
        return "Click Analytics Count-based"
    elif "machine-outlier" == directory:
        return "Machine Outlier"
    elif "linear-road" == directory:
        return "Linear Road"
    elif "tpch" == directory:
        return "TPCH"
    elif "tpch-1" == directory:
        return "TPCH"
    elif "tpch-2" == directory:
        return "TPCH Window"
    elif "traffic-monitoring" == directory:
        return "Traffic Monitoring"
    else:
        raise NotImplementedError(f'Directory "{directory}" is not assigned a query name.')


# ============================================================
# Hardware metrics aggregation
# ============================================================

def get_hardware_values(_info: dict, data: dict) -> dict:
    out: Dict[str, float] = {}
    cpu_50 = cpu_95 = cpu_98 = 0.0
    mem_50 = mem_95 = mem_98 = 0.0
    net_50 = net_95 = net_98 = 0.0

    hw = data.get("hardwareMetrics", {})
    for k, v in hw.items():
        fv = to_float(v)
        if fv is None:
            continue
        if k == "node_cpu_50":
            cpu_50 += fv
        elif k == "node_cpu_95":
            cpu_95 += fv
        elif k == "node_cpu_98":
            cpu_98 += fv
        elif k == "node_memory_50":
            mem_50 += fv
        elif k == "node_memory_95":
            mem_95 += fv
        elif k == "node_memory_98":
            mem_98 += fv
        elif k == "node_network_out_50":
            net_50 += fv
        elif k == "node_network_out_95":
            net_95 += fv
        elif k == "node_network_out_98":
            net_98 += fv

    out["cpu_50"] = cpu_50
    out["cpu_95"] = cpu_95
    out["cpu_98"] = cpu_98
    out["memory_50"] = mem_50
    out["memory_95"] = mem_95
    out["memory_98"] = mem_98
    out["network_50"] = net_50
    out["network_95"] = net_95
    out["network_98"] = net_98
    return out


# ============================================================
# Run-level E2E latency percentiles
# ============================================================

def extract_e2e_latency_percentiles(qm: Dict[str, Any], percentiles=("50", "95", "99")) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for p in percentiles:
        acc = 0.0
        found = False
        for k, v in qm.items():
            ks = str(k)
            if ks.startswith("endtoend_latency") and ks.endswith(str(p)):
                fv = to_float(v)
                if fv is not None:
                    acc += fv
                    found = True
        if found:
            out[f"endtoend_latency_{p}"] = acc
    return out


# ============================================================
# Run-level row builders
# ============================================================

def _common_run_fields(info: dict, path_parts: List[str], framework: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out["framework"] = framework
    out["query"] = get_query_from_directory(path_parts[1])

    out["parallelism"] = info.get("job_parallelization")
    out["parallelism_source"] = str(out["parallelism"]).split(",")[0] if out["parallelism"] is not None else None
    out["parallelism_avg"] = compute_parallelism_avg(out["parallelism"]) if out["parallelism"] is not None else 0.0
    out["parallelism_sink"] = str(out["parallelism"]).split(",")[-1] if out["parallelism"] is not None else None

    out["class"], out["class_num"], out["class_num_distinct"] = get_class(out["parallelism_avg"])

    out["producer_rate"] = info.get("producer_event_per_second")
    out["job_query_number"] = info.get("job_query_number")

    out["hardware_type"] = path_parts[2]
    if str(path_parts[2]).endswith("f"):
        out["hardware_type"] = out["hardware_type"] + " (mem)"

    out["nodes"] = int(info["cluster_nodes"]) if "cluster_nodes" in info else int(str(path_parts[2]).split("-")[-1])
    out["slots_per_node"] = (
        int(info["cluster_slots_per_node"])
        if "cluster_slots_per_node" in info
        else int(str(path_parts[3]).split("-")[-1]) / int(out["nodes"])
    )
    out["slots"] = out["slots_per_node"] * out["nodes"]
    out["enumeration_strategy"] = info.get("enumeration_strategy", "Custom")

    out["time"] = info.get("job_run_time")
    out["window_size"] = info.get("job_window_size")
    out["window_slide"] = info.get("job_window_slide_size")
    return out


def get_storm_values(info: dict, data: dict, path_parts: List[str]) -> pd.DataFrame:
    out = _common_run_fields(info, path_parts, framework="storm")
    qm = data.get("queryMetrics", {})

    event_rate_50 = event_rate_95 = event_rate_98 = 0.0
    throughput_50 = throughput_95 = throughput_98 = 0.0

    for k, v in qm.items():
        fv = to_float(v)
        if fv is None:
            continue
        ks = str(k)
        if ks.startswith("recPerSecOutSource") and ks.endswith("50"):
            event_rate_50 += fv
        elif ks.startswith("recPerSecOutSource") and ks.endswith("95"):
            event_rate_95 += fv
        elif ks.startswith("recPerSecOutSource") and ks.endswith("98"):
            event_rate_98 += fv
        elif ks.startswith("throughput") and ks.endswith("50"):
            throughput_50 += fv
        elif ks.startswith("throughput") and ks.endswith("95"):
            throughput_95 += fv
        elif ks.startswith("throughput") and ks.endswith("98"):
            throughput_98 += fv

    out["event_rate_50"] = event_rate_50
    out["event_rate_95"] = event_rate_95
    out["event_rate_98"] = event_rate_98
    out["throughput_50"] = throughput_50
    out["throughput_95"] = throughput_95
    out["throughput_98"] = throughput_98

    e2e = extract_e2e_latency_percentiles(qm, percentiles=("50", "95", "99"))
    if e2e:
        out.update(e2e)
        if "endtoend_latency_50" in e2e:
            out["endtoend_latency"] = e2e["endtoend_latency_50"]
    else:
        fv = to_float(qm.get("endtoend_latency_storm"))
        if fv is not None:
            out["endtoend_latency_50"] = fv
            out["endtoend_latency"] = fv

    out |= get_hardware_values(info, data)
    return pd.DataFrame([out])


def get_flink_values(info: dict, data: dict, path_parts: List[str]) -> pd.DataFrame:
    out = _common_run_fields(info, path_parts, framework="flink")
    qm = data.get("queryMetrics", {})

    event_rate_50 = event_rate_95 = event_rate_98 = 0.0
    throughput_50 = throughput_95 = throughput_98 = 0.0

    for k, v in qm.items():
        fv = to_float(v)
        if fv is None:
            continue
        ks = str(k)
        if ks.startswith("recPerSecOutSource") and ks.endswith("50"):
            event_rate_50 += fv
        elif ks.startswith("recPerSecOutSource") and ks.endswith("95"):
            event_rate_95 += fv
        elif ks.startswith("recPerSecOutSource") and ks.endswith("98"):
            event_rate_98 += fv
        elif ks.startswith("throughput") and ks.endswith("50"):
            throughput_50 += fv
        elif ks.startswith("throughput") and ks.endswith("95"):
            throughput_95 += fv
        elif ks.startswith("throughput") and ks.endswith("98"):
            throughput_98 += fv

    out["event_rate_50"] = event_rate_50
    out["event_rate_95"] = event_rate_95
    out["event_rate_98"] = event_rate_98
    out["throughput_50"] = throughput_50
    out["throughput_95"] = throughput_95
    out["throughput_98"] = throughput_98

    e2e = extract_e2e_latency_percentiles(qm, percentiles=("50", "95", "99"))
    out.update(e2e)
    if "endtoend_latency_50" in e2e:
        out["endtoend_latency"] = e2e["endtoend_latency_50"]

    out |= get_hardware_values(info, data)
    return pd.DataFrame([out])


# ============================================================
# Operator extraction (WIDE by index, unified latency, fw-specific names)
# ============================================================

_PCT_RE = re.compile(r"_(\d+)(?:th)?$", re.IGNORECASE)

def extract_scalar_from_nested(d: Any) -> Optional[float]:
    """Flink/Storm store operator metrics as {series_id: value}; Spark, Kafka Streams and RisingWave as plain numbers."""
    if isinstance(d, (int, float)) and not isinstance(d, bool):
        return float(d)
    if not isinstance(d, dict) or not d:
        return None
    return to_float(list(d.values())[0])


def extract_operator_latency(metrics: Dict[str, Any]) -> Dict[str, float]:
    """
    Unify Storm execution_latency_* and Flink processing_latency_* into:
      latency_50, latency_95, latency_99

    If 99 is missing but 98 exists, latency_99 := latency_98.
    """
    found: Dict[str, float] = {}

    if not isinstance(metrics, dict):
        return found

    # gather any processing/execution latency percentiles
    for key, sub in metrics.items():
        if not isinstance(sub, dict):
            continue
        key_l = str(key).lower()
        if ("processing_latency" not in key_l) and ("execution_latency" not in key_l):
            continue

        m = _PCT_RE.search(str(key))
        if not m:
            continue
        pct = m.group(1)  # e.g. 50,95,98,99

        v = extract_scalar_from_nested(sub)
        if v is None:
            continue

        # store raw pct we saw
        found[f"latency_{pct}"] = v

    # normalize to 50/95/99
    out: Dict[str, float] = {}
    if "latency_50" in found:
        out["latency_50"] = found["latency_50"]
    if "latency_95" in found:
        out["latency_95"] = found["latency_95"]

    if "latency_99" in found:
        out["latency_99"] = found["latency_99"]
    elif "latency_98" in found:
        # fallback (common in your data)
        out["latency_99"] = found["latency_98"]

    return out


def build_operator_wide_row(
    base: Dict[str, Any],
    framework: str,
    operator_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    """
    One row per run.
    Creates columns:
      op1_selectivity, op1_latency_50, op1_latency_95, op1_latency_99, ...
    and name columns:
      op1_name_flink / op1_name_storm, ...
    """
    row = dict(base)

    if not isinstance(operator_metrics, dict) or not operator_metrics:
        return row

    op_names = list(operator_metrics.keys())  # insertion order preserved
    fw = framework.lower()

    for i, op_name in enumerate(op_names, start=1):
        metrics = operator_metrics.get(op_name, {})
        # framework-specific operator name column
        row[f"op{i}_name_{fw}"] = op_name

        if isinstance(metrics, dict):
            # selectivity
            sel = extract_scalar_from_nested(metrics.get("selectivity"))
            if sel is not None:
                row[f"op{i}_selectivity"] = sel

            # throughput / busy / backpressure (reported by Spark, Kafka Streams, RisingWave and some Storm runs)
            for mk in ("throughput_per_sec", "busy_ratio", "backpressure_ratio"):
                mv = extract_scalar_from_nested(metrics.get(mk))
                if mv is not None:
                    row[f"op{i}_{mk}"] = mv

            # unified latency
            lat = extract_operator_latency(metrics)
            for k, v in lat.items():
                # k is latency_50/95/99
                row[f"op{i}_{k}"] = v

    # also store how many operators were seen in this run
    row["num_operators"] = len(op_names)

    return row


# ============================================================
# Operator base builder (meta without duplicates)
# ============================================================

def build_operator_base(
    info: dict,
    data: dict,
    df_row: pd.DataFrame,
    framework: str,
    query_name: str,
    path: str,
    artifact: str,
) -> Dict[str, Any]:
    meta_flat = flatten_json(info, prefix="meta")

    # Drop meta keys that duplicate canonical columns
    drop_dupe_meta_keys = {
        "meta__job_framework",
        "meta__cluster_hardware_type",
        "meta__producer_event_per_second",
        "meta__job_query_number",
        "meta__job_parallelization",
        "meta__cluster_nodes",
        "meta__cluster_slots_per_node",
        "meta__enumeration_strategy",
        "meta__job_run_time",
        "meta__job_window_size",
        "meta__job_window_slide_size",
    }
    for k in drop_dupe_meta_keys:
        meta_flat.pop(k, None)

    job_query_number = info.get("job_query_number")
    job_name = data.get("job_name", info.get("job_name", artifact))

    def _v(col: str):
        s = df_row.get(col, pd.Series([None]))
        return s.iloc[0] if len(s) else None

    base = {
        "job_name": job_name,
        "framework": framework,
        "query": query_name,
        "job_query_number": job_query_number,
        "hardware_type": _v("hardware_type"),
        "producer_rate": _v("producer_rate"),
        "parallelism": _v("parallelism"),
        "parallelism_source": _v("parallelism_source"),
        "parallelism_avg": _v("parallelism_avg"),
        "parallelism_sink": _v("parallelism_sink"),
        "class": _v("class"),
        "class_num": _v("class_num"),
        "class_num_distinct": _v("class_num_distinct"),
        "enumeration_strategy": _v("enumeration_strategy"),
        "query_variant": _v("query_variant"),
        "path": path,
        "artifact": artifact,
    }

    base.update(meta_flat)
    return base


# ============================================================
# Artifact discovery
# ============================================================

def get_artifacts() -> Iterable[Tuple[str, str]]:
    for root, _dirs, files in os.walk("artifacts/"):
        for f in files:
            if f.startswith("evaluationMetrics") and f.endswith(".json"):
                artifact_id = f.removeprefix("evaluationMetrics-").removesuffix(".json")
                yield root, artifact_id


# ============================================================
# Main
# ============================================================

def main():
    os.chdir(_HERE)  # artifacts/ and outputs are relative to this script
    df_runs = pd.DataFrame()
    op_rows_by_query: Dict[str, List[Dict[str, Any]]] = {}

    for path, artifact in get_artifacts():
        meta_path = os.path.join(path, f"metaInfo-{artifact}.json")
        eval_path = os.path.join(path, f"evaluationMetrics-{artifact}.json")

        try:
            with open(meta_path) as f:
                info = json.load(f)
            with open(eval_path) as f:
                data = json.load(f)
        except Exception as e:
            print(f"[WARN] Failed reading {path} {artifact}: {e}")
            continue

        path_parts = path.split(os.sep)
        fw_raw = info.get("job_framework", "")

        location = step1.parse_location(path, info)
        if location is None:
            continue  # uniform_1 excluded, as in 1_data_extractor.py

        try:
            if fw_raw == "Apache Storm":
                df_row = get_storm_values(info, data, path_parts)
                framework = "storm"
            elif fw_raw == "Apache Flink":
                df_row = get_flink_values(info, data, path_parts)
                framework = "flink"
            elif fw_raw == "Apache Spark":
                df_row = step1.get_spark_values(info, data, path_parts)
                framework = "spark"
            elif fw_raw == "Apache Kafka Streams":
                df_row = step1.get_kafkastreams_values(info, data, path_parts)
                framework = "kafkastream"
            elif fw_raw == "RisingWave":
                df_row = step1.get_risingwave_values(info, data, path_parts)
                framework = "risingwave"
            else:
                print(f"[WARN] Unknown framework '{fw_raw}', skipping {artifact}")
                continue
        except (NotImplementedError, KeyError, IndexError, ValueError) as e:
            # folders that do not follow <app>/<hw>/<engine>/... (e.g. workloads_hardwares_additional/)
            print(f"[WARN] Skipping {path} {artifact}: {type(e).__name__}: {e}")
            continue
        df_row["enumeration_strategy"] = location[0]
        df_row["query_variant"] = location[1]

        # run-level append
        df_row["path"] = path
        df_row["artifact"] = artifact
        df_row["count"] = 1
        df_runs = pd.concat([df_runs, df_row], ignore_index=True)

        # operator-level WIDE row per run
        query_name = str(df_row["query"].iloc[0])

        base = build_operator_base(
            info=info,
            data=data,
            df_row=df_row,
            framework=framework,
            query_name=query_name,
            path=path,
            artifact=artifact,
        )

        op_metrics = data.get("operatorMetrics", {})
        op_wide_row = build_operator_wide_row(base, framework=framework, operator_metrics=op_metrics)
        op_rows_by_query.setdefault(query_name, []).append(op_wide_row)

    # save run-level
    df_runs = df_runs.sort_index()
    run_out = "data_20251201.csv"
    df_runs.to_csv(run_out, index=False)
    print(f"[OK] Saved run-level metrics -> {run_out} ({len(df_runs)} rows)")

    # save operator-wide per query
    os.makedirs("operator_metrics_wide", exist_ok=True)
    for q, rows in op_rows_by_query.items():
        if not rows:
            continue
        dfq = pd.DataFrame(rows)

        # stable ordering: base first, meta, then operator columns
        base_cols = [
            "job_name",
            "framework",
            "query",
            "job_query_number",
            "hardware_type",
            "producer_rate",
            "parallelism",
            "parallelism_source",
            "parallelism_avg",
            "parallelism_sink",
            "class",
            "class_num",
            "class_num_distinct",
            "enumeration_strategy",
            "query_variant",
            "num_operators",
            "path",
            "artifact",
        ]
        meta_cols = sorted([c for c in dfq.columns if c.startswith("meta__")])

        # operator columns = everything else not in base/meta
        base_set = set(base_cols)
        meta_set = set(meta_cols)
        op_cols = [c for c in dfq.columns if c not in base_set and c not in meta_set]

        # keep operator columns in a readable order:
        # op1_name_*, op1_selectivity, op1_latency_50/95/99, op2_...
        def op_sort_key(c: str):
            m = re.match(r"^op(\d+)_(.+)$", c)
            if not m:
                return (10**9, c)
            idx = int(m.group(1))
            rest = m.group(2)
            # prefer name columns first, then selectivity, then latency_*
            rank = 50
            if rest.startswith("name_"):
                rank = 0
            elif rest == "selectivity":
                rank = 10
            elif rest.startswith("latency_"):
                rank = 20
            elif rest == "throughput_per_sec":
                rank = 30
            return (idx, rank, rest)

        op_cols = sorted(op_cols, key=op_sort_key)

        ordered = [c for c in base_cols if c in dfq.columns] + meta_cols + op_cols
        dfq = dfq[ordered]

        out_path = os.path.join("operator_metrics_wide", f"operator_metrics_{safe_query_name(q)}.csv")
        dfq.to_csv(out_path, index=False)
        print(f"[OK] Saved operator WIDE metrics for '{q}' -> {out_path} ({len(dfq)} rows, {len(dfq.columns)} cols)")

    print("[DONE]")


if __name__ == "__main__":
    main()
