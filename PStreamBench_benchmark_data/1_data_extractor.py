#!/usr/bin/env python3

import os
import json
import statistics
import pandas as pd
from collections import defaultdict


#def compute_parallelism_avg(par_str: str) -> float:
#    nums = [float(v) for v in par_str.split(",")]
#    if len(nums) <= 2:
#        return statistics.fmean(nums)
#    middle = nums[1:-1]
#    return statistics.fmean(middle)

def compute_parallelism_avg(par_str: str) -> float:
    nums = [float(v) for v in par_str.split(",") if v.strip() != ""]
    if len(nums) <= 2:
        return statistics.fmean(nums)

    # Identify sinks as all trailing values equal to the last value
    sink_val = nums[-1]
    sink_count = 1
    i = len(nums) - 2
    while i >= 1 and nums[i] == sink_val:  # stop before index 0 (source)
        sink_count += 1
        i -= 1

    # Middle = exclude source and exclude all sinks
    middle_end = len(nums) - sink_count
    middle = nums[1:middle_end]

    # If nothing left (e.g., "10,1,1"), fall back to mean of all
    if not middle:
        return statistics.fmean(nums)

    return statistics.fmean(middle)

def count_trailing_sinks(par_str: str) -> int:
    nums = [float(v) for v in par_str.split(",") if v.strip() != ""]
    if len(nums) < 2:
        return 0
    sink_val = nums[-1]
    c = 1
    i = len(nums) - 2
    while i >= 1 and nums[i] == sink_val:
        c += 1
        i -= 1
    return c


def get_storm_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "storm"
    output["query"] = get_query_from_directory(path[1])
    output["parallelism"] = info["job_parallelization"]
    output["parallelism_source"] = output["parallelism"].split(",")[0]
    output["parallelism_avg"] = compute_parallelism_avg(output["parallelism"])
    output["parallelism_sink"] = output["parallelism"].split(",")[-1]
    output["parallelism_sink_count"] = count_trailing_sinks(output["parallelism"])
    output["class"], output["class_num"], output["class_num_distinct"] = get_class(output["parallelism_avg"])
    output["producer_rate"] = info["producer_event_per_second"]

    output["hardware_type"] = path[2]
    if path[2][-1] == "f":
        output["hardware_type"] = output["hardware_type"] + " (mem)"

    output["nodes"] = int(info["cluster_nodes"]) if "cluster_nodes" in info else int(path[2].split("-")[-1])
    output["slots_per_node"] = int(info["cluster_slots_per_node"]) if "cluster_slots_per_node" in info else int(path[3].split("-")[-1]) / int(output["nodes"])
    output["slots"] = output["slots_per_node"] * output["nodes"]
    output["enumeration_strategy"] = info["enumeration_strategy"] if "enumeration_strategy" in info else "Custom"
    output["time"] = info["job_run_time"]
    output["window_size"] = info["job_window_size"]
    output["window_slide"] = info["job_window_slide_size"]
    output["job_query_number"] = info.get("job_query_number")

    # Keep your existing event_rate logic (already precomputed percentiles)
    event_rate_50 = 0
    event_rate_95 = 0
    event_rate_98 = 0

    for key, value in data["queryMetrics"].items():
        if key.startswith("recPerSecOutSource") and key.endswith("50"):
            event_rate_50 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("95"):
            event_rate_95 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("98"):
            event_rate_98 += float(value)

    output["event_rate_50"] = event_rate_50
    output["event_rate_95"] = event_rate_95
    output["event_rate_98"] = event_rate_98

    # Precomputed throughput (we'll overwrite later with recomputed)
    output["throughput_50"] = float(data["queryMetrics"].get("throughput_50", pd.NA))
    output["throughput_95"] = float(data["queryMetrics"].get("throughput_95", pd.NA))
    output["throughput_98"] = float(data["queryMetrics"].get("throughput_98", pd.NA))

    output["endtoend_latency"] = \
        float(data["queryMetrics"]["endtoend_latency_storm"])

    output = output | get_hardware_values(info, data)
    return pd.DataFrame([output])


def get_flink_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "flink"
    output["query"] = get_query_from_directory(path[1])
    output["parallelism"] = info["job_parallelization"]
    output["parallelism_source"] = output["parallelism"].split(",")[0]
    output["parallelism_avg"] = compute_parallelism_avg(output["parallelism"])
    output["parallelism_sink"] = output["parallelism"].split(",")[-1]
    output["parallelism_sink_count"] = count_trailing_sinks(output["parallelism"])
    output["class"], output["class_num"], output["class_num_distinct"] = get_class(output["parallelism_avg"])
    output["producer_rate"] = info["producer_event_per_second"]

    output["hardware_type"] = path[2]
    if path[2][-1] == "f":
        output["hardware_type"] = output["hardware_type"] + " (mem)"

    output["nodes"] = int(info["cluster_nodes"]) if "cluster_nodes" in info else int(path[2].split("-")[-1])
    output["slots_per_node"] = int(info["cluster_slots_per_node"]) if "cluster_slots_per_node" in info else int(path[3].split("-")[-1]) / int(output["nodes"])
    output["slots"] = output["slots_per_node"] * output["nodes"]
    output["enumeration_strategy"] = info["enumeration_strategy"] if "enumeration_strategy" in info else "Custom"
    output["time"] = info["job_run_time"]
    output["window_size"] = info["job_window_size"]
    output["window_slide"] = info["job_window_slide_size"]
    output["job_query_number"] = info.get("job_query_number")

    # Keep your existing event_rate logic (already precomputed percentiles)
    event_rate_50 = 0
    event_rate_95 = 0
    event_rate_98 = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("recPerSecOutSource") and key.endswith("50"):
            event_rate_50 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("95"):
            event_rate_95 += float(value)
        elif key.startswith("recPerSecOutSource") and key.endswith("98"):
            event_rate_98 += float(value)

    output["event_rate_50"] = event_rate_50
    output["event_rate_95"] = event_rate_95
    output["event_rate_98"] = event_rate_98

    # Precomputed throughput (we'll overwrite later with recomputed)
    output["throughput_50"] = float(data["queryMetrics"].get("throughput_50", pd.NA))
    output["throughput_95"] = float(data["queryMetrics"].get("throughput_95", pd.NA))
    output["throughput_98"] = float(data["queryMetrics"].get("throughput_98", pd.NA))

    latency = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("endtoend_latency") and key.endswith("50"):
            latency += float(value)
    output["endtoend_latency"] = latency

    output = output | get_hardware_values(info, data)
    return pd.DataFrame([output])


def get_class(n: int) -> str:
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


def get_hardware_values(info: dict, data: dict) -> dict:
    output = {}

    cpu_50 = float(data["hardwareMetrics"].get("node_cpu_50", 0))
    cpu_95 = float(data["hardwareMetrics"].get("node_cpu_95", 0))
    cpu_98 = float(data["hardwareMetrics"].get("node_cpu_98", 0))
    output["cpu_50"] = cpu_50
    output["cpu_95"] = cpu_95
    output["cpu_98"] = cpu_98

    memory_50 = float(data["hardwareMetrics"].get("node_memory_50", 0))
    memory_95 = float(data["hardwareMetrics"].get("node_memory_95", 0))
    memory_98 = float(data["hardwareMetrics"].get("node_memory_98", 0))
    output["memory_50"] = memory_50
    output["memory_95"] = memory_95
    output["memory_98"] = memory_98

    network_50 = float(data["hardwareMetrics"].get("node_network_out_50", 0))
    network_95 = float(data["hardwareMetrics"].get("node_network_out_95", 0))
    network_98 = float(data["hardwareMetrics"].get("node_network_out_98", 0))
    output["network_50"] = network_50
    output["network_95"] = network_95
    output["network_98"] = network_98

    return output


def get_artifacts() -> list[str]:
    for root, dirs, files in os.walk("artifacts/"):
        for file in files:
            if file.startswith("evaluationMetrics"):
                yield (root, file.removeprefix("evaluationMetrics-").removesuffix(".json"))


# ---------- NEW: throughput recomputation utilities ----------

def extract_prom_series(obj, agg="sum", ts_round=3):
    """
    Extracts a single time-ordered series from Prometheus-style payload:
      obj = [ { "metric": {...}, "values": [[ts, "v"], ...] }, ... ]
    If multiple series exist (e.g., many subtasks), aggregate per timestamp.
    """
    if not isinstance(obj, list) or len(obj) == 0:
        return [], []

    buckets = defaultdict(list)  # ts_key -> [v1, v2, ...]
    for series in obj:
        for ts, v in series.get("values", []):
            try:
                ts_key = round(float(ts), ts_round)
                val = float(v)
                buckets[ts_key].append(val)
            except (ValueError, TypeError):
                continue

    if not buckets:
        return [], []

    timestamps = []
    values = []
    for ts_key in sorted(buckets.keys()):
        vs = buckets[ts_key]
        if agg == "mean":
            values.append(sum(vs) / len(vs))
        else:
            values.append(sum(vs))
        timestamps.append(ts_key)

    return timestamps, values


def trim_leading_trailing_zeros(timestamps, values):
    """Trim warmup (leading) and cooldown (trailing) zeros only."""
    if not values:
        return [], [], 0, 0

    first = None
    for i, v in enumerate(values):
        if v != 0:
            first = i
            break
    if first is None:
        return [], [], len(values), 0

    last = None
    for i in range(len(values) - 1, -1, -1):
        if values[i] != 0:
            last = i
            break

    lead = first
    trail = (len(values) - 1) - last
    return timestamps[first:last + 1], values[first:last + 1], lead, trail


def compute_percentiles_sorted(values, qs=(0.50, 0.95, 0.98)):
    """
    Sort explicitly, then compute quantiles using pandas interpolation='linear'
    (robust, matches common "continuous" percentile behavior).
    """
    if not values:
        return {q: pd.NA for q in qs}

    s = pd.Series(sorted(values), dtype="float64")
    out = s.quantile(list(qs), interpolation="linear")
    return {q: float(out.loc[q]) for q in qs}


def recompute_and_store_throughput(data, out_dir, artifact, agg="sum"):
    """
    Returns:
      p50,p95,p98,n,lead_zeros,trail_zeros,out_file,trimmed_values
    Also writes trimmed series to CSV for inspection.
    """
    throughput_obj = data.get("queryMetrics", {}).get("throughput")
    ts, vals = extract_prom_series(throughput_obj, agg=agg)

    ts_trim, vals_trim, lead, trail = trim_leading_trailing_zeros(ts, vals)
    pct = compute_percentiles_sorted(vals_trim, qs=(0.50, 0.95, 0.98))

    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"throughputValues-{artifact}.csv")

    pd.DataFrame({"timestamp": ts_trim, "throughput": vals_trim}).to_csv(out_file, index=False)

    return {
        "throughput_50": pct[0.50],
        "throughput_95": pct[0.95],
        "throughput_98": pct[0.98],
        "throughput_n": len(vals_trim),
        "throughput_trim_leading": lead,
        "throughput_trim_trailing": trail,
        "throughput_values_file": out_file,
        "throughput_values_trimmed": vals_trim,  # for optional printing
    }


def main():
    df = pd.DataFrame()

    for root_path, artifact in get_artifacts():
        print(os.path.join(root_path, artifact))

        with open(os.path.join(root_path, "metaInfo-" + artifact + ".json")) as f:
            info = json.load(f)
        with open(os.path.join(root_path, "evaluationMetrics-" + artifact + ".json")) as f:
            data = json.load(f)

        if info["job_framework"] == "Apache Storm":
            output = get_storm_values(info, data, root_path.split("/"))
        elif info["job_framework"] == "Apache Flink":
            output = get_flink_values(info, data, root_path.split("/"))
        else:
            continue

        # Keep original precomputed throughput for comparison
        output["throughput_50_precomputed"] = output.get("throughput_50", pd.NA)
        output["throughput_95_precomputed"] = output.get("throughput_95", pd.NA)
        output["throughput_98_precomputed"] = output.get("throughput_98", pd.NA)

        # NEW: recompute throughput percentiles after trimming warmup/cooldown zeros
        thr = recompute_and_store_throughput(
            data=data,
            out_dir=root_path,     # store next to the metrics files
            artifact=artifact,
            agg="sum"              # sum across subtasks if multiple series exist
        )

        # overwrite with recomputed values
        output["throughput_50"] = thr["throughput_50"]
        output["throughput_95"] = thr["throughput_95"]
        output["throughput_98"] = thr["throughput_98"]
        output["throughput_n"] = thr["throughput_n"]
        output["throughput_trim_leading"] = thr["throughput_trim_leading"]
        output["throughput_trim_trailing"] = thr["throughput_trim_trailing"]
        output["throughput_values_file"] = thr["throughput_values_file"]

        # Optional: print a quick summary + a short preview of values
        # (comment out if too verbose)
        preview = thr["throughput_values_trimmed"][:10]
        print(f"  throughput samples used: {thr['throughput_n']} (trim lead={thr['throughput_trim_leading']}, trail={thr['throughput_trim_trailing']})")
        print(f"  p50={thr['throughput_50']}, p95={thr['throughput_95']}, p98={thr['throughput_98']}")
        print(f"  values preview: {preview} ... saved to {thr['throughput_values_file']}")

        output["path"] = root_path
        output["artifact"] = artifact
        output["count"] = 1

        df = pd.concat([df, output], ignore_index=True)

    df = df.sort_index()
    df.to_csv("query_performance_data.csv", index=False)


if __name__ == "__main__":
    main()