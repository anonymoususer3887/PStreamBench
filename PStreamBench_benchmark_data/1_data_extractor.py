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


def get_spark_endtoend_latency_seconds(data: dict) -> float:
    """Spark has no engine-native marker-based latency the way Flink/Storm
    do (see SPARK_VALIDATION.md) -- our own Tier 3 instrumentation reports
    it under endToEndLatency as one or more "*_seconds_50th"-suffixed keys.
    Ad Analytics Query 1 has two independent sources (clicks/impressions),
    so it reports two such keys instead of one; summing all matches mirrors
    get_flink_values' own "sum every endtoend_latency*50 key found" loop
    below, so a query with multiple lag sources is handled the same way on
    both engines.
    """
    total = 0.0
    for key, value in data.get("endToEndLatency", {}).items():
        if "end_to_end_latency_seconds" in key and key.endswith("_50th") and value is not None:
            total += float(value)
    return total


def get_spark_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "spark"
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

    # event_rate_* <- sparkStreamingMetrics.input_rate_*th, Spark's own
    # rows-arriving-per-second gauge -- the same semantic as Flink's
    # recPerSecOutSource/Storm's recPerSecOutSource_spout (records read at
    # the source per second). queryMetrics' Kafka-JMX-derived event rate is
    # deliberately not used here: its "_50th" is unreliable (near-always 0
    # in practice), unlike Spark's own streaming-query metric.
    ssm = data.get("sparkStreamingMetrics", {})
    output["event_rate_50"] = float(ssm.get("input_rate_50th", 0) or 0)
    output["event_rate_95"] = float(ssm.get("input_rate_95th", 0) or 0)
    output["event_rate_98"] = float(ssm.get("input_rate_98th", 0) or 0)

    # throughput_* <- sparkStreamingMetrics.processing_rate_*th, Spark's own
    # rows-processed-per-second gauge -- the closest analog to Flink/Storm's
    # "throughput" (records out per second). Unlike Flink/Storm, there is no
    # raw per-timestamp series behind this (just precomputed percentiles),
    # so these are NOT overwritten by recompute_and_store_throughput() the
    # way Flink/Storm rows are -- see the framework check in main().
    output["throughput_50"] = float(ssm.get("processing_rate_50th", 0) or 0)
    output["throughput_95"] = float(ssm.get("processing_rate_95th", 0) or 0)
    output["throughput_98"] = float(ssm.get("processing_rate_98th", 0) or 0)

    # endtoend_latency <- endToEndLatency, converted seconds -> ms to match
    # Flink/Storm's existing unit in this column (confirmed from real data:
    # Flink's endtoend_latency_50 and Storm's endtoend_latency_storm are
    # both in milliseconds, e.g. ~62381 and ~913 respectively).
    output["endtoend_latency"] = get_spark_endtoend_latency_seconds(data) * 1000.0

    output = output | get_hardware_values(info, data)
    return pd.DataFrame([output])


def get_kafkastreams_values(info: dict, data: dict, path: list) -> dict:
    """Kafka Streams has no raw per-timestamp series behind its throughput/
    event-rate numbers (unlike Flink/Storm's queryMetrics.throughput) and no
    percentile breakdown of its own -- each collected value is already a
    single 5-minute time-averaged number (see
    PStreamBench_controller/dsp_be/infra/reporting_scripts/
    getprom_kafkastreams.py), so this follows the same "use precomputed
    values as-is, don't attempt recompute_and_store_throughput" treatment as
    get_spark_values() below.
    """
    output = {}

    output["framework"] = "kafkastream"
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

    # event_rate_* <- queryMetrics.kafka_event_rate_*th, the broker-level
    # (Tier 1) JMX event-rate gauge -- same "records arriving per second at
    # the source" semantic as Flink/Storm's recPerSecOutSource. Best-effort:
    # this tier is known to go null under heavy per-client-id quota-bean
    # scrape load at high parallelism/event-rate (see benchmark session
    # notes) -- fall back to 0 rather than raising when absent.
    qm = data.get("queryMetrics", {})

    def _qm(key, subkey):
        v = qm.get(key, {}).get(subkey)
        return float(v) if v is not None else 0.0

    output["event_rate_50"] = _qm("kafka_event_rate_50th", "event rate 50th")
    output["event_rate_95"] = _qm("kafka_event_rate_95th", "event rate 95th")
    output["event_rate_98"] = _qm("kafka_event_rate_98th", "event rate 98th")

    # throughput_* <- operatorMetrics.sink.throughput_per_sec, our own Tier 3
    # OperatorMetricsProcessor sensor (see OperatorMetricsProcessor.java) --
    # the most reliable throughput signal collected for Kafka Streams jobs,
    # unlike Tier 1's kafka_throughput_*th (frequently null). It's a single
    # 5-minute time-averaged value with no independent percentile
    # breakdown, so all three columns get the same number -- same treatment
    # as Spark's precomputed sparkStreamingMetrics percentiles.
    sink_tp = data.get("operatorMetrics", {}).get("sink", {}).get("throughput_per_sec")
    sink_tp = float(sink_tp) if sink_tp is not None else 0.0
    output["throughput_50"] = sink_tp
    output["throughput_95"] = sink_tp
    output["throughput_98"] = sink_tp

    # endtoend_latency <- streamsMetrics.end_to_end_latency_avg, Kafka
    # Streams' own native KIP-613 end-to-end latency metric (Tier 2),
    # already in milliseconds, matching Flink/Storm/Spark's existing unit
    # in this column.
    sm = data.get("streamsMetrics", {})
    latency = sm.get("end_to_end_latency_avg")
    output["endtoend_latency"] = float(latency) if latency is not None else 0.0

    output = output | get_hardware_values(info, data)
    return pd.DataFrame([output])


def get_risingwave_values(info: dict, data: dict, path: list) -> dict:
    """RisingWave has no raw per-timestamp queryMetrics.throughput series to
    recompute from (unlike Flink/Storm -- see get_kafkastreams_values'/
    get_spark_values' same caveat), and no per-record end-to-end latency
    metric even in principle for an aggregating query (COUNT(*)/GROUP BY
    loses per-row identity -- see getprom_risingwave.py's getActorMetrics
    docstring). Both this function's throughput and latency choices follow
    the same "prefer the most reliable available Tier 3/2 signal" reasoning
    already established for Kafka Streams.
    """
    output = {}

    output["framework"] = "risingwave"
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

    # event_rate_* <- queryMetrics.kafka_event_rate_*th, the broker-level
    # (Tier 1) JMX event-rate gauge on the INPUT topic -- same "records
    # arriving per second at the source" semantic as Flink/Storm's
    # recPerSecOutSource/Kafka Streams' kafka_event_rate.
    qm = data.get("queryMetrics", {})

    def _qm(key, subkey):
        v = qm.get(key, {}).get(subkey)
        return float(v) if v is not None else 0.0

    output["event_rate_50"] = _qm("kafka_event_rate_50th", "event rate 50th")
    output["event_rate_95"] = _qm("kafka_event_rate_95th", "event rate 95th")
    output["event_rate_98"] = _qm("kafka_event_rate_98th", "event rate 98th")

    # throughput_* <- operatorMetrics.sink.throughput_per_sec, RisingWave's
    # own Tier 3 fragment-level metric (see getOperatorMetrics in
    # getprom_risingwave.py) -- preferred over Tier 1's
    # kafka_throughput_*th (the sink's Kafka-JMX output rate) for the same
    # reliability reason get_kafkastreams_values documents for its sibling
    # Tier 1 metric. A single 5-minute time-averaged value with no
    # independent percentile breakdown, so all three columns get the same
    # number -- same treatment as Spark/Kafka Streams' precomputed values.
    sink_tp = data.get("operatorMetrics", {}).get("sink", {}).get("throughput_per_sec")
    sink_tp = float(sink_tp) if sink_tp is not None else 0.0
    output["throughput_50"] = sink_tp
    output["throughput_95"] = sink_tp
    output["throughput_98"] = sink_tp

    # endtoend_latency <- actorMetrics.barrier_inflight_latency_avg, the
    # meta node's own barrier (checkpoint) round-trip duration -- the
    # closest native processing-latency signal RisingWave exposes for an
    # aggregating query (no per-record e2e latency is possible once
    # COUNT(*)/GROUP BY loses row identity). Underlying Prometheus metric
    # is meta_barrier_duration_seconds, so convert seconds -> ms to match
    # every other engine's existing unit in this column (Flink's
    # endtoend_latency_50, Storm's endtoend_latency_storm, and Spark's
    # explicit *1000.0 conversion are all in milliseconds).
    am = data.get("actorMetrics", {})
    latency_s = am.get("barrier_inflight_latency_avg")
    output["endtoend_latency"] = float(latency_s) * 1000.0 if latency_s is not None else 0.0

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


def _hw(hardware_metrics: dict, key: str) -> float:
    # RisingWave (see BUGS_AND_FIXES.md bug #19) writes an explicit `null`
    # for a metric it failed to collect, rather than omitting the key --
    # plain .get(key, 0) only supplies the default when the key is absent,
    # so a present-but-None value still reaches float(None) and raises.
    value = hardware_metrics.get(key)
    return float(value) if value is not None else 0.0


def get_hardware_values(info: dict, data: dict) -> dict:
    output = {}
    hw = data["hardwareMetrics"]

    output["cpu_50"] = _hw(hw, "node_cpu_50")
    output["cpu_95"] = _hw(hw, "node_cpu_95")
    output["cpu_98"] = _hw(hw, "node_cpu_98")

    output["memory_50"] = _hw(hw, "node_memory_50")
    output["memory_95"] = _hw(hw, "node_memory_95")
    output["memory_98"] = _hw(hw, "node_memory_98")

    output["network_50"] = _hw(hw, "node_network_out_50")
    output["network_95"] = _hw(hw, "node_network_out_95")
    output["network_98"] = _hw(hw, "node_network_out_98")

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


STRATEGY_FOLDERS = {"uniform", "random", "ds2", "dhalion"}

# Verified against each engine's source (Flink ClickAnalytics.java,
# Kafka Streams ClickAnalyticsTopology.java): Click Analytics Q1 is the
# time window, Q2 geo, Q3 the count window. Keyed by job_query_number,
# not by folder name, since folder names were mislabeled in earlier
# migrations.
QUERY_VARIANT = {
    "ad-analytics": {"1": "with_join", "2": "without_join"},
    "click-analytics": {"1": "time-based", "2": "geo-based", "3": "count-based"},
    "google-cloud-monitoring": {"1": "cpu-per-category", "2": "cpu-per-job"},
    "linear-road": {"1": "toll-notification", "2": "accident-detection",
                    "3": "daily-expenditure", "4": "travel-time"},
    "smart-grid": {"1": "global-avg-load", "2": "local-avg-load"},
    "tpch": {"1": "without_window", "2": "with_window"},
}


def parse_location(root_path: str, info: dict):
    """Returns (strategy, query_variant), or None to skip the artifact.

    Layout: artifacts/<slug>/<hw>/<engine>/[<variant>/][<strategy>/]<rate>/
    The strategy is taken from the folder name, because metaInfo's
    enumeration_strategy is absent or "Custom" for our strategy sweeps.
    Folders named uniform_1 are excluded.
    """
    parts = root_path.split(os.sep)
    if "uniform_1" in parts:
        return None
    slug = parts[1]
    rest = parts[4:]
    strategy = next((p for p in rest if p in STRATEGY_FOLDERS), None)
    folder_variant = None
    if rest and rest[0] not in STRATEGY_FOLDERS and not rest[0].isdigit():
        folder_variant = rest[0]
    qn = str(info.get("job_query_number", ""))
    variant = QUERY_VARIANT.get(slug, {}).get(qn) or folder_variant or ""
    return (strategy or "uniform"), variant


def main():
    df = pd.DataFrame()

    for root_path, artifact in get_artifacts():
        print(os.path.join(root_path, artifact))

        with open(os.path.join(root_path, "metaInfo-" + artifact + ".json")) as f:
            info = json.load(f)
        with open(os.path.join(root_path, "evaluationMetrics-" + artifact + ".json")) as f:
            data = json.load(f)

        location = parse_location(root_path, info)
        if location is None:
            print("  SKIPPED (uniform_1 excluded)")
            continue

        try:
            if info["job_framework"] == "Apache Storm":
                output = get_storm_values(info, data, root_path.split("/"))
            elif info["job_framework"] == "Apache Flink":
                output = get_flink_values(info, data, root_path.split("/"))
            elif info["job_framework"] == "Apache Spark":
                output = get_spark_values(info, data, root_path.split("/"))
            elif info["job_framework"] == "Apache Kafka Streams":
                output = get_kafkastreams_values(info, data, root_path.split("/"))
            elif info["job_framework"] == "RisingWave":
                output = get_risingwave_values(info, data, root_path.split("/"))
            else:
                continue
        except (NotImplementedError, KeyError, IndexError) as e:
            # Folders like workloads_hardwares_additional/ and
            # query_and_operator_metrics/ don't follow the standard
            # <app>/<hardware>/<engine>/... layout (different nesting,
            # sometimes missing metaInfo fields entirely) -- skip rather
            # than aborting the whole extraction.
            print(f"  SKIPPED ({type(e).__name__}: {e})")
            continue

        output["enumeration_strategy"] = location[0]
        output["query_variant"] = location[1]

        # Keep original precomputed throughput for comparison
        output["throughput_50_precomputed"] = output.get("throughput_50", pd.NA)
        output["throughput_95_precomputed"] = output.get("throughput_95", pd.NA)
        output["throughput_98_precomputed"] = output.get("throughput_98", pd.NA)

        # Spark, Kafka Streams, and RisingWave have no raw per-timestamp
        # queryMetrics.throughput series to recompute from (see
        # get_spark_values / get_kafkastreams_values / get_risingwave_values)
        # -- keep their precomputed throughput_50/95/98 as final values
        # rather than overwriting them with the Flink/Storm-only recompute.
        if info["job_framework"] in ("Apache Spark", "Apache Kafka Streams", "RisingWave"):
            output["throughput_n"] = pd.NA
            output["throughput_trim_leading"] = pd.NA
            output["throughput_trim_trailing"] = pd.NA
            output["throughput_values_file"] = pd.NA
            fw_label = {"Apache Spark": "spark", "Apache Kafka Streams": "kafkastream", "RisingWave": "risingwave"}[info["job_framework"]]
            print(f"  ({fw_label}: using precomputed throughput) p50={output['throughput_50'].iloc[0]}, "
                  f"p95={output['throughput_95'].iloc[0]}, p98={output['throughput_98'].iloc[0]}")
        else:
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