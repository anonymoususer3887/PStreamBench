#!/usr/bin/env python3

import os
import json
import statistics
import pandas as pd


def get_storm_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "storm"
    output["query"] = get_query_from_directory(path[1])
    output["parallelism"] = info["job_parallelization"]
    output["parallelism_avg"] = statistics.fmean([float(v) for v in output["parallelism"].split(",")[0:-1]])
    output["parallelism_sink"] = output["parallelism"].split(",")[-1]
    output["class"], output["class_num"] = get_class(output["parallelism_avg"])
    output["producer_rate"] = info["producer_event_per_second"]
    output["hardware_type"] = info["cluster_hardware_type"] \
    output["nodes"] = int(info["cluster_nodes"]) \
    output["slots_per_node"] = int(info["cluster_slots_per_node"]) \
    output["slots"] = output["slots_per_node"] * output["nodes"]
    output["enumeration_strategy"] = info["enumeration_strategy"] \
            if "enumeration_strategy" in info \
            else pd.NA
    output["time"] = info["job_run_time"]

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

    throughput_50 = 0
    throughput_95 = 0
    throughput_98 = 0

    for key, value in data["queryMetrics"].items():
        if key.startswith("throughput") and key.endswith("50"):
            throughput_50 += float(value)
        elif key.startswith("throughput") and key.endswith("95"):
            throughput_95 += float(value)
        elif key.startswith("throughput") and key.endswith("98"):
            throughput_98 += float(value)

    output["throughput_50"] = throughput_50
    output["throughput_95"] = throughput_95
    output["throughput_98"] = throughput_98

    output["endtoend_latency"] = \
        float(data["queryMetrics"]["endtoend_latency_storm"])

    output = {**output, **get_hardware_values(info, data)}

    return pd.DataFrame([output])


def get_flink_values(info: dict, data: dict, path: list) -> dict:
    output = {}

    output["framework"] = "flink"
    output["query"] = get_query_from_directory(path[1])
    output["parallelism"] = info["job_parallelization"]
    output["parallelism_avg"] = statistics.fmean([float(v) for v in output["parallelism"].split(",")[0:-1]])
    output["parallelism_sink"] = output["parallelism"].split(",")[-1]
    output["class"], output["class_num"] = get_class(output["parallelism_avg"])
    output["producer_rate"] = info["producer_event_per_second"]
    output["hardware_type"] = info["cluster_hardware_type"] \
    output["nodes"] = int(info["cluster_nodes"]) \
    output["slots_per_node"] = int(info["cluster_slots_per_node"]) \
    output["slots"] = output["slots_per_node"] * output["nodes"]
    output["enumeration_strategy"] = info["enumeration_strategy"] \
            if "enumeration_strategy" in info \
            else pd.NA
    output["time"] = info["job_run_time"]

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

    throughput_50 = 0
    throughput_95 = 0
    throughput_98 = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("throughput") and key.endswith("50"):
            throughput_50 += float(value)
        elif key.startswith("throughput") and key.endswith("95"):
            throughput_95 += float(value)
        elif key.startswith("throughput") and key.endswith("98"):
            throughput_98 += float(value)
    output["throughput_50"] = throughput_50
    output["throughput_95"] = throughput_95
    output["throughput_98"] = throughput_98

    latency = 0
    for key, value in data["queryMetrics"].items():
        if key.startswith("endtoend_latency") and key.endswith("50"):
            latency += float(value)
    output["endtoend_latency"] = latency

    output = {**output, **get_hardware_values(info, data)}

    return pd.DataFrame([output])


def get_class(n: int) -> str:
    if n < 10:
        return ("XS", 1)
    elif n < 20:
        return ("S", 2)
    elif n < 30:
        return ("M", 3)
    elif n < 40:
        return ("L", 4)
    elif n < 60:
        return ("XL", 5)
    else:
        return ("XXL", 6)

def get_query_from_directory(directory: str) -> str:
    if "word-count" == directory:
        return "Word Count"
    elif "smart-grid" == directory or "Smart Grid" == directory:
        return "Smart Grid"
    elif "ad-analytics" == directory:
        return "Ad Analytics"
    elif "spike-detection" == directory or "Spike Detection" == directory:
        return "Spike Detection"
    else:
        raise NotImplementedError("Directory \"" + directory + "\" is not assigned a query name.")


def get_hardware_values(info: dict, data: dict) -> dict:
    output = {}

    cpu_50 = 0
    cpu_95 = 0
    cpu_98 = 0
    for key, value in data["hardwareMetrics"].items():
        if key == "node_cpu_50":
            cpu_50 += float(value)
        elif key == "node_cpu_95":
            cpu_95 += float(value)
        elif key == "node_cpu_98":
            cpu_98 += float(value)
    output["cpu_50"] = cpu_50
    output["cpu_95"] = cpu_95
    output["cpu_98"] = cpu_98

    memory_50 = 0
    memory_95 = 0
    memory_98 = 0
    for key, value in data["hardwareMetrics"].items():
        if key == "node_memory_50":
            memory_50 += float(value)
        elif key == "node_memory_95":
            memory_95 += float(value)
        elif key == "node_memory_98":
            memory_98 += float(value)
    output["memory_50"] = memory_50
    output["memory_95"] = memory_95
    output["memory_98"] = memory_98

    network_50 = 0
    network_95 = 0
    network_98 = 0
    for key, value in data["hardwareMetrics"].items():
        if key == "node_network_out_50":
            network_50 += float(value)
        elif key == "node_network_out_95":
            network_95 += float(value)
        elif key == "node_network_out_98":
            network_98 += float(value)
    output["network_50"] = network_50
    output["network_95"] = network_95
    output["network_98"] = network_98

    return output


def get_artifacts() -> list:
    for root, dirs, files in os.walk("./"):
        for file in files:
            if file.startswith("evaluationMetrics"):
                yield (root,
                       remove_suffix(remove_prefix(file, "evaluationMetrics-"),
                                     ".json"))

def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]

def remove_suffix(text, suffix):
    return text[:-len(suffix)] if text.endswith(suffix) and len(suffix) != 0 else text


def main():
    df = pd.DataFrame()

    for path, artifact in get_artifacts():
        print(os.path.join(path, artifact))
        info = {}
        data = {}
        with open(os.path.join(path, "metaInfo-" + artifact +
                               ".json")) as f:
            info = json.load(f)
        with open(os.path.join(path, "evaluationMetrics-" + artifact +
                               ".json")) as f:
            data = json.load(f)

        output = {}
        if info["job_framework"] == "Apache Storm":
            output = get_storm_values(info, data, path.split("/"))
        elif info["job_framework"] == "Apache Flink":
            output = get_flink_values(info, data, path.split("/"))
        output["path"] = path
        output["artifact"] = artifact
        output["count"] = 1
        df = pd.concat([df, output], ignore_index=True)

    df = df.sort_index()
    df.to_csv('artifacts.csv', index=False)


if __name__ == '__main__':
    main()
