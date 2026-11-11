import os
import click
import json
import requests
import time
import urllib.parse

from typing import List


def getBoltMetrics(parallelization, main_node_ip, job_id, operatorId,
                   operatorName, timestamp_prom):
    dict_response = {}

    print("bolt: " + operatorName)

    # parallelism degree
    # prom_endpoint_Selectivity = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{job_id=\"" + job_id + "\",operator_id=\""+operatorId+"\"}[5m] @ " +timestamp_prom+")%20%2F%20avg_over_time(flink_taskmanager_job_task_operator_numRecordsInPerSecond{job_id=\"" + job_id + "\",operator_id=\""+operatorId+"\"}[5m] @ " +timestamp_prom+")"
    # response = requests.get(prom_endpoint_Selectivity)
    # data = response.json()
    # pd = len(data["data"]["result"])

    # selectivity
    prom_endpoint_Selectivity = "http://" + main_node_ip + ":9090/api/v1/query?query=(bolts_emitted{TopologyId='"+job_id+"',BoltId='"+operatorId+"'} / on() group_left() uptime_seconds{TopologyId='"+job_id+"'}) / (bolts_executed{TopologyId='"+job_id+"',BoltId='"+operatorId+"'} / on() group_left() uptime_seconds{TopologyId='"+job_id+"'})"
    response = requests.get(prom_endpoint_Selectivity)
    data = response.json()
    # Confirmed live, 2026-10-25: a bolt with zero executed/emitted
    # tuples in the query window (legitimate right after a cluster
    # restart, or at low parallelism/low rate) makes this division
    # query return an empty Prometheus result vector rather than one
    # with a NaN/0 value -- result[0] then raised a bare IndexError
    # that crashed the whole collect() call for the combo (TwoFilters/
    # 100000/level2+4 confirmed hit by this), same class of bug as the
    # one already guarded below for "capacity".
    try:
        selectivity_avg = data["data"]["result"][0]["value"][1]
    except (IndexError, KeyError):
        selectivity_avg = None
    response_data = {"Selectivity": selectivity_avg}
    dict_response["selectivity"] = response_data

    # throughput_per_sec -- executed-tuples rate, same
    # bolts_executed/uptime_seconds averaging as the selectivity query
    # above (Storm's REST API reports emitted/executed as counts over its
    # own default stats window, not a raw Prometheus counter, so dividing
    # by uptime_seconds -- not rate() -- is what the rest of this file
    # already does elsewhere to get a per-second figure).
    prom_endpoint_Throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=bolts_executed{TopologyId='"+job_id+"',BoltId='"+operatorId+"'} / on() group_left() uptime_seconds{TopologyId='"+job_id+"'}"
    response = requests.get(prom_endpoint_Throughput)
    data = response.json()
    try:
        throughput_per_sec = float(data["data"]["result"][0]["value"][1])
    except (IndexError, KeyError):
        throughput_per_sec = 0.0
    dict_response["throughput_per_sec"] = throughput_per_sec

    # busy_ratio / backpressure_ratio -- derived from Storm's native
    # "capacity" metric (num executed * avg execute latency / time
    # window), the same "fraction of time spent doing useful work"
    # concept ds2_strategy/dhalion_strategy already consume for
    # RisingWave/Spark (see parallelism_strategies.py's DS2/Dhalion
    # docstrings). capacity can exceed 1.0 when a bolt is fully
    # saturated and falling behind (it needed more than the whole
    # window to process what arrived) -- busy_ratio clamps that to 1.0
    # (matches DS2's [0,1] "fraction of time busy" assumption), while
    # backpressure_ratio keeps the overshoot as
    # max(0, 1 - 1/capacity): capacity=1.25 (25% over capacity) maps to
    # backpressure_ratio=0.2, i.e. Dhalion's scale-up resolver would
    # then propose new_p = ceil(current_p / (1 - 0.2)) = 1.25x -- the
    # same relationship the exporter's own metric name promises.
    prom_endpoint_Capacity = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(bolts_capacity{TopologyId=\"" + job_id + "\", BoltId=\""+operatorId+"\"}[5m] @ " +timestamp_prom+")"
    response = requests.get(prom_endpoint_Capacity)
    data = response.json()
    try:
        capacity = float(data["data"]["result"][0]["value"][1])
    except (IndexError, KeyError):
        capacity = 0.0
    dict_response["busy_ratio"] = min(1.0, capacity)
    dict_response["backpressure_ratio"] = max(0.0, 1.0 - 1.0 / capacity) if capacity > 0 else 0.0

    # latency 98th
    prom_endpoint_Latency_98th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.98, bolts_execute_latency{TopologyId=\"" + job_id + "\", BoltId=\""+operatorId+"\"}[5m] @ " +timestamp_prom+")"
    response = requests.get(prom_endpoint_Latency_98th)
    data = response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    response_data = {"Latency_98th": data}
    dict_response["execution_latency_98th"] = response_data

    # latency 95th
    prom_endpoint_Latency_95th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.95, bolts_execute_latency{TopologyId=\"" + job_id + "\", BoltId=\""+operatorId+"\"}[5m] @ " +timestamp_prom+")"
    response = requests.get(prom_endpoint_Latency_95th)
    data = response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    response_data = {"Latency_95th": data}
    dict_response["execution_latency_95th"] = response_data

    # latency 50th
    prom_endpoint_Latency_50th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.50, bolts_execute_latency{TopologyId=\"" + job_id + "\", BoltId=\""+operatorId+"\"}[5m] @ " +timestamp_prom+")"
    response = requests.get(prom_endpoint_Latency_50th)
    data = response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    response_data = {"Latency_50th": data}
    dict_response["execution_latency_50th"] = response_data

    return dict_response


def getQueryMetrics(main_node_ip, job_id, timestamp_prom, spouts):
    dictReturn = {}

    for spout in spouts:
        # records out -- was rate(spouts_emitted[1m])[5m:1m], a narrow-window
        # rate query on Storm's own periodically-reported (not continuously
        # incrementing) emitted count. Confirmed via live audit (storm_
        # spout_vs_bolt.csv, 2026-10-25): this read 0 at p50 in 63 of 489
        # runs, mostly at 500k/1m event rates, while the SAME runs' bolts
        # (using the division-by-uptime pattern below, never rate()) showed
        # 1000-160000 rows/s -- i.e. the pipeline was fine, only this one
        # query's narrow rate window was dropping out under the increased
        # scrape/reporting load that comes with higher event rates. Switched
        # to the identical uptime-division pattern getBoltMetrics() already
        # uses for bolts_executed, for the same robustness: a cumulative
        # count over cumulative uptime only needs ONE scrape of two gauges,
        # not several back-to-back 1-minute windows all landing cleanly.
        eventrate_query = "(spouts_emitted{TopologyId=\"" + job_id + "\" , SpoutId='" + spout + "'} / on() group_left() uptime_seconds{TopologyId=\"" + job_id + "\"})[5m:1s] @ " + timestamp_prom
        prom_endpoint_recPerSecOutSource = "http://" + main_node_ip + ":9090/api/v1/query?query=" + eventrate_query
        recPerSecOutSource_resp = requests.get(prom_endpoint_recPerSecOutSource)
        data = recPerSecOutSource_resp.json()
        data = data['data']['result']
        dictReturn["recPerSecOutSource_" + spout] = data

        prom_endpoint_recPerSecOutSource_max = "http://" + main_node_ip + ":9090/api/v1/query?query=max_over_time(" + eventrate_query + ")"
        recPerSecOutSource_resp = requests.get(prom_endpoint_recPerSecOutSource_max)
        data = recPerSecOutSource_resp.json()
        try:
            data = data['data']['result'][0]['value'][1]
        except (IndexError, KeyError):
            data = None
        dictReturn["recPerSecOutSource_" + spout + "_max"] = data

        # records out 98th
        prom_endpoint_recPerSecOutSource_98 = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(quantile_over_time(0.98," + eventrate_query + "))"
        recPerSecOutSource_resp = requests.get(prom_endpoint_recPerSecOutSource_98)
        data = recPerSecOutSource_resp.json()
        try:
            data = data['data']['result'][0]['value'][1]
        except (IndexError, KeyError):
            data = None
        dictReturn["recPerSecOutSource_" + spout + "_98"] = data

        # records out 95th
        prom_endpoint_recPerSecOutSource_95 = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(quantile_over_time(0.95," + eventrate_query + "))"
        recPerSecOutSource_resp = requests.get(prom_endpoint_recPerSecOutSource_95)
        data = recPerSecOutSource_resp.json()
        try:
            data = data['data']['result'][0]['value'][1]
        except (IndexError, KeyError):
            data = None
        dictReturn["recPerSecOutSource_" + spout + "_95"] = data

        # records out 50th
        prom_endpoint_recPerSecOutSource_50 = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(quantile_over_time(0.50," + eventrate_query + "))"
        recPerSecOutSource_resp = requests.get(prom_endpoint_recPerSecOutSource_50)
        data = recPerSecOutSource_resp.json()
        try:
            data = data['data']['result'][0]['value'][1]
        except (IndexError, KeyError):
            data = None
        dictReturn["recPerSecOutSource_" + spout + "_50"] = data

        # records out 50th
        prom_endpoint_recPerSecOutSource_avg = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(avg_over_time(" + eventrate_query + "))"
        recPerSecOutSource_resp = requests.get(prom_endpoint_recPerSecOutSource_avg)
        data = recPerSecOutSource_resp.json()
        try:
            data = data['data']['result'][0]['value'][1]
        except (IndexError, KeyError):
            data = None
        dictReturn["recPerSecOutSource_" + spout + "_avg"] = data

        # acked/failed tuple counts -- storm_exporter.py has always exported
        # spouts_acked/spouts_failed (see spoutMetric()), but nothing here
        # ever queried them, so every collected run silently had neither.
        # Both only become meaningful once the spout actually emits with a
        # message id (SyntheticSpout.java fix, 2026-10-25) -- before that,
        # Storm never tracks these tuples at all, so both would read 0
        # regardless of this query existing. Cumulative counts (not a rate)
        # since that's what's needed to compute a fail ratio for telling
        # contention apart from timeouts/replays.
        try:
            prom_endpoint_acked = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(
                "spouts_acked{{TopologyId=\"{}\", SpoutId='{}'}} @ {}".format(job_id, spout, timestamp_prom))
            response = requests.get(prom_endpoint_acked)
            data = response.json()
            dictReturn["spout_acked_" + spout] = float(data['data']['result'][0]['value'][1])
        except (IndexError, KeyError):
            dictReturn["spout_acked_" + spout] = None

        try:
            prom_endpoint_failed = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(
                "spouts_failed{{TopologyId=\"{}\", SpoutId='{}'}} @ {}".format(job_id, spout, timestamp_prom))
            response = requests.get(prom_endpoint_failed)
            data = response.json()
            dictReturn["spout_failed_" + spout] = float(data['data']['result'][0]['value'][1])
        except (IndexError, KeyError):
            dictReturn["spout_failed_" + spout] = None

    # throughput
    throughput_query = "rate(bolts_executed{TopologyId=\"" + job_id + "\" , BoltId='sink'}[1m])[5m:1m] @ " + timestamp_prom
    prom_endpoint_Throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=" + throughput_query
    throughput_response = requests.get(prom_endpoint_Throughput)
    data = throughput_response.json()
    data = data['data']['result']
    dictReturn["throughput"] = data

    # throughput 98th
    prom_endpoint_Throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(quantile_over_time(0.98," + throughput_query + "))"
    throughput_response = requests.get(prom_endpoint_Throughput)
    data = throughput_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["throughput_98"] = data

    # throughput 95th
    prom_endpoint_Throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(quantile_over_time(0.95," + throughput_query + "))"
    throughput_response = requests.get(prom_endpoint_Throughput)
    data = throughput_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["throughput_95"] = data

    # throughput 50th
    prom_endpoint_Throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(quantile_over_time(0.50," + throughput_query + "))"
    throughput_response = requests.get(prom_endpoint_Throughput)
    data = throughput_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["throughput_50"] = data

    prom_endpoint_Throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(avg_over_time(" + throughput_query + "))"
    throughput_response = requests.get(prom_endpoint_Throughput)
    data = throughput_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["throughput_avg"] = data

#    # latency
#    latency_query = "e2e_latency_seconds[5m] @ " + timestamp_prom
#    prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=" + latency_query
#    latency_response = requests.get(prom_endpoint_Latency)
#    data = latency_response.json()
#    data = data['data']['result']
#    dictReturn["endtoend_latency"] = data
#
#    # latency 98th
#    prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.98," + latency_query + ")"
#    latency_response = requests.get(prom_endpoint_Latency)
#    data = latency_response.json()
#    data = data['data']['result'][0]['value'][1]
#    dictReturn["endtoend_latency_98"] = data
#
#    # latency 95th
#    prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.95," + latency_query + ")"
#    latency_response = requests.get(prom_endpoint_Latency)
#    data = latency_response.json()
#    data = data['data']['result'][0]['value'][1]
#    dictReturn["endtoend_latency_95"] = data
#
#    # latency 50th
#    prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.50," + latency_query + ")"
#    latency_response = requests.get(prom_endpoint_Latency)
#    data = latency_response.json()
#    data = data['data']['result'][0]['value'][1]
#    dictReturn["endtoend_latency_50"] = data
#
#    # latency avg
#    prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + latency_query + ")"
#    latency_response = requests.get(prom_endpoint_Latency)
#    data = latency_response.json()
#    data = data['data']['result'][0]['value'][1]
#    dictReturn["endtoend_latency_avg"] = data

    # e2e latency
    try:
        prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=topology_stats_complete_latency{TopologyId=\"" + job_id + "\", window=\"600\"} @ " + timestamp_prom
        latency_response = requests.get(prom_endpoint_Latency)
        data = latency_response.json()
        try:
            data = data['data']['result'][0]['value'][1]
        except (IndexError, KeyError):
            data = None
        dictReturn["endtoend_latency_storm"] = data
    except IndexError:
        dictReturn["endtoend_latency_storm"] = 0

    # prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(avg_over_time(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{job_id=\"" + job_id + "\",operator_subtask_index='0',quantile='0.98'}[5m] @ " + timestamp_prom + "))"
    # latency_response = requests.get(prom_endpoint_Latency)
    # data = latency_response.json()
    # data = data['data']['result'][0]['value'][1]
    # dictReturn["endtoend_latency_98"] = data

    # prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(avg_over_time(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{job_id=\"" + job_id + "\",operator_subtask_index='0',quantile='0.95'}[5m] @ " + timestamp_prom + "))"
    # latency_response = requests.get(prom_endpoint_Latency)
    # data = latency_response.json()
    # data = data['data']['result'][0]['value'][1]
    # dictReturn["endtoend_latency_95"]= data

    # prom_endpoint_Latency = "http://" + main_node_ip + ":9090/api/v1/query?query=sum(avg_over_time(flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency{job_id=\"" + job_id + "\",operator_subtask_index='0',quantile='0.5'}[5m] @ " + timestamp_prom + "))"
    # latency_response = requests.get(prom_endpoint_Latency)
    # data = latency_response.json()
    # data = data['data']['result'][0]['value'][1]
    # dictReturn["endtoend_latency_50"] = data

    return dictReturn


def getHardwareMetrics(main_node_ip, job_id, timestamp_prom):
    dictReturn = {}

    # cpu
    cpu_query = "(100 - 100 * (avg by (instance) (irate(node_cpu_seconds_total{job=\"node_exporter\",mode=\"idle\"}[10s]))))[5m:10s] @ " + timestamp_prom
    prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=" + cpu_query
    cpu_response = requests.get(prom_endpoint_cpu)
    data = cpu_response.json()
    data = data['data']['result']
    dictReturn["node_cpu"] = data

    # cpu 98th
    prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.98," + cpu_query + "))"
    cpu_response = requests.get(prom_endpoint_cpu)
    data = cpu_response.json()
    print(data)
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_cpu_98"] = data

    # cpu 95th
    prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.95," + cpu_query + "))"
    cpu_response = requests.get(prom_endpoint_cpu)
    data = cpu_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_cpu_95"] = data

    # cpu 50th
    prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.50," + cpu_query + "))"
    cpu_response = requests.get(prom_endpoint_cpu)
    data = cpu_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_cpu_50"] = data

    # control network out
    network_query = "rate(node_network_transmit_bytes_total{device=~\"eno1|eno33np0\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=" + network_query
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    data = data['data']['result']
    dictReturn["node_network_out"] = data

    # control network out 98th
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.98," + network_query + "))"
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_network_out_98"] = data

    # control network out 95th
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.95," + network_query + "))"
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_network_out_95"] = data

    # control network out 50th
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.50," + network_query + "))"
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_network_out_50"] = data

    # experiment network out
    network_query = "rate(node_network_transmit_bytes_total{device=~\"eno2|eno1d1|enp65s0f0np0\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=" + network_query
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    data = data['data']['result']
    dictReturn["experiment_network_out"] = data

    # experiment network out 98th
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.98," + network_query + "))"
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["experiment_network_out_98"] = data

    # experiment network out 95th
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.95," + network_query + "))"
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["experiment_network_out_95"] = data

    # experiment network out 50th
    prom_endpoint_NW = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.50," + network_query + "))"
    nw_response = requests.get(prom_endpoint_NW)
    data = nw_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["experiment_network_out_50"] = data

    # memory
    memory_query = "(100 - 100 * (sum(node_memory_MemFree_bytes{job=\"node_exporter\"} + node_memory_Cached_bytes{job=\"node_exporter\"}+node_memory_Buffers_bytes{job=\"node_exporter\"}))/sum(node_memory_MemTotal_bytes{job=\"node_exporter\"}))[5m:10s] @ " + timestamp_prom
    prom_endpoint_Memory = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(memory_query)
    mem_response = requests.get(prom_endpoint_Memory)
    data = mem_response.json()
    data = data['data']['result']
    dictReturn["node_memory"] = data

    # memory 98th
    prom_endpoint_Memory = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.98," + urllib.parse.quote(memory_query) + "))"
    mem_response = requests.get(prom_endpoint_Memory)
    data = mem_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_memory_98"] = data

    # memory 95th
    prom_endpoint_Memory = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.95," + urllib.parse.quote(memory_query) + "))"
    mem_response = requests.get(prom_endpoint_Memory)
    data = mem_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_memory_95"] = data

    # memory 50th
    prom_endpoint_Memory = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(0.50," + urllib.parse.quote(memory_query) + "))"
    mem_response = requests.get(prom_endpoint_Memory)
    data = mem_response.json()
    try:
        data = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        data = None
    dictReturn["node_memory_50"] = data

    return dictReturn


def getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom):
    dict_response = {}

    # event rate
    messages_query = "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{topic='{}'}}[30s]))[5m:30s] @ {}"
    prom_event_rate = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + messages_query.format(input_topic, timestamp_prom) + ")"
    response = requests.get(prom_event_rate)
    data = response.json()
    try:
        event_rate = data["data"]["result"][0]["value"][1]
    except (IndexError, KeyError):
        event_rate = None
    response_data = {"event rate": event_rate}
    dict_response["kafka_event_rate"] = response_data

    # event rate 98th
    prom_event_rate_98th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.98, " + messages_query.format(input_topic, timestamp_prom) + ")"
    response = requests.get(prom_event_rate_98th)
    data = response.json()
    try:
        event_rate_98th = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        event_rate_98th = None
    response_data = {"event rate 98th": event_rate_98th}
    dict_response["kafka_event_rate_98th"] = response_data

    # event rate 95th
    prom_event_rate_95th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.95, " + messages_query.format(input_topic, timestamp_prom) + ")"
    response = requests.get(prom_event_rate_95th)
    data = response.json()
    try:
        event_rate_95th = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        event_rate_95th = None
    response_data = {"event rate 95th": event_rate_95th}
    dict_response["kafka_event_rate_95th"] = response_data

    # event rate 50th
    prom_event_rate_50th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.50, " + messages_query.format(input_topic, timestamp_prom) + ")"
    response = requests.get(prom_event_rate_50th)
    data = response.json()
    try:
        event_rate_50th = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        event_rate_50th = None
    response_data = {"event rate 50th": event_rate_50th}
    dict_response["kafka_event_rate_50th"] = response_data

    # throughput
    prom_throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + messages_query.format(output_topic, timestamp_prom) + ")"
    response = requests.get(prom_throughput)
    data = response.json()
    try:
        throughput = data["data"]["result"][0]["value"][1]
    except (IndexError, KeyError):
        throughput = None
    response_data = {"throughput": throughput}
    dict_response["kafka_throughput"] = response_data

    # throughput 98th
    prom_throughput_98th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.98, " + messages_query.format(output_topic, timestamp_prom) + ")"
    response = requests.get(prom_throughput_98th)
    data = response.json()
    try:
        throughput_98th = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        throughput_98th = None
    response_data = {"throughput 98th": throughput_98th}
    dict_response["kafka_throughput_98th"] = response_data

    # throughput 95th
    prom_throughput_95th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.95, " + messages_query.format(output_topic, timestamp_prom) + ")"
    response = requests.get(prom_throughput_95th)
    data = response.json()
    try:
        throughput_95th = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        throughput_95th = None
    response_data = {"throughput 95th": throughput_95th}
    dict_response["kafka_throughput_95th"] = response_data

    # throughput 50th
    prom_throuthput_50th = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(0.50, " + messages_query.format(output_topic, timestamp_prom) + ")"
    response = requests.get(prom_throuthput_50th)
    data = response.json()
    try:
        throughput_50th = data['data']['result'][0]['value'][1]
    except (IndexError, KeyError):
        throughput_50th = None
    response_data = {"throughput 50th": throughput_50th}
    dict_response["kafka_throughput_50th"] = response_data

    return dict_response


def getTopologyMetrics(job_id, spouts: List[str], bolts: List[str],
                       main_node_ip, timestamp_prom):
    response = requests.get("http://" + main_node_ip +
                            ":8080/api/v1/topology/" + job_id)
    jsonResponse = response.json()
    result = {}

    topologyName = jsonResponse["name"]
    bolt_props = {}

    if "bolts" in jsonResponse:
        for bolt in bolts:
            for bolt_json in jsonResponse["bolts"]:
                if "boltId" in bolt_json and bolt in bolt_json["boltId"]:
                    bolt_props[bolt] = {
                            "name": bolt,
                            "id": bolt_json["encodedBoltId"],
                            "num": int(bolt_json["tasks"])
                            }

    result["job_name"] = topologyName
    result["operatorMetrics"] = {}
    for name, props in bolt_props.items():
        result["operatorMetrics"][name] = getBoltMetrics(
                props["num"], main_node_ip, job_id, props["id"], name,
                timestamp_prom)

    # try:
    #     kafkametrics = getKafkaMetrics(main_node_ip, "WordCountIn",
    #                                    "WordCountOut", timestamp_prom)
    #     dictCombo["kafkaMetrics"] = kafkametrics
    # except Exception:
    #     pass

    querymetric = getQueryMetrics(main_node_ip, job_id, timestamp_prom, spouts)
    result["queryMetrics"] = querymetric

    hardwaremetrics = getHardwareMetrics(main_node_ip, job_id, timestamp_prom)
    result["hardwareMetrics"] = hardwaremetrics

    return result


def getWordCount(job_id, main_node_ip, timestamp_prom):
    response = requests.get("http://" + main_node_ip +
                            ":8080/api/v1/topology/" + job_id)
    jsonResponse = response.json()

    tokenizerId = None
    counterId = None
    dictTokenizer = {}
    dictCounter = {}
    dictCombo = {}
    topologyName = jsonResponse["name"]
    if "bolts" in jsonResponse:
        for bolt in jsonResponse["bolts"]:
            if "boltId" in bolt and "tokenizer" in bolt["boltId"]:
                tokenizer_name = "tokenizer"
                tokenizerId = bolt["encodedBoltId"]
                numOfTokenizer = int(bolt["tasks"])
            elif "boltId" in bolt and "counter" in bolt["boltId"]:
                counter_name = "counter"
                counterId = bolt["encodedBoltId"]
                numOfCounter = int(bolt["tasks"])
    dictTokenizer = getBoltMetrics(numOfTokenizer, main_node_ip, job_id,
                                   tokenizerId, tokenizer_name,
                                   timestamp_prom)

    dictCounter = getBoltMetrics(numOfCounter, main_node_ip, job_id,
                                 counterId, counter_name, timestamp_prom)

    # Create a new dictionary with "job_name": job_name
    job_dict = {"job_name": topologyName}

    # Merge the new dictionary with dictTokenizer and dictCounter using update
    dictCombo = job_dict.copy()

    dictCombo["operatorMetrics"] = {}
    dictCombo["operatorMetrics"]["Tokenizer"] = dictTokenizer
    dictCombo["operatorMetrics"]["Counter"] = dictCounter

    #try:
    #    kafkametrics = getKafkaMetrics(main_node_ip, "WordCountIn",
    #                                   "WordCountOut", timestamp_prom)
    #    dictCombo["kafkaMetrics"] = kafkametrics
    #except Exception:
    #    pass

    querymetric = getQueryMetrics(main_node_ip, job_id, timestamp_prom, ["spout"])
    dictCombo["queryMetrics"] = querymetric

    hardwaremetrics = getHardwareMetrics(main_node_ip, job_id, timestamp_prom)
    dictCombo["hardwareMetrics"] = hardwaremetrics

    return dictCombo


@click.command()
@click.option('--main_node_ip', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_id', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_run_time', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_parallelization', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_query_number', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_window_size', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_window_slide_size', type=str,
              help='node address where prometheus and flink resides')
@click.option('--job_sample', type=int, help='number of samples')
@click.option('--job_iteration', type=int, help='number of iterations')
@click.option('--producer_event_per_second', type=str,
              help='node address where prometheus and flink resides')
@click.option('--hardware_type', type=str, help='type of hardware')
@click.option('--num_nodes', type=str, help='number of nodes')
@click.option('--num_slots', type=str, help='number of slots')
@click.option('--enumeration_strategy', type=str, help='enumeration strategy')
def main(main_node_ip, job_id, job_run_time, job_parallelization,
         job_query_number, job_window_size, job_window_slide_size,
         job_iteration, job_sample, producer_event_per_second, hardware_type,
         num_nodes, num_slots, enumeration_strategy):
    response = requests.get("http://" + main_node_ip + ":8080" +
                            "/api/v1/topology/" + job_id)
    response = response.json()
    jobName = response["name"]
    # start_time = time.time() - response["uptimeSeconds"]
    # timestamp_prom = start_time + 300
    timestamp_prom = time.time() - 30
    timestamp_prom = str(timestamp_prom)

    current_directory = os.getcwd()
    folder_name = jobName
    full_folder_path = os.path.join(current_directory, folder_name)
    if not os.path.exists(full_folder_path):
        os.makedirs(full_folder_path)
        print(f"Folder '{folder_name}' created successfully at '{full_folder_path}'")
    else:
        print(f"Folder '{folder_name}' already exists at '{full_folder_path}'")

    spouts = []
    bolts = []
    print("jobName:", jobName)
    if jobName == 'GooglecloudMonitoring':
        spouts = ["spout"]
        bolts = ["task-event-parser-bolt", "cpu-per-job-bolt",
                 "cpu-per-category-bolt"]
    elif jobName == 'AdAnalytics':
        if job_query_number == "1":
            spouts = ["clicks-spout", "impressions-spout"]
            bolts = ["clicks-parser", "impressions-parser",
                     "clicks-counter", "impressions-counter", "join",
                     "rolling-ctr"]
        elif job_query_number == "2":
            spouts = ["spout"]
            bolts = ["parser", "counter", "rolling-ctr"]
    elif jobName == 'SmartGrid':
        spouts = ["spout"]
        bolts = ["house-event-parser-bolt", "global-median-bolt",
                 "plug-median-bolt"]
    elif jobName == 'WordCount':
        spouts = ["spout"]
        bolts = ["tokenizer", "counter"]
    elif jobName == "SentimentAnalysis":
        spouts = ["spout"]
        bolts = ["parse-tweet-bolt", "analyze-sentiment-bolt"]
    elif jobName == "SpikeDetection":
        spouts = ["spout"]
        bolts = ["parser-bolt", "average-calculator-bolt",
                 "spike-detector-bolt"]
    elif jobName == "TrendingTopics":
        spouts = ["spout"]
        bolts = ["twitter-parser-bolt", "topic-extractor-bolt",
                 "rolling-counter-sliding-window-bolt",
                 "intermediate-ranking-bolt", "final-ranking-bolt",
                 "popularity-detector-bolt"]
    elif jobName == "LogProcessing":
        spouts = ["spout"]
        bolts = ["logParserBolt", "statusCounterBolt", "volumeCounterBolt"]
    elif jobName == "BargainIndex":
        spouts = ["spout"]
        bolts = ["parser-bolt", "vwap-calculator-bolt", "bargain-index-bolt"]
    elif jobName == "ClickAnalytics":
        spouts = ["spout"]
        bolts = ["parse-click-log-bolt", "repeat-visit-operator-bolt",
                 "sum-reducer-bolt", "geography-operator-bolt"]
    elif jobName == "LinearQuery":
        # Synthetic Linear query -- see LinearQueryTopology.java (parallelism
        # indices: 0 spout, 1 filter1, 2 window-agg, 3 filter2, 4 sink).
        spouts = ["spout"]
        bolts = ["filter1", "window-agg", "filter2"]
    elif jobName in ("TwoFilters", "ThreeFilters", "FourFilters"):
        # Synthetic chained-filters query -- see ChainedFiltersTopology.java
        # (spout -> window-agg -> filter1..N -> sink; N = 2/3/4 per app name).
        spouts = ["spout"]
        n = {"TwoFilters": 2, "ThreeFilters": 3, "FourFilters": 4}[jobName]
        bolts = ["window-agg"] + [f"filter{i}" for i in range(1, n + 1)]
    elif jobName in ("TwoWayJoin", "ThreeWayJoin", "FourWayJoin", "FiveWayJoin", "SixWayJoin"):
        # Synthetic N-way join -- see JoinTopology.java (N sources -> [filter-i
        # per source, 2/3-way only] -> join -> window-agg -> sink).
        n = {"TwoWayJoin": 2, "ThreeWayJoin": 3, "FourWayJoin": 4,
             "FiveWayJoin": 5, "SixWayJoin": 6}[jobName]
        spouts = [f"spout-{i}" for i in range(n)]
        bolts = ([f"filter-{i}" for i in range(n)] if n <= 3 else []) + ["join", "window-agg"]
    elif jobName == "LinearRoad":
        spouts = ["spout"]
        # Union of all 4 query variants' real bolt names (verified against
        # LinearRoadTopology.java directly, 2026-09-02) -- Q2's
        # "accident-notification"/"accident-notification-formatter" were
        # missing here entirely before this fix, meaning a Q2 (accident-
        # detection) run would have silently collected metrics for only
        # "vehicleEvent-parser-bolt" (the one bolt common to every query)
        # and nothing else, since getTopologyMetrics only creates an
        # operatorMetrics entry for bolt names that are actually found in
        # the live topology (substring match against the real registered
        # boltIds) -- harmless to list every variant's names together like
        # this, same reasoning as the existing ClickAnalytics/SmartGrid/
        # LogProcessing entries below.
        bolts = ["vehicleEvent-parser-bolt", "toll-notification-mapper",
                 "toll-notification-formatter", "accident-notification",
                 "accident-notification-formatter", "daily-expenditure-bolt",
                 "vehicle-report-mapper", "logger-bolt"]
    elif jobName == "MachineOutlier":
        spouts = ["spout"]
        bolts = ["machine-usage-parser-bolt", "machine-outlier-bolt"]
    elif jobName == "TrafficMonitoring":
        spouts = ["spout"]
        bolts = ["ParserBolt", "MapMatcherBolt", "AvgSpeedCalculatorBolt"]
    elif jobName == "TPCH":
        spouts = ["spout"]
        bolts = ["tpchEventParserBolt", "filterCalculatorBolt",
                 "priorityMapperBolt", "sumBolt"]

    processedMetrics = getTopologyMetrics(
            job_id,
            spouts,
            bolts,
            main_node_ip,
            timestamp_prom)

    metaInfo = {
        'job_framework': 'Apache Storm',
        'job_iteration': job_iteration,
        'job_sample': job_sample,
        'job_run_time': job_run_time,
        'job_parallelization': job_parallelization,
        'job_query_number': job_query_number,
        'job_window_size': job_window_size,
        'job_window_slide_size': job_window_slide_size,
        'producer_event_per_second': producer_event_per_second,
        'cluster_hardware_type': hardware_type,
        'cluster_nodes': num_nodes,
        'cluster_slots_per_node': num_slots,
        'enumeration_strategy': enumeration_strategy,
    }
    meta_file_name = "metaInfo-i{}s{}-{}.json" \
        .format(job_iteration, job_sample, job_id)
    evaluation_file_name = "evaluationMetrics-i{}s{}-{}.json" \
        .format(job_iteration, job_sample, job_id)
    metaFile = os.path.join(full_folder_path, meta_file_name)
    evaluationFile = os.path.join(full_folder_path, evaluation_file_name)

    try:
        # Write the dictionary to a JSON file
        with open(metaFile, 'w') as json_file:
            json.dump(metaInfo, json_file, indent=4)
        # Write the dictionary to a JSON file
        with open(evaluationFile, 'w') as json_file:
            json.dump(processedMetrics, json_file, indent=4)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    """ Main function
    """
    main()
