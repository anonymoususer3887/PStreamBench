import json
import os
import time
import urllib.parse

import click
import requests

"""
Kafka Streams counterpart of getprom_spark.py / getprom.py / getprom_storm.py,
invoked the same way by JobRunner.get_logs() with the exact same CLI
arguments (see runner.py) -- only the metric-collection internals differ per
SUT. See KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.4 for the full design.

Scope: Word Count only, matching what's currently implemented in
PStreamBench_workload_generator/kafkastreams/dsp_kafkastreams_jobs. Extend
JOB_CLASS_DISPATCH the same way getprom_spark.py does as more workloads land.

Unlike every other engine here, Kafka Streams has no single "the instance"
to point Prometheus queries at (no driver/JobManager/nimbus) -- one
independent JVM instance runs per node (see kafkastreams-provide-job.yaml
and Part A.1), each with its own JMX-exporter target
("kafkastreams_app" in prometheus.yml). Every query below aggregates
(sum/avg) across all of them rather than assuming one target.
"""


def _first_value(result):
    """Pull {"value": [ts, v]}'s v out of a Prometheus vector result, or None
    if empty. Kafka/node_exporter/JMX-exporter targets only exist while the
    thing they scrape is alive, and a fresh topic's per-topic broker metric
    may not have a sample yet at the exact queried instant -- an empty
    result here is expected, not exceptional, so this must not raise (same
    reasoning as getprom_spark.py's version of this helper, which the
    functions below did not originally share -- indexing result[0] directly
    crashed the whole collection run on a single missing metric).

    Also normalizes literal "NaN" to None: Prometheus renders float NaN as
    the JSON string "NaN", which is valid to receive but not something
    callers should have to special-case downstream -- belt-and-suspenders
    alongside the "> 0" NaN-filtering already done at the query level for
    kafka_streams_processor_node_record_e2e_latency_*, see getStreamsMetrics.
    """
    if not result:
        return None
    value = result[0]["value"][1]
    return None if value == "NaN" else value


def getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom):
    """Identical to getprom_spark.py's getKafkaMetrics() in spirit: Kafka's
    own JMX metrics are scraped regardless of which SUT is producing/
    consuming. Unlike that version, uses _first_value() throughout rather
    than indexing result[0] directly -- see _first_value()'s docstring.
    """
    dict_response = {}

    messages_query = "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{topic='{}'}}[1m]))[5m:1s] @ {}"

    prom_event_rate = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + messages_query.format(input_topic, timestamp_prom) + ")"
    response = requests.get(prom_event_rate)
    data = response.json()
    dict_response["kafka_event_rate"] = {"event rate": _first_value(data["data"]["result"])}

    for quantile, label in [(0.98, "98th"), (0.95, "95th"), (0.50, "50th")]:
        prom_event_rate_q = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(" + str(quantile) + ", " + messages_query.format(input_topic, timestamp_prom) + ")"
        response = requests.get(prom_event_rate_q)
        data = response.json()
        dict_response["kafka_event_rate_" + label] = {"event rate " + label: _first_value(data['data']['result'])}

    prom_throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + messages_query.format(output_topic, timestamp_prom) + ")"
    response = requests.get(prom_throughput)
    data = response.json()
    dict_response["kafka_throughput"] = {"throughput": _first_value(data["data"]["result"])}

    for quantile, label in [(0.98, "98th"), (0.95, "95th"), (0.50, "50th")]:
        prom_throughput_q = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(" + str(quantile) + ", " + messages_query.format(output_topic, timestamp_prom) + ")"
        response = requests.get(prom_throughput_q)
        data = response.json()
        dict_response["kafka_throughput_" + label] = {"throughput " + label: _first_value(data['data']['result'])}

    return dict_response


def getHardwareMetrics(main_node_ip, timestamp_prom):
    """Identical to getprom_spark.py's getHardwareMetrics() in spirit
    (node_exporter metrics are SUT-agnostic), but uses _first_value()
    throughout rather than indexing result[0] directly.
    """
    dict_return = {}

    cpu_query = "(100 - 100 * (avg by (instance) (irate(node_cpu_seconds_total{job=\"node_exporter\",mode=\"idle\"}[10s]))))[5m:10s] @ " + timestamp_prom
    prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=" + cpu_query
    data = requests.get(prom_endpoint_cpu).json()
    dict_return["node_cpu"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + cpu_query + "))"
        data = requests.get(prom_endpoint_cpu).json()
        dict_return["node_cpu_" + label] = _first_value(data['data']['result'])

    network_query = "rate(node_network_transmit_bytes_total{device=~\"eno1|eno33np0\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=" + network_query
    data = requests.get(prom_endpoint_nw).json()
    dict_return["node_network_out"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + network_query + "))"
        data = requests.get(prom_endpoint_nw).json()
        dict_return["node_network_out_" + label] = _first_value(data['data']['result'])

    experiment_network_query = "rate(node_network_transmit_bytes_total{device=~\"eno2|eno1d1|enp65s0f0np0\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_exp_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=" + experiment_network_query
    data = requests.get(prom_endpoint_exp_nw).json()
    dict_return["experiment_network_out"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_exp_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + experiment_network_query + "))"
        data = requests.get(prom_endpoint_exp_nw).json()
        dict_return["experiment_network_out_" + label] = _first_value(data['data']['result'])

    memory_query = "(100 - 100 * (sum(node_memory_MemFree_bytes{job=\"node_exporter\"} + node_memory_Cached_bytes{job=\"node_exporter\"}+node_memory_Buffers_bytes{job=\"node_exporter\"}))/sum(node_memory_MemTotal_bytes{job=\"node_exporter\"}))[5m:10s] @ " + timestamp_prom
    prom_endpoint_mem = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(memory_query)
    data = requests.get(prom_endpoint_mem).json()
    dict_return["node_memory"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_mem = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + urllib.parse.quote(memory_query) + "))"
        data = requests.get(prom_endpoint_mem).json()
        dict_return["node_memory_" + label] = _first_value(data['data']['result'])

    return dict_return


def getStreamsMetrics(main_node_ip, timestamp_prom):
    """Kafka Streams' own native metrics (Tier 2), scraped via the
    jmx_prometheus_javaagent attached to every instance's `java` command
    line on port 7076 (see kafkastreams-provide-job.yaml / jmx_exporter.yml),
    averaged across every node+thread since one independent JVM instance
    runs per worker node (Part A.1) -- there is no single driver/JobManager
    target to point at like Spark/Flink.

    record-e2e-latency-avg/max is Kafka Streams' *native* end-to-end latency
    metric (KIP-613, source+terminal processor nodes) -- unlike Spark, no
    custom Dataset.observe()-style plumbing was needed to get this number.
    Caveat carried over from KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.4/A.5:
    this is wall-clock time, not stream time, so it spikes during
    consumer-group rebalances even with no real processing regression.
    """
    dict_return = {}

    # cross-node aggregation function paired with each metric: averaging
    # per-node *max* values (as the code previously did, hardcoding avg()
    # for both) produces an "end_to_end_latency_max" that is itself just
    # another average -- routinely smaller than "end_to_end_latency_avg"
    # whenever one node's sustained average latency exceeds the cross-node
    # mean of everyone's peak latency (confirmed live: avg=56454ms,
    # max=624ms on a windowed-aggregation-heavy workload). avg_i <= max_i
    # holds per node/thread/task by definition (both stats come from the
    # same underlying Sensor over the same sample window), so avg(avg_i) <=
    # avg(max_i) <= max(max_i) always -- using max() for the max metric
    # restores that guarantee. max_over_time (vs avg_over_time) for the max
    # metric is likewise the correct time-window reduction: the single
    # worst instantaneous cross-node max observed during the run, not an
    # average of it.
    metric_queries = {
        "end_to_end_latency_avg": ("kafka_streams_processor_node_record_e2e_latency_avg", "avg", "avg_over_time"),
        "end_to_end_latency_max": ("kafka_streams_processor_node_record_e2e_latency_max", "max", "max_over_time"),
    }

    for label, (metric_name, agg_fn, time_fn) in metric_queries.items():
        # "> 0" filters out tasks/threads that haven't processed a record
        # yet in this recording window -- Kafka Streams' own Avg stat
        # reports NaN (not absent) for zero samples, and unlike a missing
        # series, Prometheus's avg()/max() does NOT skip NaN inputs: one
        # idle task's NaN poisons the whole aggregate to NaN. A "> 0"
        # comparison naturally excludes NaN (any comparison against NaN is
        # false per IEEE754), which is safe here since real latencies are
        # always > 0.
        base_query = "{0}({1} > 0)[5m:1s] @ {2}".format(agg_fn, metric_name, timestamp_prom)
        prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(time_fn + "(" + base_query + ")")
        data = requests.get(prom_endpoint).json()
        dict_return[label] = _first_value(data['data']['result'])

    return dict_return


def _get_operator_rate(name, main_node_ip, timestamp_prom):
    """Fetch one named OperatorMetricsProcessor stage's total processed-record
    rate (records/sec), summed across every node+thread (see
    OperatorMetricsProcessor.java) -- the Kafka Streams counterpart of
    getprom_spark.py's _get_operator_rows(). Unlike Spark's custom gauges
    (row *counts* per micro-batch, needing a separate division by batch
    duration), Kafka Streams' addRateTotalSensor() already reports a rate
    directly, so this is "rows" for the selectivity math below in the same
    sense a rate ratio is comparable to a row-count ratio -- both are
    per-stage output/input, just one is instantaneous and one is
    per-micro-batch.
    """
    query = 'sum(kafka_streams_operator_records_rate{{operator_id="{0}"}})[5m:1s] @ {1}'.format(name, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + query + ")")
    data = requests.get(prom_endpoint).json()
    return _first_value(data['data']['result'])


def _get_thread_ratio(metric, main_node_ip, timestamp_prom):
    """One of Kafka Streams' own KIP-444 thread-state-fraction metrics
    (process/poll/punctuate/commit-ratio, see jmx_exporter_kafkastreams_app.yml),
    averaged across every node+thread -- added 2026-09-19 for
    ds2_strategy/dhalion_strategy's busy_ratio (parallelism_strategies.py).
    These are THREAD-level, not per-operator (unlike Flink/Storm's native
    busy/backpressure signals) -- Kafka Streams has no per-stage parallelism
    control at all (see WordCountTopology.java's javadoc), so there is no
    finer-grained signal to query even in principle. This single job-wide
    value gets applied uniformly across every "stage" the same way Spark's
    adapter already does for its own job-wide busy_ratio proxy.

    "> 0" filters out threads that haven't processed anything yet in this
    recording window (same reasoning, and same pattern, as
    getStreamsMetrics()'s end-to-end-latency queries above) -- Kafka
    Streams' Rate-based metrics report NaN (not 0 or absent) before a
    thread's first sample, and confirmed live (2026-09-19) a single
    NaN-reporting thread (out of several real ones with genuine 0..1
    values) poisons avg_over_time()/avg() for the *entire* aggregate to
    NaN, since Prometheus does not skip NaN inputs in aggregation by
    default -- without this filter, busy_ratio came back NaN (silently
    coerced to the 0.0 fallback below) on every single real test despite
    the underlying per-thread data being genuinely non-zero.
    """
    # Subquery syntax ([5m:1s], not a plain range selector), same as
    # getStreamsMetrics() above -- a plain range selector can only apply
    # directly to a metric name, not to a filtered expression like
    # "metric > 0"; the subquery form evaluates avg(metric > 0) at each
    # resolution step first, THEN range-aggregates those instant results.
    base_query = "avg(kafka_streams_thread_{0}_ratio > 0)[5m:1s] @ {1}".format(metric, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + base_query + ")")
    data = requests.get(prom_endpoint).json()
    return _first_value(data['data']['result'])


def getOperatorMetrics(operator_names, main_node_ip, timestamp_prom):
    """Per-stage throughput (records/sec, summed across every node+thread) +
    selectivity for the OperatorMetricsProcessor sensors (Tier 3) -- the
    Kafka Streams counterpart of getprom_spark.py's getOperatorMetrics().
    operator_names must be given in pipeline order; selectivity for each
    stage is this stage's throughput / the previous stage's, with the first
    stage fixed at 1.0 since it has no upstream to compare against -- same
    convention as every other engine's collector in this project.

    No batch_duration_ms parameter here, unlike Spark: Kafka Streams has no
    micro-batch concept, so there is no separate duration to divide by --
    addRateTotalSensor() already reports a real rate.

    busy_ratio is Kafka Streams' own native "process-ratio" thread metric
    (genuinely real, not fabricated -- see _get_thread_ratio()), replicated
    across every stage the same way Spark's job-wide proxy is, since Kafka
    Streams has no per-operator equivalent to query in the first place.
    backpressure_ratio has no honest Kafka-Streams-native analog available
    without extra consumer-lag plumbing this project doesn't have yet --
    left at a neutral 0.0 (documented, not guessed) rather than reusing
    poll-ratio or another metric that doesn't actually mean the same thing;
    this matches the project-wide "can't compute a real rate, don't guess"
    convention already used for missing data everywhere else.
    """
    rates = {}
    for name in operator_names:
        rates[name] = _get_operator_rate(name, main_node_ip, timestamp_prom)

    busy_ratio_raw = _get_thread_ratio("process", main_node_ip, timestamp_prom)
    busy_ratio = float(busy_ratio_raw) if busy_ratio_raw is not None else 0.0

    dict_return = {}
    previous_rate = None
    for name in operator_names:
        current = rates[name]
        selectivity = None
        if current is not None:
            current = float(current)
            if previous_rate is None:
                selectivity = 1.0
            elif previous_rate != 0:
                selectivity = current / previous_rate
            previous_rate = current
        dict_return[name] = {
            "throughput_per_sec": current,
            "selectivity": selectivity,
            "busy_ratio": busy_ratio,
            "backpressure_ratio": 0.0,
        }

    return dict_return


def getCombo(job_name, main_node_ip, input_topic, output_topic, timestamp_prom, operator_names=None):
    combo = {"job_name": job_name}
    combo["queryMetrics"] = getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom)
    combo["streamsMetrics"] = getStreamsMetrics(main_node_ip, timestamp_prom)
    if operator_names:
        combo["operatorMetrics"] = getOperatorMetrics(operator_names, main_node_ip, timestamp_prom)
    combo["hardwareMetrics"] = getHardwareMetrics(main_node_ip, timestamp_prom)
    return combo


def getWordCount(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Word Count", main_node_ip, "WordCountIn", "WordCountOut", timestamp_prom,
                     operator_names=["source", "tokenizer", "counter", "sink"])


def getSpikeDetection(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Spike Detection", main_node_ip, "SpikeIn", "SpikeOut", timestamp_prom,
                     operator_names=["source", "parser", "average", "spikedetector", "sink"])


def getTrafficMonitoring(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Traffic Monitoring", main_node_ip, "TMIn", "TMOut", timestamp_prom,
                     operator_names=["source", "parser", "roadmatcher", "avgspeed", "sink"])


def getTpch(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("TPCH", main_node_ip, "TPIn", "TPOut", timestamp_prom,
                     operator_names=["source", "parser", "filter", "flatmapper", "counter", "sink"])


def getLogProcessing(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Log Processing", main_node_ip, "LogIn", "LogOut", timestamp_prom,
                     operator_names=["source", "parser", "counter", "sink"])


def getTrendingTopics(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Trending Topics", main_node_ip, "TTIn", "TTOut", timestamp_prom,
                     operator_names=["source", "parser", "topicextractor", "counter", "filter", "sink"])


def getSmartGrid(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Smart Grid", main_node_ip, "SmartGridIn", "SmartGridOut", timestamp_prom,
                     operator_names=["source", "parser", "window", "sink"])


def getAdAnalytics(main_node_ip, timestamp_prom, job_query_number=None):
    # Unlike Spark (two independent Kafka consumer groups, no unified
    # "source" stage), this reads the topic once and fans out to both
    # branches from a single tagged "source" stage -- see
    # AdsAnalyticsTopology.java's class javadoc.
    if job_query_number == "1":
        operator_names = ["source", "clicksparser", "impressionsparser", "window", "sink"]
    else:
        operator_names = ["source", "parser", "counter", "rollingCTR", "sink"]
    return getCombo("Ad Analytics", main_node_ip, "AdAnalyticsIn", "AdAnalyticsOut", timestamp_prom,
                     operator_names=operator_names)


def getSentimentAnalysis(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Sentiment Analysis", main_node_ip, "SentimentIn", "SentimentOut", timestamp_prom,
                     operator_names=["source", "parser", "analyser", "sink"])


def getClickAnalytics(main_node_ip, timestamp_prom, job_query_number=None):
    # Query 2 has a "geo" stage instead of "repeatvisit"/"reduce" (see
    # ClickAnalyticsTopology.java's class javadoc).
    if job_query_number == "2":
        operator_names = ["source", "parser", "geo", "sink"]
    else:
        operator_names = ["source", "parser", "repeatvisit", "reduce", "sink"]
    return getCombo("Click Analytics", main_node_ip, "CAIn", "CAOut", timestamp_prom,
                     operator_names=operator_names)


def getGoogleCloudMonitoring(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Google cloud Monitoring", main_node_ip, "GoogleIn", "GoogleOut", timestamp_prom,
                     operator_names=["source", "parser", "window", "sink"])


def getBargainIndex(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Bargain Index", main_node_ip, "BIIn", "BIOut", timestamp_prom,
                     operator_names=["source", "parser", "vwap", "bargain", "sink"])


def getMachineOutlier(main_node_ip, timestamp_prom, job_query_number=None):
    # "outlier" is registered by MachineOutlierProcessor itself, not by a
    # plain OperatorMetricsProcessor stage -- see that class's javadoc.
    return getCombo("Machine Outlier", main_node_ip, "MOIn", "MOOut", timestamp_prom,
                     operator_names=["source", "parser", "outlier", "sink"])


def getLinearRoad(main_node_ip, timestamp_prom, job_query_number=None):
    # Per-query stage shape actually differs (see LinearRoadTopology.java):
    # query 1 (toll notification) tags only "formatter" before sink, no
    # "mapper" -- TollNotificationProcessor has no windowed-aggregation
    # stage of its own. Query 2 (accident detection) is the only branch
    # with both "mapper" (the windowed collision aggregate) and
    # "formatter". Queries 3/4 (daily expenditure / vehicle report) emit
    # straight from their custom processor with no intermediate
    # OperatorMetricsProcessor stage at all -- just source/sink. The
    # previous version of this function queried "mapper" for every query
    # number; since kafka_streams_operator_records_rate carries no job/run
    # id label and Prometheus scrape targets are static per-node addresses
    # reused across successive job runs, querying a stage a given query
    # never creates could return stale leftover data from a *different*
    # job's run on the same node within the ~5min lookback window, rather
    # than a clean absence (confirmed live on a query-1 result: "mapper"
    # showed real-looking nonzero throughput despite that code path never
    # instantiating that stage). Unlike Spark's LinearRoad.java (which
    # never instruments a "sink" stage at all), this always tags "sink"
    # before the final .to(output, ...) call, matching every other job in
    # this module.
    if job_query_number == "1":
        operator_names = ["source", "formatter", "sink"]
    elif job_query_number == "2":
        operator_names = ["source", "mapper", "formatter", "sink"]
    else:
        operator_names = ["source", "sink"]
    return getCombo("Linear Road", main_node_ip, "LRIn", "LROut", timestamp_prom,
                     operator_names=operator_names)


# job_class (the human-readable name from the controller's job_class field,
# e.g. "Word Count") -> collector function. Extend this alongside
# constants_kafkastreams.py and the Java job port as more workloads land
# (see KAFKA_STREAMS_RISINGWAVE_PLAN.md's suggested order of work).
JOB_CLASS_DISPATCH = {
    "Word Count": getWordCount,
    "Spike Detection": getSpikeDetection,
    "Log Processing": getLogProcessing,
    "Trending Topics": getTrendingTopics,
    "Smart Grid": getSmartGrid,
    "Ad Analytics": getAdAnalytics,
    "Sentiment Analysis": getSentimentAnalysis,
    "Traffic Monitoring": getTrafficMonitoring,
    "TPCH": getTpch,
    "Linear Road": getLinearRoad,
    "Machine Outlier": getMachineOutlier,
    "Bargain Index": getBargainIndex,
    "Google cloud Monitoring": getGoogleCloudMonitoring,
    "Click Analytics": getClickAnalytics,
}


@click.command()
@click.option('--main_node_ip', type=str, help='node address where prometheus resides')
@click.option('--job_id', type=str, help='kafka streams application.id (see runner.py::_run_kafkastreams_job)')
@click.option('--job_class', type=str, help='human-readable job class, e.g. "Word Count"')
@click.option('--job_run_time', type=str)
@click.option('--job_parallelization', type=str)
@click.option('--job_query_number', type=str)
@click.option('--job_window_size', type=str)
@click.option('--job_window_slide_size', type=str)
@click.option('--job_sample', type=int)
@click.option('--job_iteration', type=int)
@click.option('--producer_event_per_second', type=str)
@click.option('--hardware_type', type=str)
@click.option('--num_nodes', type=str)
@click.option('--num_slots', type=str)
@click.option('--enumeration_strategy', type=str)
def main(main_node_ip, job_id, job_class, job_run_time, job_parallelization,
         job_query_number, job_window_size, job_window_slide_size,
         job_iteration, job_sample, producer_event_per_second, hardware_type,
         num_nodes, num_slots, enumeration_strategy):
    timestamp_prom = str(time.time() - 30)

    collector = JOB_CLASS_DISPATCH.get(job_class, getWordCount)
    processed_metrics = collector(main_node_ip, timestamp_prom, job_query_number)

    job_name = job_class if job_class in JOB_CLASS_DISPATCH else "Word Count"
    current_directory = os.getcwd()
    full_folder_path = os.path.join(current_directory, job_name)
    os.makedirs(full_folder_path, exist_ok=True)

    meta_info = {
        'job_framework': 'Apache Kafka Streams',
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

    # Timestamped so repeated runs of the same job/iteration/sample don't
    # overwrite each other's results -- same fix as getprom_spark.py.
    run_timestamp = time.strftime("%Y%m%dT%H%M%S")
    meta_file_name = "metaInfo-i{}s{}-{}-{}.json".format(job_iteration, job_sample, job_id, run_timestamp)
    evaluation_file_name = "evaluationMetrics-i{}s{}-{}-{}.json".format(job_iteration, job_sample, job_id, run_timestamp)

    try:
        with open(os.path.join(full_folder_path, meta_file_name), 'w') as f:
            json.dump(meta_info, f, indent=4)
        with open(os.path.join(full_folder_path, evaluation_file_name), 'w') as f:
            json.dump(processed_metrics, f, indent=4)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
