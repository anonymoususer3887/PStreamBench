import json
import os
import re
import time
import urllib.parse

import click
import requests


def _job_id_pattern_fragment(job_id):
    """Scopes the __name__=~ regex used throughout this file to ONE
    specific job run instead of matching every Spark app that has ever
    reported metrics to this Prometheus instance.

    Root-caused live, 2026-10-17 (Spark synthetic strategy sweep): the
    unscoped "metrics_app_.+_driver_..." pattern matches ANY app's metric
    series still within Prometheus's lookback window, not just the
    current job's. Back-to-back submissions (a short cooldown between
    combos, or this script's own baseline-retry loop resubmitting almost
    immediately after a failed attempt) land multiple distinct app-id
    series inside the same 5-minute avg_over_time/quantile_over_time
    window, and Prometheus then returns a hard query ERROR
    ("vector cannot contain metrics with the same labelset") instead of a
    result -- silently swallowed by this file's existing
    except (KeyError, IndexError) guards, surfacing downstream as every
    field coming back None (and, for ds2_strategy, an all-zero-throughput
    nudge computed from nothing). Confirmed directly against the live
    Prometheus instance: the exact subquery this file issues fails with
    that error whenever 2+ app-id series are in range, and succeeds
    (single series, real value) once only one is.

    Spark's Dropwizard metrics bridge sanitizes the app id into the
    metric name by replacing every character that isn't alphanumeric
    with "_" -- confirmed live: app id "app-20261017110620-0178" shows up
    in Prometheus as "metrics_app_20261017110620_0178_driver_...". This
    reproduces that exact substitution so the regex matches only that one
    job's series. job_id=None preserves the old, unscoped ".+" behavior
    (used for every call site that doesn't have a job_id in scope yet --
    see call sites' own comments for why only the synthetic jobs' path
    is threaded through today)."""
    if not job_id:
        return ".+"
    # every call site already hardcodes the literal "metrics_app_" prefix,
    # so strip job_id's own leading "app-"/"app_" before sanitizing the
    # rest, or the fragment would duplicate it ("metrics_app_app_...").
    rest = re.sub(r"^app[-_]", "", job_id)
    return re.escape(re.sub(r"[^A-Za-z0-9]", "_", rest))

"""
Spark counterpart of getprom.py / getprom_storm.py, invoked the same way by
JobRunner.get_logs() with the exact same CLI arguments (see runner.py) --
only the metric-collection internals differ per SUT.

Scope: Word Count only, matching what's currently implemented in
PStreamBench_workload_generator/spark/dsp_spark_jobs. Unlike Flink, Spark
Structured Streaming has no per-operator REST API to enumerate vertices/
operator ids, so there is no getOperatorMetrics()-style breakdown here yet --
only query-level (Kafka topic throughput) and hardware-level metrics, which
are what the DoP-vs-throughput/latency scaling plots actually consume.
Per-workload operator metrics can be added the same way getprom.py has one
get<Workload>() function per workload, once more Spark jobs are ported.
"""


def getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom):
    """Identical to getprom.py's getKafkaMetrics(): Kafka's own JMX metrics
    are scraped regardless of which SUT is producing/consuming, so this is
    reused verbatim rather than reimplemented per SUT.
    """
    dict_response = {}

    messages_query = "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{topic='{}'}}[1m]))[5m:1s] @ {}"

    prom_event_rate = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + messages_query.format(input_topic, timestamp_prom) + ")"
    response = requests.get(prom_event_rate)
    data = response.json()
    dict_response["kafka_event_rate"] = {"event rate": data["data"]["result"][0]["value"][1]}

    for quantile, label in [(0.98, "98th"), (0.95, "95th"), (0.50, "50th")]:
        prom_event_rate_q = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(" + str(quantile) + ", " + messages_query.format(input_topic, timestamp_prom) + ")"
        response = requests.get(prom_event_rate_q)
        data = response.json()
        dict_response["kafka_event_rate_" + label] = {"event rate " + label: data['data']['result'][0]['value'][1]}

    prom_throughput = "http://" + main_node_ip + ":9090/api/v1/query?query=avg_over_time(" + messages_query.format(output_topic, timestamp_prom) + ")"
    response = requests.get(prom_throughput)
    data = response.json()
    dict_response["kafka_throughput"] = {"throughput": data["data"]["result"][0]["value"][1]}

    for quantile, label in [(0.98, "98th"), (0.95, "95th"), (0.50, "50th")]:
        prom_throughput_q = "http://" + main_node_ip + ":9090/api/v1/query?query=quantile_over_time(" + str(quantile) + ", " + messages_query.format(output_topic, timestamp_prom) + ")"
        response = requests.get(prom_throughput_q)
        data = response.json()
        dict_response["kafka_throughput_" + label] = {"throughput " + label: data['data']['result'][0]['value'][1]}

    return dict_response


def _safe_result(prom_endpoint):
    """Guards against the same crash class fixed in getSparkStreamingMetrics
    (2026-10-16): a Prometheus query can come back as an ERROR response (no
    "data" key) rather than an empty "result" list -- confirmed live, first
    distributed synthetic sweep run, taking down the whole evaluationMetrics
    collection for every combo past the first. Returns [] on any failure
    (same "don't guess, return nothing" fallback used throughout this file),
    not just on the one query shape that was observed to crash."""
    try:
        return requests.get(prom_endpoint).json()['data']['result']
    except (KeyError, IndexError, ValueError):
        return []


def _safe_scalar(prom_endpoint):
    result = _safe_result(prom_endpoint)
    try:
        return result[0]['value'][1]
    except (KeyError, IndexError):
        return None


def getHardwareMetrics(main_node_ip, timestamp_prom):
    """Identical in spirit to getprom.py's getHardwareMetrics(): node_exporter
    metrics are SUT-agnostic. See that file if the CPU/network device names
    need adjusting for a given CloudLab hardware type.
    """
    dict_return = {}

    cpu_query = "(100 - 100 * (avg by (instance) (irate(node_cpu_seconds_total{job=\"node_exporter\",mode=\"idle\"}[10s]))))[5m:10s] @ " + timestamp_prom
    prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=" + cpu_query
    dict_return["node_cpu"] = _safe_result(prom_endpoint_cpu)

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_cpu = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + cpu_query + "))"
        dict_return["node_cpu_" + label] = _safe_scalar(prom_endpoint_cpu)

    network_query = "rate(node_network_transmit_bytes_total{device=~\"eno1|eno33np0\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=" + network_query
    dict_return["node_network_out"] = _safe_result(prom_endpoint_nw)

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + network_query + "))"
        dict_return["node_network_out_" + label] = _safe_scalar(prom_endpoint_nw)

    experiment_network_query = "rate(node_network_transmit_bytes_total{device=~\"eno2|eno1d1|enp65s0f0np0\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_exp_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=" + experiment_network_query
    dict_return["experiment_network_out"] = _safe_result(prom_endpoint_exp_nw)

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_exp_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + experiment_network_query + "))"
        dict_return["experiment_network_out_" + label] = _safe_scalar(prom_endpoint_exp_nw)

    memory_query = "(100 - 100 * (sum(node_memory_MemFree_bytes{job=\"node_exporter\"} + node_memory_Cached_bytes{job=\"node_exporter\"}+node_memory_Buffers_bytes{job=\"node_exporter\"}))/sum(node_memory_MemTotal_bytes{job=\"node_exporter\"}))[5m:10s] @ " + timestamp_prom
    prom_endpoint_mem = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(memory_query)
    dict_return["node_memory"] = _safe_result(prom_endpoint_mem)

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_mem = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + urllib.parse.quote(memory_query) + "))"
        dict_return["node_memory_" + label] = _safe_scalar(prom_endpoint_mem)

    return dict_return


def _first_value(result):
    """Pull {"value": [ts, v]}'s v out of a Prometheus vector result, or None
    if empty. The spark_driver scrape target only exists while a job's
    driver is alive, so an empty result here (job crashed before starting a
    streaming query, or query never produced a batch) is expected, not
    exceptional -- unlike the other collectors in this file, this must not
    raise, since it's exactly the signal used to tell a broken job apart
    from a working one.
    """
    return result[0]["value"][1] if result else None


def getSparkStreamingMetrics(main_node_ip, timestamp_prom, job_id=None):
    """Spark's own per-query Structured Streaming metrics, scraped via the
    built-in PrometheusServlet (enabled through spark.metrics.conf +
    spark.sql.streaming.metricsEnabled=true on the shared spark-submit
    command, see spark-provide-job.yaml). The metric names embed a
    per-application-run id and a per-query UUID that changes every job
    submission, so these are matched by regex on __name__ rather than an
    exact name.

    job_id (optional): scopes the match to one specific app id via
    _job_id_pattern_fragment() -- see that function's docstring for why
    this matters (an unscoped ".+" can match multiple still-in-window app
    ids and make the underlying Prometheus query error out). Left
    optional, defaulting to the old unscoped behavior, because only the
    synthetic-shapes call path has a job_id threaded all the way through
    main() today -- see main()'s own comment on this.
    """
    dict_return = {}

    metric_suffixes = {
        "input_rate": "inputRate_total",
        "processing_rate": "processingRate_total",
        "processing_latency_ms": "latency",
    }

    app_fragment = _job_id_pattern_fragment(job_id)
    for label, suffix in metric_suffixes.items():
        name_pattern = "metrics_app_" + app_fragment + "_driver_spark_streaming_.+_" + suffix + "_Value"
        base_query = "{{__name__=~\"{}\"}}[5m:1s] @ {}".format(name_pattern, timestamp_prom)

        # Unlike the plain-identifier PromQL used elsewhere in this file, this
        # query contains a regex with literal '+'/'"' characters -- sent
        # unencoded, '+' gets silently read back as a space by the query
        # string parser, corrupting the regex into a query that matches
        # nothing (no error, just an empty result). Must be quoted.
        # Guarded the same way as getprom.py's analogous NaN-guard spots
        # (five similar unguarded IndexError/KeyError crashes fixed there
        # 2026-10-11): a Prometheus query against a metric name pattern
        # that genuinely has zero matching series on this engine/config can
        # come back as an ERROR response (no "data" key at all), not just
        # an empty "result" list -- confirmed live, 2026-10-16, first
        # distributed synthetic sweep run: this crashed every collect()
        # call past the first, losing the whole evaluationMetrics payload
        # (operatorMetrics included) for every subsequent combo, not just
        # this one sub-metric.
        try:
            prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + base_query + ")")
            data = requests.get(prom_endpoint).json()
            dict_return[label] = _first_value(data['data']['result'])
        except (KeyError, IndexError):
            dict_return[label] = None

        for quantile, pct_label in [(0.98, "98th"), (0.95, "95th"), (0.50, "50th")]:
            try:
                prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("quantile_over_time(" + str(quantile) + ", " + base_query + ")")
                data = requests.get(prom_endpoint).json()
                dict_return[label + "_" + pct_label] = _first_value(data['data']['result'])
            except (KeyError, IndexError):
                dict_return[label + "_" + pct_label] = None

    return dict_return


def _get_operator_rows(name, main_node_ip, timestamp_prom, job_id=None):
    """Fetch one named OperatorMetricsSource gauge's average row count over
    the query window. Shared by getOperatorMetrics's generic sequential
    chain and getAdAnalyticsQuery1OperatorMetrics's bespoke fan-in
    calculation (see that function for why Ad Analytics Query 1 can't use
    the generic chain).

    job_id (optional): see getSparkStreamingMetrics's docstring -- same
    multi-app-id query-ambiguity issue applies here too (confirmed live,
    2026-10-17), just against the operatorMetrics_* gauges instead of the
    spark_streaming_* ones.
    """
    name_pattern = "metrics_app_" + _job_id_pattern_fragment(job_id) + "_driver_operatorMetrics_" + name + "_rows_Value"
    base_query = "{{__name__=~\"{}\"}}[5m:1s] @ {}".format(name_pattern, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + base_query + ")")
    return _first_value(_safe_result(prom_endpoint))


def _capacity_ratio(spark_streaming_metrics):
    """Job-wide "is this job keeping up with its input" ratio, derived from
    Spark's own built-in input_rate/processing_rate (see
    getSparkStreamingMetrics) -- this project's Spark stand-in for
    busy_ratio/backpressure_ratio, the two fields ds2_strategy/
    dhalion_strategy (parallelism_strategies.py) need and that RisingWave's
    per-fragment stream_actor_poll_duration/output_buffer_blocking_duration
    metrics have no Spark equivalent for (Structured Streaming exposes no
    per-operator timing at all, only per-microbatch job-wide numbers -- see
    getOperatorMetrics's own docstring).

    processing_rate < input_rate is a direct, real signal that the pipeline
    is falling behind its input during the measurement window -- the same
    thing backpressure_ratio measures for RisingWave, just at job
    granularity instead of per-fragment. Applied uniformly to every stage
    (see getOperatorMetrics below): a coarser version of the same
    simplification RisingWave's own adapter already documents (per-fragment,
    not per-instance) -- here it's per-job, not per-fragment, because that
    is the finest granularity Spark's built-in metrics actually expose
    without new per-stage timing instrumentation in the job code itself.

    Returns None if either rate is missing/zero (can't compute a ratio, same
    "don't guess" fallback used throughout this file and
    parallelism_strategies.py) -- capped at 1.0 since processing transiently
    faster than input arrives during a catch-up burst isn't "over capacity"
    in the sense busy_ratio means.
    """
    input_rate = spark_streaming_metrics.get("input_rate")
    processing_rate = spark_streaming_metrics.get("processing_rate")
    if input_rate is None or processing_rate is None:
        return None
    input_rate = float(input_rate)
    processing_rate = float(processing_rate)
    if input_rate <= 0:
        return None
    return min(1.0, processing_rate / input_rate)


def getOperatorMetrics(operator_names, main_node_ip, timestamp_prom, batch_duration_ms, spark_streaming_metrics=None, job_id=None):
    """Per-stage row counts + selectivity + throughput for the custom
    OperatorMetricsSource gauges fed by a job's Dataset.observe() calls (see
    MetricsUtils.java) -- the Spark counterpart of Flink/Storm's native
    per-operator numRecordsIn/numRecordsOut. operator_names must be given in
    pipeline order; selectivity for each stage is defined the same way as
    Flink's (this stage's row count / the previous stage's), with the first
    stage's selectivity fixed at 1.0 since it has no upstream to compare
    against. Same regex-by-__name__ matching as getSparkStreamingMetrics,
    for the same reason (the app/run id changes every job submission).

    Throughput is rows / batch_duration_ms (the same trigger-execution
    duration sparkStreamingMetrics.processing_latency_ms already reports --
    all operators run within the same micro-batch, so it applies uniformly)
    -- NOT a per-operator processing latency. Structured Streaming has no
    per-record timing like Flink's latency markers, so there is no
    equivalent "time spent inside this operator" number here; do not confuse
    rows/duration (a rate) with a latency.

    busy_ratio/backpressure_ratio (only populated when spark_streaming_metrics
    is given) are the same job-wide capacity-ratio value replicated onto
    every stage -- see _capacity_ratio's docstring for why this is job-wide,
    not truly per-stage. Present so load_stage_metrics_spark()
    (parallelism_strategies.py) can feed this file's output straight into
    ds2_strategy/dhalion_strategy without a shape mismatch against the
    RisingWave adapter.

    This assumes a single sequential pipeline -- see
    getAdAnalyticsQuery1OperatorMetrics for a job whose stages fan in from
    two independent sources, where this chain-based selectivity doesn't apply.
    """
    rows = {}
    for name in operator_names:
        rows[name] = _get_operator_rows(name, main_node_ip, timestamp_prom, job_id=job_id)

    batch_duration_seconds = None
    if batch_duration_ms is not None and float(batch_duration_ms) > 0:
        batch_duration_seconds = float(batch_duration_ms) / 1000.0

    busy_ratio = _capacity_ratio(spark_streaming_metrics) if spark_streaming_metrics else None
    backpressure_ratio = (1.0 - busy_ratio) if busy_ratio is not None else None

    dict_return = {}
    previous_rows = None
    for name in operator_names:
        current = rows[name]
        selectivity = None
        throughput_per_sec = None
        if current is not None:
            current = float(current)
            if previous_rows is None:
                selectivity = 1.0
            elif previous_rows != 0:
                selectivity = current / previous_rows
            previous_rows = current
            if batch_duration_seconds is not None:
                throughput_per_sec = current / batch_duration_seconds
        dict_return[name] = {
            "rows": current,
            "selectivity": selectivity,
            "throughput_per_sec": throughput_per_sec,
            "busy_ratio": busy_ratio,
            "backpressure_ratio": backpressure_ratio,
        }

    return dict_return


def getEndToEndLatency(main_node_ip, timestamp_prom, observation_name="end_to_end_lag", key_prefix="end_to_end_latency_seconds", job_id=None):
    """Average seconds between Kafka's own per-record ingestion timestamp
    and "now" (when that record's batch is processed) -- see the
    "end_to_end_lag" Dataset.observe() call in WordCountJob.java. This is
    the closest Spark equivalent to Flink's latency markers, though it
    measures ingestion-to-processing lag at the source rather than a marker
    traveling the whole pipeline through to the sink (Structured Streaming
    has no per-record tracing once a stateful aggregation like groupBy
    combines many input records' timestamps into one output row). Same
    regex-by-__name__ matching as getOperatorMetrics/getSparkStreamingMetrics,
    for the same reason (the app/run id changes every job submission).

    observation_name/key_prefix let a job report more than one lag (e.g. Ad
    Analytics Query 1 has two independent Kafka sources, each observed as
    "end_to_end_lag_clicks"/"end_to_end_lag_impressions" -- see
    AdsAnalytics.java) without colliding on the same output keys.
    """
    name_pattern = "metrics_app_" + _job_id_pattern_fragment(job_id) + "_driver_operatorMetrics_" + observation_name + "_rows_Value"
    base_query = "{{__name__=~\"{}\"}}[5m:1s] @ {}".format(name_pattern, timestamp_prom)

    dict_return = {}
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + base_query + ")")
    data = requests.get(prom_endpoint).json()
    value = _first_value(data['data']['result'])
    dict_return[key_prefix] = float(value) if value is not None else None

    for quantile, pct_label in [(0.98, "98th"), (0.95, "95th"), (0.50, "50th")]:
        prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("quantile_over_time(" + str(quantile) + ", " + base_query + ")")
        data = requests.get(prom_endpoint).json()
        value = _first_value(data['data']['result'])
        dict_return[key_prefix + "_" + pct_label] = float(value) if value is not None else None

    return dict_return


def getCombo(job_name, main_node_ip, input_topic, output_topic, timestamp_prom, operator_names=None, has_end_to_end_lag=False):
    combo = {"job_name": job_name}
    combo["queryMetrics"] = getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom)
    spark_streaming_metrics = getSparkStreamingMetrics(main_node_ip, timestamp_prom)
    combo["sparkStreamingMetrics"] = spark_streaming_metrics
    if operator_names:
        combo["operatorMetrics"] = getOperatorMetrics(
            operator_names, main_node_ip, timestamp_prom,
            spark_streaming_metrics.get("processing_latency_ms"),
            spark_streaming_metrics=spark_streaming_metrics)
    if has_end_to_end_lag:
        combo["endToEndLatency"] = getEndToEndLatency(main_node_ip, timestamp_prom)
    combo["hardwareMetrics"] = getHardwareMetrics(main_node_ip, timestamp_prom)
    return combo


def getAdAnalyticsQuery1OperatorMetrics(main_node_ip, timestamp_prom, batch_duration_ms, spark_streaming_metrics=None):
    """Ad Analytics Query 1 has two independent Kafka sources
    ("clicksparser"/"impressionsparser") feeding a single windowed
    aggregation ("window"), not one sequential pipeline -- so unlike
    getOperatorMetrics's generic chain (this stage / the previous one),
    "window"'s selectivity is computed against the *combined*
    clicksparser+impressionsparser row count, since that combined union is
    what it actually aggregates (see the union+conditional-aggregate fix in
    AdsAnalytics.java's class javadoc). clicksparser/impressionsparser each
    get a selectivity of 1.0, same reasoning as any pipeline's first stage.
    """
    batch_duration_seconds = None
    if batch_duration_ms is not None and float(batch_duration_ms) > 0:
        batch_duration_seconds = float(batch_duration_ms) / 1000.0

    busy_ratio = _capacity_ratio(spark_streaming_metrics) if spark_streaming_metrics else None
    backpressure_ratio = (1.0 - busy_ratio) if busy_ratio is not None else None

    def _entry(rows, selectivity):
        throughput = None
        if rows is not None and batch_duration_seconds is not None:
            throughput = rows / batch_duration_seconds
        return {"rows": rows, "selectivity": selectivity, "throughput_per_sec": throughput,
                "busy_ratio": busy_ratio, "backpressure_ratio": backpressure_ratio}

    clicks_rows = _get_operator_rows("clicksparser", main_node_ip, timestamp_prom)
    impressions_rows = _get_operator_rows("impressionsparser", main_node_ip, timestamp_prom)
    window_rows = _get_operator_rows("window", main_node_ip, timestamp_prom)
    sink_rows = _get_operator_rows("sink", main_node_ip, timestamp_prom)

    clicks_rows = float(clicks_rows) if clicks_rows is not None else None
    impressions_rows = float(impressions_rows) if impressions_rows is not None else None
    window_rows = float(window_rows) if window_rows is not None else None
    sink_rows = float(sink_rows) if sink_rows is not None else None

    combined_input = None
    if clicks_rows is not None or impressions_rows is not None:
        combined_input = (clicks_rows or 0) + (impressions_rows or 0)

    window_selectivity = None
    if window_rows is not None and combined_input:
        window_selectivity = window_rows / combined_input

    sink_selectivity = None
    if sink_rows is not None and window_rows:
        sink_selectivity = sink_rows / window_rows

    return {
        "clicksparser": _entry(clicks_rows, 1.0 if clicks_rows is not None else None),
        "impressionsparser": _entry(impressions_rows, 1.0 if impressions_rows is not None else None),
        "window": _entry(window_rows, window_selectivity),
        "sink": _entry(sink_rows, sink_selectivity),
    }


def getWordCount(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Word Count", main_node_ip, "WordCountIn", "WordCountOut", timestamp_prom,
                     operator_names=["source", "tokenizer", "counter", "sink"],
                     has_end_to_end_lag=True)


def getSpikeDetection(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Spike Detection", main_node_ip, "SpikeIn", "SpikeOut", timestamp_prom,
                     operator_names=["source", "parser", "average", "spikedetector", "sink"],
                     has_end_to_end_lag=True)


def getTrafficMonitoring(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Traffic Monitoring", main_node_ip, "TMIn", "TMOut", timestamp_prom,
                     operator_names=["source", "parser", "roadmatcher", "avgspeed", "sink"],
                     has_end_to_end_lag=True)


def getTpch(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("TPCH", main_node_ip, "TPIn", "TPOut", timestamp_prom,
                     operator_names=["source", "parser", "filter", "flatmapper", "counter", "sink"],
                     has_end_to_end_lag=True)


def getLogProcessing(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Log Processing", main_node_ip, "LogIn", "LogOut", timestamp_prom,
                     operator_names=["source", "parser", "counter", "sink"],
                     has_end_to_end_lag=True)


def getTrendingTopics(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Trending Topics", main_node_ip, "TTIn", "TTOut", timestamp_prom,
                     operator_names=["source", "parser", "topicextractor", "counter", "filter", "sink"],
                     has_end_to_end_lag=True)


def getSmartGrid(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Smart Grid", main_node_ip, "SmartGridIn", "SmartGridOut", timestamp_prom,
                     operator_names=["source", "parser", "window", "sink"],
                     has_end_to_end_lag=True)


def getAdAnalytics(main_node_ip, timestamp_prom, job_query_number=None):
    if job_query_number == "1":
        combo = {"job_name": "Ad Analytics"}
        combo["queryMetrics"] = getKafkaMetrics(main_node_ip, "AdAnalyticsIn", "AdAnalyticsOut", timestamp_prom)
        spark_streaming_metrics = getSparkStreamingMetrics(main_node_ip, timestamp_prom)
        combo["sparkStreamingMetrics"] = spark_streaming_metrics
        combo["operatorMetrics"] = getAdAnalyticsQuery1OperatorMetrics(
            main_node_ip, timestamp_prom, spark_streaming_metrics.get("processing_latency_ms"),
            spark_streaming_metrics=spark_streaming_metrics)
        combo["endToEndLatency"] = {}
        combo["endToEndLatency"].update(getEndToEndLatency(
            main_node_ip, timestamp_prom,
            observation_name="end_to_end_lag_clicks", key_prefix="end_to_end_latency_seconds_clicks"))
        combo["endToEndLatency"].update(getEndToEndLatency(
            main_node_ip, timestamp_prom,
            observation_name="end_to_end_lag_impressions", key_prefix="end_to_end_latency_seconds_impressions"))
        combo["hardwareMetrics"] = getHardwareMetrics(main_node_ip, timestamp_prom)
        return combo

    return getCombo("Ad Analytics", main_node_ip, "AdAnalyticsIn", "AdAnalyticsOut", timestamp_prom,
                     operator_names=["parser", "counter", "rollingCTR", "sink"],
                     has_end_to_end_lag=True)


def getSentimentAnalysis(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Sentiment Analysis", main_node_ip, "SentimentIn", "SentimentOut", timestamp_prom,
                     operator_names=["source", "parser", "analyser", "sink"],
                     has_end_to_end_lag=True)


def getClickAnalytics(main_node_ip, timestamp_prom, job_query_number=None):
    # Query 2 has a "geo" stage instead of "repeatvisit"/"reduce" (see
    # ClickAnalytics.java's class javadoc).
    if job_query_number == "2":
        operator_names = ["source", "parser", "geo", "sink"]
    else:
        operator_names = ["source", "parser", "repeatvisit", "reduce", "sink"]
    return getCombo("Click Analytics", main_node_ip, "CAIn", "CAOut", timestamp_prom,
                     operator_names=operator_names,
                     has_end_to_end_lag=True)


def getGoogleCloudMonitoring(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Google cloud Monitoring", main_node_ip, "GoogleIn", "GoogleOut", timestamp_prom,
                     operator_names=["source", "parser", "window", "sink"],
                     has_end_to_end_lag=True)


def getBargainIndex(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Bargain Index", main_node_ip, "BIIn", "BIOut", timestamp_prom,
                     operator_names=["source", "parser", "vwap", "bargain", "sink"],
                     has_end_to_end_lag=True)


def getMachineOutlier(main_node_ip, timestamp_prom, job_query_number=None):
    return getCombo("Machine Outlier", main_node_ip, "MOIn", "MOOut", timestamp_prom,
                     operator_names=["source", "parser", "outlier", "sink"],
                     has_end_to_end_lag=True)


def getLinearRoad(main_node_ip, timestamp_prom, job_query_number=None):
    # Queries 1/2 have a separate "formatter" stage after "mapper"; queries
    # 3/4 emit directly from "mapper" (see LinearRoad.java's class javadoc).
    if job_query_number in ("1", "2"):
        operator_names = ["source", "mapper", "formatter"]
    else:
        operator_names = ["source", "mapper"]
    return getCombo("Linear Road", main_node_ip, "LRIn", "LROut", timestamp_prom,
                     operator_names=operator_names,
                     has_end_to_end_lag=True)


# Synthetic-workload dispatch (dsp_spark_synthetic_jobs), added 2026-10-16 --
# same pattern as getprom.py's Flink synthetic dispatch, adapted to this
# file's job_class-keyed convention. Unlike Flink, Spark has no per-operator
# vertex-discovery step (no REST API call needed to find a vertex id by
# name) -- Prometheus matches _get_operator_rows()'s regex against the
# metric name directly, so this is simpler than the Flink port's version.
def _prom_safe_name(name):
    """Prometheus's own metric-name sanitization turns '-' into '_' when
    exposing a Dropwizard gauge (confirmed live, 2026-10-15: a Java-side
    .observe() name of "window-agg" surfaces as the Prometheus series
    "...operatorMetrics_window_agg_rows_Value", not "...window-agg...").
    Every real-workload stage name in this file (source/tokenizer/counter/
    ...) is hyphen-free, so this normalization is kept local to the
    synthetic-dispatch helpers below rather than folded into
    _get_operator_rows()/getOperatorMetrics() (shared code the 14
    real-workload dispatchers above also depend on)."""
    return name.replace("-", "_")


def getSyntheticOperatorMetrics(operator_names, main_node_ip, timestamp_prom, batch_duration_ms, spark_streaming_metrics=None, job_id=None):
    """Same contract/shape as getOperatorMetrics() above, but looks up each
    name's Prometheus series via _prom_safe_name() (see its docstring)
    while keeping the ORIGINAL (hyphenated) name as this dict's own key, so
    callers see the same stage names the Java .observe() calls (and
    dsp_flink_synthetic_jobs' Flink port) use -- only the Prometheus lookup
    itself needs the underscore form.

    job_id (optional): see getSparkStreamingMetrics's docstring -- threaded
    through here because the synthetic shapes' rapid back-to-back sweep
    submissions are exactly the case that triggers the multi-app-id query
    ambiguity (confirmed live, 2026-10-17)."""
    rows = {name: _get_operator_rows(_prom_safe_name(name), main_node_ip, timestamp_prom, job_id=job_id) for name in operator_names}

    batch_duration_seconds = None
    if batch_duration_ms is not None and float(batch_duration_ms) > 0:
        batch_duration_seconds = float(batch_duration_ms) / 1000.0

    busy_ratio = _capacity_ratio(spark_streaming_metrics) if spark_streaming_metrics else None
    backpressure_ratio = (1.0 - busy_ratio) if busy_ratio is not None else None

    dict_return = {}
    previous_rows = None
    for name in operator_names:
        current = rows[name]
        selectivity = None
        throughput_per_sec = None
        if current is not None:
            current = float(current)
            if previous_rows is None:
                selectivity = 1.0
            elif previous_rows != 0:
                selectivity = current / previous_rows
            previous_rows = current
            if batch_duration_seconds is not None:
                throughput_per_sec = current / batch_duration_seconds
        dict_return[name] = {
            "rows": current,
            "selectivity": selectivity,
            "throughput_per_sec": throughput_per_sec,
            "busy_ratio": busy_ratio,
            "backpressure_ratio": backpressure_ratio,
        }
    return dict_return


def getSyntheticCombo(job_name, main_node_ip, timestamp_prom, operator_names, job_id=None):
    """No Kafka involvement -- synthetic jobs use the built-in "rate" source
    and "noop" sink (see dsp_spark_synthetic_jobs' SyntheticTopology
    javadoc), so this skips getKafkaMetrics() entirely, matching
    getprom.py's getSyntheticMetrics() for the Flink port. endToEndLatency
    is NOT skipped, unlike an earlier version of this function (added
    2026-10-20, see docs/14_issue_log.md's "no end-to-end latency for
    synthetic Spark jobs" finding) -- SyntheticTopology.java's sink() now
    observes "end_to_end_lag" the same way WordCountJob.java does for the
    real workloads (same metric name/column alias, so getEndToEndLatency()
    itself needed no query changes), except measured true source-to-sink
    (ts propagated through every filter/windowAvg/join call), not
    WordCountJob.java's source-only approximation -- see SyntheticTopology
    .java's sink() javadoc for why that divergence from the real-workload
    convention is deliberate (comparability with Flink's/Kafka Streams'
    own true end-to-end metrics was the point of adding this at all).

    job_id (optional): see getSparkStreamingMetrics's docstring -- scopes
    every metric query below to this one job run, fixing a real,
    confirmed-live (2026-10-17) failure mode where the synthetic sweep's
    rapid back-to-back submissions left multiple app-id series in
    Prometheus's lookback window at once."""
    combo = {"job_name": job_name}
    spark_streaming_metrics = getSparkStreamingMetrics(main_node_ip, timestamp_prom, job_id=job_id)
    combo["sparkStreamingMetrics"] = spark_streaming_metrics
    combo["operatorMetrics"] = getSyntheticOperatorMetrics(
        operator_names, main_node_ip, timestamp_prom,
        spark_streaming_metrics.get("processing_latency_ms"),
        spark_streaming_metrics=spark_streaming_metrics, job_id=job_id)
    combo["endToEndLatency"] = getEndToEndLatency(main_node_ip, timestamp_prom, job_id=job_id)
    combo["hardwareMetrics"] = getHardwareMetrics(main_node_ip, timestamp_prom)
    return combo


def getLinearQuerySynthetic(main_node_ip, timestamp_prom, job_query_number=None, job_id=None):
    return getSyntheticCombo("LinearQuery", main_node_ip, timestamp_prom,
                              ["source", "filter1", "window-agg", "filter2", "sink"], job_id=job_id)


def getChainedFiltersSynthetic(main_node_ip, timestamp_prom, job_query_number, job_id=None):
    n = int(job_query_number)
    names = ["source", "window-agg"] + [f"filter{i}" for i in range(1, n + 1)] + ["sink"]
    job_name = {2: "TwoFilters", 3: "ThreeFilters", 4: "FourFilters"}[n]
    return getSyntheticCombo(job_name, main_node_ip, timestamp_prom, names, job_id=job_id)


def getJoinSynthetic(main_node_ip, timestamp_prom, job_query_number, job_id=None):
    n = int(job_query_number)
    names = [f"source-{i}" for i in range(n)]
    if n <= 3:
        names += [f"filter-{i}" for i in range(n)]
    names += [f"join-{i}" for i in range(1, n)] + ["window-agg", "sink"]
    job_name = {2: "TwoWayJoin", 3: "ThreeWayJoin", 4: "FourWayJoin", 5: "FiveWayJoin", 6: "SixWayJoin"}[n]
    return getSyntheticCombo(job_name, main_node_ip, timestamp_prom, names, job_id=job_id)


# job_class (the human-readable name from the controller's job_class field,
# e.g. "Word Count", "Spike Detection") -> collector function. Extend this
# alongside constants_spark.py as more Spark workloads get validated.
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
    "LinearQuery": getLinearQuerySynthetic,
    "TwoFilters": getChainedFiltersSynthetic,
    "ThreeFilters": getChainedFiltersSynthetic,
    "FourFilters": getChainedFiltersSynthetic,
    "TwoWayJoin": getJoinSynthetic,
    "ThreeWayJoin": getJoinSynthetic,
    "FourWayJoin": getJoinSynthetic,
    "FiveWayJoin": getJoinSynthetic,
    "SixWayJoin": getJoinSynthetic,
}

# Job classes whose collector accepts job_id (see _job_id_pattern_fragment's
# docstring for why that matters) -- only the synthetic ones today. The 14
# real-workload collectors above are left untouched/unscoped on purpose:
# threading job_id through their signatures too is a bigger, riskier change
# against already-validated, already-collected sweep data, for a bug this
# project has only confirmed live against the synthetic sweep's much
# tighter back-to-back submission cadence.
SYNTHETIC_JOB_CLASSES = {
    "LinearQuery", "TwoFilters", "ThreeFilters", "FourFilters",
    "TwoWayJoin", "ThreeWayJoin", "FourWayJoin", "FiveWayJoin", "SixWayJoin",
}


@click.command()
@click.option('--main_node_ip', type=str, help='node address where prometheus and the spark master reside')
@click.option('--job_id', type=str, help='spark application id / --name given to spark-submit')
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
    if job_class in SYNTHETIC_JOB_CLASSES:
        processed_metrics = collector(main_node_ip, timestamp_prom, job_query_number, job_id=job_id)
    else:
        processed_metrics = collector(main_node_ip, timestamp_prom, job_query_number)

    job_name = job_class if job_class in JOB_CLASS_DISPATCH else "Word Count"
    current_directory = os.getcwd()
    full_folder_path = os.path.join(current_directory, job_name)
    os.makedirs(full_folder_path, exist_ok=True)

    meta_info = {
        'job_framework': 'Apache Spark',
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

    # Timestamped so repeated runs of the same job/iteration/sample (the
    # common case when testing/benchmarking one workload multiple times)
    # don't overwrite each other's results.
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
