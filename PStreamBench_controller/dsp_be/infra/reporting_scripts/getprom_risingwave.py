import json
import os
import re
import time
import urllib.parse

import click
import psycopg2
import requests

"""
RisingWave counterpart of getprom_spark.py / getprom_storm.py /
getprom_kafkastreams.py, invoked the same way by JobRunner.get_logs() with
the exact same CLI arguments (see runner.py) -- only the metric-collection
internals differ per SUT. See KAFKA_STREAMS_RISINGWAVE_PLAN.md Part B.4 for
the full design.

Scope: all 14 workloads now have a JOB_CLASS_DISPATCH entry, matching
PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs. Only Word
Count has actually been live-validated end-to-end (2026-07-21, CloudLab,
RisingWave v3.0.1, genuine 5-node distributed cluster: meta+frontend+MinIO
on the master, one compute process per worker) -- every other workload's
.sql translation and stage_relations here are written but unvalidated, per
each workload's own file-header notes.

Unlike Kafka Streams (one independent JVM per node, Part A.1), RisingWave's
compute nodes are one-per-worker-node with meta+frontend on the master (see
Part B.1). Each node's own metrics endpoint (:1222 on computes, :1250/:2222
on the master) is scraped as a separate Prometheus target (see
_prometheus_config_manipulation()'s "risingwave" job in views.py), but every
PromQL query in this file aggregates with sum()/avg() over whatever targets
exist, so no per-topology-shape branching is needed here regardless of node
count -- unlike getStreamsMetrics() in getprom_kafkastreams.py (Part A.4),
which has to enumerate Kafka Streams instances explicitly since each one is
an independent, unaggregated JVM rather than a single query target.
"""


def _first_value(result):
    """Pull {"value": [ts, v]}'s v out of a Prometheus vector result, or None
    if empty -- see getprom_kafkastreams.py's identical helper for why this
    must not raise on an empty result.
    """
    if not result:
        return None
    value = result[0]["value"][1]
    return None if value == "NaN" else value


def getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom):
    """Identical to getprom_kafkastreams.py's getKafkaMetrics(): Kafka's own
    JMX metrics are scraped regardless of which SUT is producing/consuming.
    Reused verbatim -- this function has no RisingWave-specific content.
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


# Known CloudLab hardware-profile NIC names (see infra/views.py's
# getprom_manipulataion()/grafana_manipulation() for the Flink-side version
# of this same per-profile mapping, and runner.py's `cores` dict for the
# profile-name spellings). "Node network" = the control-plane NIC (also
# used for SSH/management); "experiment network" = the isolated NIC CloudLab
# assigns for actual inter-node experiment traffic -- two distinct physical
# interfaces on real CloudLab hardware, not just naming variants of one NIC.
# Kept as an OR'd regex (not a hardware_type-keyed branch) because that's
# how the sibling getprom_{spark,storm,kafkastreams}.py scripts already do
# it; extended here to also include xl170's eno49np0, which the original
# node-network regex was missing (confirmed absent from every sibling
# script too -- a pre-existing gap on that one profile, not introduced by
# this change).
_CLOUDLAB_NODE_NETWORK_DEVICES = "eno1|eno33np0|eno49np0"
_CLOUDLAB_EXPERIMENT_NETWORK_DEVICES = "eno2|eno1d1|enp65s0f0np0"

# Interfaces to exclude when auto-detecting a primary NIC on hosts that
# don't match any known CloudLab profile above (local dev laptops, or any
# future hardware profile not yet added to the lists above) -- loopback,
# container/VM bridges, and macOS's virtual interfaces (AWDL peer-to-peer,
# "low latency WLAN", tunnels, etc.), confirmed against this Mac's own
# node_exporter output (`en0` is the only one that actually carries
# non-zero traffic; everything in this list reports 0 or is clearly
# virtual).
_VIRTUAL_NETWORK_DEVICES = (
    r"lo|lo0|docker.*|veth.*|br-.*|virbr.*|utun.*|awdl.*|llw.*|"
    r"bridge.*|stf.*|gif.*|anpi.*|ap[0-9]*|p2p[0-9]*"
)


def _discover_primary_network_device(main_node_ip, timestamp_prom):
    """Auto-detect the primary non-virtual NIC from node_exporter itself,
    for any host that doesn't match one of the hardcoded CloudLab profile
    device names above -- covers local dev (macOS 'en0', Linux dev boxes'
    'eth0'/'enp0s3', etc.) and any future CloudLab profile without needing
    to keep extending a name list forever. Picks whichever non-virtual
    device currently has the highest transmit-byte counter (i.e. the one
    actually carrying traffic), since a dev laptop has no separate
    node/experiment network split -- the same discovered device is reused
    for both roles in that case (see getHardwareMetrics).
    """
    query = 'topk(1, node_network_transmit_bytes_total{{device!~"{0}"}} @ {1})'.format(
        _VIRTUAL_NETWORK_DEVICES, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(query)
    data = requests.get(prom_endpoint).json()
    result = data['data']['result']
    if not result:
        return None
    return result[0]['metric'].get('device')


def _resolve_network_devices(main_node_ip, timestamp_prom):
    """Returns (node_device_regex, experiment_device_regex) -- the known
    CloudLab names if either is actually present, otherwise an
    auto-detected single device used for both (see
    _discover_primary_network_device's docstring)."""
    probe_query = 'node_network_transmit_bytes_total{{device=~"{0}"}} @ {1}'.format(
        _CLOUDLAB_NODE_NETWORK_DEVICES, timestamp_prom)
    probe = requests.get("http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(probe_query)).json()
    if probe['data']['result']:
        return _CLOUDLAB_NODE_NETWORK_DEVICES, _CLOUDLAB_EXPERIMENT_NETWORK_DEVICES

    discovered = _discover_primary_network_device(main_node_ip, timestamp_prom)
    if discovered is None:
        return _CLOUDLAB_NODE_NETWORK_DEVICES, _CLOUDLAB_EXPERIMENT_NETWORK_DEVICES
    escaped = re.escape(discovered)
    return escaped, escaped


def _resolve_memory_formula(main_node_ip, timestamp_prom):
    """Returns a PromQL expression computing memory-used-percent, using
    the Linux-procfs metric names if present (real CloudLab nodes), or the
    Darwin node_exporter's differently-named memory metrics as a fallback
    (confirmed present on this Mac: node_memory_free_bytes/_total_bytes --
    no direct equivalent of Linux's Cached/Buffers split, so this is a
    simpler free-vs-total approximation, not a discrepancy to worry about
    since it's local-dev-only).

    Deliberately does NOT pin this probe to `timestamp_prom` (unlike every
    other query in this file): which metric-naming convention a node's
    node_exporter uses is a static fact about the deployment, not something
    that changes within a sweep, so pinning to a historical instant only
    adds a staleness race. Confirmed live (2026-08-10): pinning it caused
    ~51% of real CloudLab runs to intermittently see an empty probe result
    at that exact historical second, silently falling back to the Darwin
    formula (which doesn't exist on CloudLab) and nulling out
    node_memory_98/95/50 for that run.
    """
    probe_query = 'node_memory_MemTotal_bytes{job="node_exporter"}'
    probe = requests.get("http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(probe_query)).json()
    if probe['data']['result']:
        return ("(100 - 100 * (sum(node_memory_MemFree_bytes{job=\"node_exporter\"} + "
                "node_memory_Cached_bytes{job=\"node_exporter\"}+node_memory_Buffers_bytes{job=\"node_exporter\"}))"
                "/sum(node_memory_MemTotal_bytes{job=\"node_exporter\"}))")
    return ("(100 - 100 * sum(node_memory_free_bytes{job=\"node_exporter\"})"
            "/sum(node_memory_total_bytes{job=\"node_exporter\"}))")


def getHardwareMetrics(main_node_ip, timestamp_prom):
    """Mostly identical to getprom_kafkastreams.py's getHardwareMetrics()
    (node_exporter metrics are SUT-agnostic) -- diverges from that verbatim
    copy in one respect: network-device and memory-metric selection are
    auto-detected here (see _resolve_network_devices/_resolve_memory_formula)
    instead of hardcoded to CloudLab-only names, so this also works
    against a local dev laptop's node_exporter. Confirmed live (2026-07-27):
    without this, node_network_out/experiment_network_out/node_memory were
    unconditionally null on macOS (device regex never matches 'en0'; Linux
    memory metric names don't exist on Darwin's node_exporter build).
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

    node_device_regex, experiment_device_regex = _resolve_network_devices(main_node_ip, timestamp_prom)

    network_query = "rate(node_network_transmit_bytes_total{device=~\"" + node_device_regex + "\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=" + network_query
    data = requests.get(prom_endpoint_nw).json()
    dict_return["node_network_out"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + network_query + "))"
        data = requests.get(prom_endpoint_nw).json()
        dict_return["node_network_out_" + label] = _first_value(data['data']['result'])

    experiment_network_query = "rate(node_network_transmit_bytes_total{device=~\"" + experiment_device_regex + "\"}[10s])[5m:10s] @ " + timestamp_prom
    prom_endpoint_exp_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=" + experiment_network_query
    data = requests.get(prom_endpoint_exp_nw).json()
    dict_return["experiment_network_out"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_exp_nw = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + experiment_network_query + "))"
        data = requests.get(prom_endpoint_exp_nw).json()
        dict_return["experiment_network_out_" + label] = _first_value(data['data']['result'])

    memory_formula = _resolve_memory_formula(main_node_ip, timestamp_prom)
    memory_query = memory_formula + "[5m:10s] @ " + timestamp_prom
    prom_endpoint_mem = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(memory_query)
    data = requests.get(prom_endpoint_mem).json()
    dict_return["node_memory"] = data['data']['result']

    for quantile, label in [(0.98, "98"), (0.95, "95"), (0.50, "50")]:
        prom_endpoint_mem = "http://" + main_node_ip + ":9090/api/v1/query?query=avg(quantile_over_time(" + str(quantile) + "," + urllib.parse.quote(memory_query) + "))"
        data = requests.get(prom_endpoint_mem).json()
        dict_return["node_memory_" + label] = _first_value(data['data']['result'])

    return dict_return


def getActorMetrics(main_node_ip, timestamp_prom):
    """RisingWave's own native metrics (Tier 2), scraped via its built-in
    Prometheus endpoint (:1222 -- see this file's module docstring re: fused
    single-process playground mode vs a real distributed deployment).

    Unlike Kafka Streams, RisingWave has no per-record end-to-end latency
    metric even in principle for an aggregating query -- COUNT(*) loses
    per-row identity, so there is no analog of KIP-613's
    record-e2e-latency-avg/max here. meta_barrier_duration_seconds (the
    meta node's own view of total barrier round-trip time -- injection to
    completion, RisingWave's standard "checkpoint latency" signal) is used
    here as the closest native processing-latency metric for this kind of
    query. True per-event end-to-end latency via the _rw_kafka_timestamp
    hidden source column (confirmed queryable, see word_count.sql's
    validation notes) only makes sense for a non-aggregating/pass-through
    workload -- add a getEndToEndLatency() function using that column once
    one of those (e.g. Machine Outlier) lands, per Part B.2 Phase 1.

    NOTE: an earlier version of this function used
    stream_barrier_inflight_duration_seconds (a per-actor, compute-node-side
    metric) instead -- that metric does exist, but returned null in a live
    Word Count run under this test pass, likely because it only has samples
    when a barrier is measurably delayed in-flight, not on every barrier
    tick. meta_barrier_duration_seconds is meta's own aggregate of every
    barrier's total duration (recorded once per barrier_interval_ms, ~1s by
    default), so it should have samples any time the job is running at all.

    NOTE 2: confirmed live on a genuine 4-node distributed cluster
    (2026-07-27) that avg_over_time(base_query) can come back NaN (not
    absent -- a real sample, just NaN-valued) for a job that's only been
    running ~1-2 minutes, while max_over_time(base_query) over the exact
    same underlying series returns a real number in the same window.
    Root cause: histogram_quantile(0.99, ...) legitimately emits NaN at
    early evaluation instants within the job's short lifetime (e.g. an
    instant where the barrier-duration histogram's bucket counts are all
    zero, a 0/0 inside histogram_quantile) -- Prometheus's avg_over_time
    includes that NaN in its sum (poisoning the whole average to NaN),
    while max_over_time skips NaN samples during its reduction and returns
    the genuine max. So "avg is null, max is a real number" does not mean
    the metric has no data -- it means the metric has real data AND at
    least one early NaN sample. Falling back to the (real, measured) max
    is a better answer than leaving a measurable latency metric null.
    """
    dict_return = {}

    base_query = "histogram_quantile(0.99, sum(rate(meta_barrier_duration_seconds_bucket[1m])) by (le))[5m:1s] @ {}".format(timestamp_prom)
    for label, time_fn in [("barrier_inflight_latency_avg", "avg_over_time"), ("barrier_inflight_latency_max", "max_over_time")]:
        prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(time_fn + "(" + base_query + ")")
        data = requests.get(prom_endpoint).json()
        dict_return[label] = _first_value(data['data']['result'])

    if dict_return["barrier_inflight_latency_avg"] is None and dict_return["barrier_inflight_latency_max"] is not None:
        dict_return["barrier_inflight_latency_avg"] = dict_return["barrier_inflight_latency_max"]

    return dict_return


def _resolve_fragment_ids(rw_host, rw_port, relation_name):
    """Look up the fragment id(s) tagged to a named source/materialized
    view/sink via RisingWave's own SQL catalog (rw_catalog.rw_fragments
    joined against rw_relations for sources/MVs, rw_sinks for sinks).

    This is a supplementary SQL lookup for *identifying which fragment_id is
    which pipeline stage* -- not a competing metrics source. The actual
    throughput/selectivity numbers still come from Prometheus in
    getOperatorMetrics() below, consistent with Part B.4's "SQL is
    supplementary only" principle. Needed because, unlike Kafka Streams'
    named Processor nodes, RisingWave's fragment_ids are opaque integers
    assigned by the query planner and differ per query plan/run -- there is
    no other way to know "which fragment_id is the sink" without asking the
    catalog.

    Confirmed live (2026-07-21): a materialized view's own relation id can
    be shared by more than one fragment (e.g. word_count_mv's id 12 tagged
    both the SOURCE_SCAN/unnest fragment and the MVIEW/group-by fragment),
    so this returns a list of (fragment_id, flags, table_id) tuples rather
    than assuming one fragment per relation name -- callers must filter by
    flag. table_id is also returned (not just used internally) because it's
    the same id stream_memory_usage's `table_id` label uses (confirmed live
    2026-07-27) -- reusing this lookup avoids a second catalog round-trip
    just to resolve memory-usage's join key.
    """
    conn = psycopg2.connect(host=rw_host, port=rw_port, dbname="dev", user="root")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.fragment_id, f.flags, f.table_id
                FROM rw_catalog.rw_fragments f
                JOIN rw_catalog.rw_relations r ON f.table_id = r.id
                WHERE r.name = %s
                UNION
                SELECT f.fragment_id, f.flags, f.table_id
                FROM rw_catalog.rw_fragments f
                JOIN rw_catalog.rw_sinks s ON f.table_id = s.id
                WHERE s.name = %s
                ORDER BY fragment_id
                """,
                (relation_name, relation_name),
            )
            return cur.fetchall()
    finally:
        conn.close()


def _get_fragment_rate(fragment_id, metric_name, main_node_ip, timestamp_prom):
    """Sum a per-fragment counter (stream_source_output_rows_counts,
    stream_actor_in_record_cnt, stream_actor_out_record_cnt, or
    stream_sink_input_row_count -- all confirmed present with these exact
    names/labels, see word_count.sql's validation notes) across every actor
    of that fragment, as a rate over the recording window. Counterpart of
    getprom_kafkastreams.py's _get_operator_rate().
    """
    query = 'sum(rate({0}{{fragment_id="{1}"}}[1m]))[5m:1s] @ {2}'.format(metric_name, fragment_id, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + query + ")")
    data = requests.get(prom_endpoint).json()
    return _first_value(data['data']['result'])


def _get_fragment_ratio(fragment_id, metric_name, main_node_ip, timestamp_prom):
    """Fraction of the last minute this fragment's actors spent in a given
    tokio-tracked duration counter (nanoseconds), e.g. blocked writing to a
    full downstream channel (stream_actor_output_buffer_blocking_duration_ns
    -- a direct backpressure signal: near 1.0 means this stage is
    bottlenecked by a slower downstream consumer) or actively polling
    (stream_actor_poll_duration -- a busy/CPU-time signal). Both confirmed
    present, labeled by fragment_id, on a live local RisingWave instance
    (2026-07-27). Rate is ns/s, so dividing by 1e9 gives a 0..1 ratio
    (can exceed 1.0 if the fragment has more than one actor summed).
    """
    query = '(sum(rate({0}{{fragment_id="{1}"}}[1m]))/1e9)[5m:1s] @ {2}'.format(metric_name, fragment_id, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote("avg_over_time(" + query + ")")
    data = requests.get(prom_endpoint).json()
    value = _first_value(data['data']['result'])
    # A stage with no measurable blocking/polling in this window (e.g. a
    # stateless pass-through source with nothing to poll) has an empty
    # series here, not a "1e9 constant" series -- 0.0 is the correct
    # answer, not unknown, same precedent as bug #13's selectivity fix.
    return 0.0 if value is None else float(value)


def _get_table_memory_usage(table_id, main_node_ip, timestamp_prom):
    """Current operator state size (bytes) for a materialized view/table,
    from RisingWave's native stream_memory_usage{table_id=...} gauge --
    confirmed present locally (2026-07-27), labeled by table_id (the same
    id _resolve_fragment_ids already resolves via rw_relations.id, reused
    here rather than a second catalog lookup) and desc (which internal
    state store this is, e.g. "Materialize", "HashAggState" -- summed
    across all of a table's state stores for one total figure per stage).
    Stateless stages (e.g. a bare SOURCE, which holds no operator state)
    have no series here at all -- 0.0 is the correct answer (no state
    kept), not unknown.
    """
    query = 'sum(stream_memory_usage{{table_id="{0}"}} @ {1})'.format(table_id, timestamp_prom)
    prom_endpoint = "http://" + main_node_ip + ":9090/api/v1/query?query=" + urllib.parse.quote(query)
    data = requests.get(prom_endpoint).json()
    value = _first_value(data['data']['result'])
    return 0.0 if value is None else float(value)


def getOperatorMetrics(stage_relations, main_node_ip, rw_port, timestamp_prom):
    """Per-stage throughput (records/sec), selectivity, memory usage,
    backpressure ratio, and busy ratio (Tier 3) -- the RisingWave
    counterpart of getprom_kafkastreams.py's getOperatorMetrics(). Unlike
    Kafka Streams (custom OperatorMetricsProcessor sensors needed, Part
    A.4), this is entirely native -- RisingWave already exposes
    per-fragment in/out counters, per-table state size, and per-actor
    blocking/busy time for every query, no per-job instrumentation code
    required (Part B.4's "no bolt-on instrumentation needed").

    stage_relations must be given in pipeline order as
    [(stage_name, relation_name, flag_substring), ...] -- flag_substring
    disambiguates which fragment to use when a relation name resolves to
    more than one fragment (see _resolve_fragment_ids' docstring), and which
    Prometheus metric applies: 'SOURCE' -> stream_source_output_rows_counts,
    'SINK' -> stream_sink_input_row_count, anything else (SOURCE_SCAN,
    MVIEW, ...) -> stream_actor_out_record_cnt (in-fragment output).

    memory_usage_bytes/backpressure_ratio/busy_ratio (added 2026-07-27,
    confirmed present on a live local RisingWave instance) -- see
    _get_table_memory_usage/_get_fragment_ratio's docstrings for exactly
    what each measures and why an empty series there means 0.0, not
    unknown.
    """
    resolved = {}
    for stage_name, relation_name, flag_substring in stage_relations:
        fragments = _resolve_fragment_ids(main_node_ip, rw_port, relation_name)
        matching = [(fid, tid) for fid, flags, tid in fragments if flag_substring in flags]
        if not matching:
            resolved[stage_name] = None
            continue
        fragment_id, table_id = matching[0]
        if flag_substring == "SOURCE":
            rate_metric = "stream_source_output_rows_counts"
        elif flag_substring == "SINK":
            rate_metric = "stream_sink_input_row_count"
        else:
            rate_metric = "stream_actor_out_record_cnt"
        resolved[stage_name] = {
            "throughput": _get_fragment_rate(fragment_id, rate_metric, main_node_ip, timestamp_prom),
            "memory_usage_bytes": _get_table_memory_usage(table_id, main_node_ip, timestamp_prom),
            "backpressure_ratio": _get_fragment_ratio(
                fragment_id, "stream_actor_output_buffer_blocking_duration_ns", main_node_ip, timestamp_prom),
            "busy_ratio": _get_fragment_ratio(
                fragment_id, "stream_actor_poll_duration", main_node_ip, timestamp_prom),
        }

    dict_return = {}
    previous_rate = None
    for stage_name, _, _ in stage_relations:
        stage = resolved[stage_name]
        current = stage["throughput"] if stage is not None else None
        selectivity = None
        if current is not None:
            current = float(current)
            if previous_rate is None:
                selectivity = 1.0
            elif previous_rate != 0:
                selectivity = current / previous_rate
            else:
                # previous_rate == 0 (upstream stage had zero throughput in
                # this window, e.g. right after job startup before the
                # first row has propagated) -- avoid a real division by
                # zero, but report 0.0 rather than leaving this null.
                # Confirmed live (2026-07-27): this was the single most
                # common cause of null operatorMetrics in an otherwise
                # fully-populated evaluationMetrics run.
                selectivity = 0.0
            previous_rate = current
        dict_return[stage_name] = {
            "throughput_per_sec": current,
            "selectivity": selectivity,
            "memory_usage_bytes": stage["memory_usage_bytes"] if stage is not None else 0.0,
            "backpressure_ratio": stage["backpressure_ratio"] if stage is not None else 0.0,
            "busy_ratio": stage["busy_ratio"] if stage is not None else 0.0,
        }
    return dict_return


def getCombo(job_name, main_node_ip, rw_port, input_topic, output_topic, timestamp_prom, stage_relations=None):
    combo = {"job_name": job_name}
    combo["queryMetrics"] = getKafkaMetrics(main_node_ip, input_topic, output_topic, timestamp_prom)
    combo["actorMetrics"] = getActorMetrics(main_node_ip, timestamp_prom)
    if stage_relations:
        combo["operatorMetrics"] = getOperatorMetrics(stage_relations, main_node_ip, rw_port, timestamp_prom)
    combo["hardwareMetrics"] = getHardwareMetrics(main_node_ip, timestamp_prom)
    return combo


def getWordCount(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline: word_count_source (SOURCE) -> unnest/split (SOURCE_SCAN,
    # shares word_count_mv's relation id) -> group-by (MVIEW, same relation
    # id) -> word_count_sink (SINK). See word_count.sql.
    stage_relations = [
        ("source", "word_count_source", "SOURCE"),
        ("tokenizer", "word_count_mv", "SOURCE_SCAN"),
        ("counter", "word_count_mv", "MVIEW"),
        ("sink", "word_count_sink", "SINK"),
    ]
    return getCombo("Word Count", main_node_ip, rw_port, "WordCountIn", "WordCountOut", timestamp_prom,
                     stage_relations=stage_relations)


def getSmartGrid(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per smart_grid.sql: smart_grid_source (SOURCE) -> parse+HOP
    # window+percentile_cont (MVIEW) -> smart_grid_sink (SINK). Unlike
    # Word Count, this hasn't been live-validated (Part B.2 Phase 1, SQL
    # written but not yet run against a real cluster) -- in particular it's
    # not confirmed whether the CTE-based parsing step fuses into the same
    # fragment as the MVIEW or gets its own SOURCE_SCAN fragment the way
    # Word Count's unnest() does. Assuming the simpler 3-fragment shape
    # (source/mview/sink) for now; revisit once validated live -- if a
    # separate parsing fragment does exist, its stage should be added here
    # the same way "tokenizer" was for Word Count.
    stage_relations = [
        ("source", "smart_grid_source", "SOURCE"),
        ("window", "smart_grid_mv", "MVIEW"),
        ("sink", "smart_grid_sink", "SINK"),
    ]
    return getCombo("Smart Grid", main_node_ip, rw_port, "SmartGridIn", "SmartGridOut", timestamp_prom,
                     stage_relations=stage_relations)


def getLogProcessing(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per logs_analyzer.sql: log_processing_source (SOURCE) ->
    # parse+HOP window+COUNT (MVIEW) -> log_processing_sink (SINK). Not yet
    # live-validated -- see logs_analyzer.sql's header comment (same
    # 3-fragment-shape assumption as Smart Grid, but COUNT(*) is a much
    # more standard streaming aggregate than percentile_cont, so this one
    # carries less risk of needing a different query structure).
    stage_relations = [
        ("source", "log_processing_source", "SOURCE"),
        ("window", "log_processing_mv", "MVIEW"),
        ("sink", "log_processing_sink", "SINK"),
    ]
    return getCombo("Log Processing", main_node_ip, rw_port, "LogIn", "LogOut", timestamp_prom,
                     stage_relations=stage_relations)


def getMachineOutlier(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per machine_outlier.sql: machine_outlier_source (SOURCE) ->
    # two materialized views (machine_outlier_reference computes the
    # per-machine-per-window 75th-percentile profile; machine_outlier_mv
    # joins raw readings against it and filters by distance) ->
    # machine_outlier_sink (SINK). Not yet live-validated -- this is also
    # the workload with the most structural uncertainty (windowed self-join
    # across two MVs is not confirmed to work as written, see
    # machine_outlier.sql's header comment), so the fragment shape assumed
    # here (4 stages: source, reference, outlier-join, sink) is the least
    # confident of any workload's stage_relations so far and should be the
    # first thing double-checked once this gets validated live.
    stage_relations = [
        ("source", "machine_outlier_source", "SOURCE"),
        ("reference", "machine_outlier_reference", "MVIEW"),
        ("outlier", "machine_outlier_mv", "MVIEW"),
        ("sink", "machine_outlier_sink", "SINK"),
    ]
    return getCombo("Machine Outlier", main_node_ip, rw_port, "MOIn", "MOOut", timestamp_prom,
                     stage_relations=stage_relations)


def getBargainIndex(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per bargain_index.sql: bargain_index_source (SOURCE) ->
    # VWAP+filter (MVIEW) -> bargain_index_sink (SINK). Not yet
    # live-validated -- see bargain_index.sql's header comment (ordered
    # ARRAY_AGG for "last close price" is the main open question, same risk
    # category as Smart Grid's percentile_cont).
    stage_relations = [
        ("source", "bargain_index_source", "SOURCE"),
        ("vwap", "bargain_index_mv", "MVIEW"),
        ("sink", "bargain_index_sink", "SINK"),
    ]
    return getCombo("Bargain Index", main_node_ip, rw_port, "BIIn", "BIOut", timestamp_prom,
                     stage_relations=stage_relations)


def getTrendingTopics(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per trending_topics.sql: trending_topics_source (SOURCE) ->
    # tokenize+filter+HOP window+COUNT+HAVING (MVIEW) -> trending_topics_sink
    # (SINK). Not yet live-validated -- see trending_topics.sql's header
    # comment (HAVING on a streaming aggregate is the main open question
    # specific to this workload).
    stage_relations = [
        ("source", "trending_topics_source", "SOURCE"),
        ("counter", "trending_topics_mv", "MVIEW"),
        ("sink", "trending_topics_sink", "SINK"),
    ]
    return getCombo("Trending Topics", main_node_ip, rw_port, "TTIn", "TTOut", timestamp_prom,
                     stage_relations=stage_relations)


def getClickAnalytics(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline shape is the same across all 3 queries (source -> one MVIEW
    # -> sink -- see click_analytics.sql/_q2.sql/_q3.sql, all reusing the
    # same object names since runner.py::_run_risingwave_job picks the
    # right .sql file variant per job_query_number), so no query-specific
    # stage-name branching is actually needed here (unlike
    # getprom_kafkastreams.py's getClickAnalytics, whose Q2 has a distinct
    # "geo" stage at the Java-class level) -- the stage LOGIC differs
    # (Q1 total/unique visits, Q2 stateless geo lookup, Q3 count-window
    # batching) but the fragment shape it produces (one source, one mview,
    # one sink) is identical across all three.
    stage_relations = [
        ("source", "click_analytics_source", "SOURCE"),
        ("counter", "click_analytics_mv", "MVIEW"),
        ("sink", "click_analytics_sink", "SINK"),
    ]
    return getCombo("Click Analytics", main_node_ip, rw_port, "CAIn", "CAOut", timestamp_prom,
                     stage_relations=stage_relations)


def getTrafficMonitoring(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per traffic_monitoring.sql: traffic_monitoring_source
    # (SOURCE) -> parse+filter+HOP window+AVG+HAVING (MVIEW) ->
    # traffic_monitoring_sink (SINK). Not yet live-validated -- see
    # traffic_monitoring.sql's header comment (the road-matching
    # simplification doesn't affect the metrics-collection shape, but the
    # array_length()-based malformed-row filter is an open question, same
    # category as Trending Topics' HAVING usage).
    stage_relations = [
        ("source", "traffic_monitoring_source", "SOURCE"),
        ("avgspeed", "traffic_monitoring_mv", "MVIEW"),
        ("sink", "traffic_monitoring_sink", "SINK"),
    ]
    return getCombo("Traffic Monitoring", main_node_ip, rw_port, "TMIn", "TMOut", timestamp_prom,
                     stage_relations=stage_relations)


def getSpikeDetection(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per spike_detection.sql: spike_detection_source (SOURCE) ->
    # spike_detection_average (MVIEW, per-sensor-window AVG) ->
    # spike_detection_mv (MVIEW, self-join + filter) ->
    # spike_detection_sink (SINK). Not yet live-validated -- same
    # windowed-self-join open question as Machine Outlier, though this
    # workload's aggregate (plain AVG) is lower-risk than that one's
    # percentile_cont.
    stage_relations = [
        ("source", "spike_detection_source", "SOURCE"),
        ("average", "spike_detection_average", "MVIEW"),
        ("spikedetector", "spike_detection_mv", "MVIEW"),
        ("sink", "spike_detection_sink", "SINK"),
    ]
    return getCombo("Spike Detection", main_node_ip, rw_port, "SpikeIn", "SpikeOut", timestamp_prom,
                     stage_relations=stage_relations)


def getSentimentAnalysis(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per sentiment_analysis.sql: sentiment_analysis_source
    # (SOURCE) -> tokenize+AFINN-lexicon-join+classify (MVIEW) ->
    # sentiment_analysis_sink (SINK). No separate "analyser" stage the way
    # getprom_kafkastreams.py's getSentimentAnalysis has -- the tokenize/
    # lookup/classify steps are all one CTE chain inside a single MV here,
    # unlike Machine Outlier/Spike Detection which needed two separate MVs.
    # Not yet live-validated -- see sentiment_analysis.sql's header comment;
    # this is also the only workload with a static reference TABLE (the
    # AFINN lexicon) joined per-event rather than a window-boundary join.
    stage_relations = [
        ("source", "sentiment_analysis_source", "SOURCE"),
        ("analyser", "sentiment_analysis_mv", "MVIEW"),
        ("sink", "sentiment_analysis_sink", "SINK"),
    ]
    return getCombo("Sentiment Analysis", main_node_ip, rw_port, "SentimentIn", "SentimentOut", timestamp_prom,
                     stage_relations=stage_relations)


def getLinearRoad(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Fragment shape genuinely differs by query (see linear_road*.sql's
    # header comments), unlike Click Analytics/Ad Analytics where every
    # variant collapses to the same single-MV shape:
    #   Q1 (toll notification): source -> vehicle_latest (MVIEW, ROW_NUMBER
    #     dedup) -> segment_stats (MVIEW, grouped off vehicle_latest) ->
    #     linear_road_mv (MVIEW, transition-detect + temporal join) -> sink.
    #   Q2 (accident detection): source -> linear_road_mv (MVIEW, single
    #     HOP+GROUP BY/HAVING) -> sink -- no auxiliary MVs at all.
    #   Q3 (daily expenditure): source -> vehicle_latest -> segment_stats ->
    #     toll_events (MVIEW) -> expenditure (MVIEW, running SUM) ->
    #     linear_road_mv (MVIEW, temporal join + filter to type=2) -> sink.
    #   Q4 (travel time): source -> vehicle_start (MVIEW, MIN aggregate) ->
    #     linear_road_mv (MVIEW, temporal join) -> sink.
    # Only the common source/final-mv/sink stages are tracked here for now
    # (matching the granularity already used for every other multi-MV
    # workload's dispatch entry, e.g. Machine Outlier/Spike Detection,
    # rather than trying to name every auxiliary MV per query variant) --
    # revisit with query-specific stage lists once this is live-validated
    # and the auxiliary-MV fragment shapes are confirmed. This is also the
    # least-validated workload of any implemented so far (temporal joins +
    # unbounded ROW_NUMBER dedup, see linear_road.sql's header comment).
    stage_relations = [
        ("source", "linear_road_source", "SOURCE"),
        ("result", "linear_road_mv", "MVIEW"),
        ("sink", "linear_road_sink", "SINK"),
    ]
    return getCombo("Linear Road", main_node_ip, rw_port, "LRIn", "LROut", timestamp_prom,
                     stage_relations=stage_relations)


def getGoogleCloudMonitoring(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per google_cloud_monitoring.sql/_q2.sql: googlecloud_monitoring_source
    # (SOURCE) -> HOP window+SUM or AVG (MVIEW) -> googlecloud_monitoring_sink
    # (SINK). Object prefix is "googlecloud_monitoring" (not
    # "google_cloud_monitoring") -- see that file's header comment for why
    # (the constant's value has a lowercase "cloud"). Not yet
    # live-validated, but structurally low-risk -- same HOP+SUM/AVG shape
    # as Smart Grid/Log Processing.
    stage_relations = [
        ("source", "googlecloud_monitoring_source", "SOURCE"),
        ("aggregate", "googlecloud_monitoring_mv", "MVIEW"),
        ("sink", "googlecloud_monitoring_sink", "SINK"),
    ]
    return getCombo("Google cloud Monitoring", main_node_ip, rw_port, "GoogleIn", "GoogleOut", timestamp_prom,
                     stage_relations=stage_relations)


def getAdAnalytics(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline shape is the same across both queries (source -> one MVIEW ->
    # sink -- see ads_analytics.sql/_q2.sql, both reusing the same object
    # names since runner.py::_run_risingwave_job picks the right .sql file
    # variant per job_query_number). Q1 collapses a windowed stream-stream
    # join into a single GROUP BY (see ads_analytics.sql's header comment for
    # the equivalence argument); Q2 uses a HOP-windowed ratio-of-sums instead
    # of Q1's unbounded one -- but both produce one source/one mview/one sink
    # fragment shape, so no query-specific stage-name branching is needed
    # here, same reasoning as Click Analytics above. Not yet live-validated.
    stage_relations = [
        ("source", "ad_analytics_source", "SOURCE"),
        ("ctr", "ad_analytics_mv", "MVIEW"),
        ("sink", "ad_analytics_sink", "SINK"),
    ]
    return getCombo("Ad Analytics", main_node_ip, rw_port, "AdAnalyticsIn", "AdAnalyticsOut", timestamp_prom,
                     stage_relations=stage_relations)


def getTpch(main_node_ip, rw_port, timestamp_prom, job_query_number=None):
    # Pipeline per tpch.sql: t_p_c_h_source (SOURCE) -> filter+GROUP BY+COUNT
    # (MVIEW) -> t_p_c_h_sink (SINK). No windowing at all -- structurally
    # identical to the already-validated Word Count, so this is one of the
    # lower-risk workloads still awaiting live validation. Object prefix is
    # "t_p_c_h" not "tpch" -- see tpch.sql's header comment for why (the
    # constant "TPCH" is all-uppercase, so runner.py's
    # underscore-before-each-capital-letter naming scheme inserts one
    # before every letter).
    stage_relations = [
        ("source", "t_p_c_h_source", "SOURCE"),
        ("counter", "t_p_c_h_mv", "MVIEW"),
        ("sink", "t_p_c_h_sink", "SINK"),
    ]
    return getCombo("TPCH", main_node_ip, rw_port, "TPIn", "TPOut", timestamp_prom,
                     stage_relations=stage_relations)


# job_class (the human-readable name from the controller's job_class field,
# e.g. "Word Count") -> collector function. Word Count is validated live;
# Smart Grid's .sql is written but not yet validated (see its own header
# comment) -- extend alongside constants_risingwave.py and each workload's
# .sql translation as they land (see KAFKA_STREAMS_RISINGWAVE_PLAN.md Part
# B.2's phased order).
# Query-variant entries (added 2026-08-01, bug #23 in BUGS_AND_FIXES.md):
# run_sweep_risingwave.py's ADDITIONAL_QUERY_VARIANTS passes "<base
# workload> Q<N>" (e.g. "Ad Analytics Q1") as --job_class verbatim, to
# keep its own manifest keys/evaluationMetrics folder separate from the
# base workload's already-completed run. Without a matching dict key
# here, `JOB_CLASS_DISPATCH.get(job_class, getWordCount)` silently fell
# back to getWordCount AND `main()`'s `job_name = job_class if job_class
# in JOB_CLASS_DISPATCH else "Word Count"` fell back to the "Word Count"
# OUTPUT FOLDER too -- every variant combo's real metrics got collected
# correctly but written into Word Count/, where the sweep script (which
# checks the "<base> Q<N>" folder for a new file) never found them,
# reporting a false "no_metrics_file" for every single combo. Same
# collector function as the base workload -- the query variant changes
# the SQL query.py file resolved (job_query_number, see
# _risingwave_job_manipulation), not this script's metrics-collection
# stage_relations, which are identical across query variants of one
# workload (confirmed for every entry added here -- see each getXxx()
# function's own comment on why one shape covers all its variants).
JOB_CLASS_DISPATCH = {
    "Word Count": getWordCount,
    "Smart Grid": getSmartGrid,
    "Smart Grid Q2": getSmartGrid,
    "Log Processing": getLogProcessing,
    "Log Processing Q2": getLogProcessing,
    "Machine Outlier": getMachineOutlier,
    "Bargain Index": getBargainIndex,
    "Trending Topics": getTrendingTopics,
    "Click Analytics": getClickAnalytics,
    "Click Analytics Q2": getClickAnalytics,
    "Click Analytics Q3": getClickAnalytics,
    "Traffic Monitoring": getTrafficMonitoring,
    "Spike Detection": getSpikeDetection,
    "Sentiment Analysis": getSentimentAnalysis,
    "TPCH": getTpch,
    "TPCH Q2": getTpch,
    "Ad Analytics": getAdAnalytics,
    "Ad Analytics Q1": getAdAnalytics,
    "Linear Road": getLinearRoad,
    "Google cloud Monitoring": getGoogleCloudMonitoring,
    "Google cloud Monitoring Q2": getGoogleCloudMonitoring,
}


@click.command()
@click.option('--main_node_ip', type=str, help='node address where prometheus resides')
@click.option('--rw_port', type=str, default='4566', help='RisingWave psql/pgwire port')
@click.option('--job_id', type=str, help='RisingWave job identifier (see runner.py::_run_risingwave_job)')
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
def main(main_node_ip, rw_port, job_id, job_class, job_run_time, job_parallelization,
         job_query_number, job_window_size, job_window_slide_size,
         job_iteration, job_sample, producer_event_per_second, hardware_type,
         num_nodes, num_slots, enumeration_strategy):
    timestamp_prom = str(time.time() - 30)

    collector = JOB_CLASS_DISPATCH.get(job_class, getWordCount)
    processed_metrics = collector(main_node_ip, rw_port, timestamp_prom, job_query_number)

    job_name = job_class if job_class in JOB_CLASS_DISPATCH else "Word Count"
    current_directory = os.getcwd()
    full_folder_path = os.path.join(current_directory, job_name)
    os.makedirs(full_folder_path, exist_ok=True)

    meta_info = {
        'job_framework': 'RisingWave',
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
