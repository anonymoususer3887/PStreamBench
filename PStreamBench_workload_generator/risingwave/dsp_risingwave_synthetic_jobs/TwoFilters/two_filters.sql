-- TwoFilters (synthetic shape 2 of 9): source -> windowed average ->
-- filter1 -> filter2 -> sink.
--
-- RisingWave counterpart of ChainedFiltersTopology (dsp_flink_synthetic_jobs
-- / dsp_spark_synthetic_jobs / dsp_kafkastreams_synthetic_jobs), query=2.
-- Same data model, window choice, and __KAFKA_BOOTSTRAP_SERVER__
-- substitution convention as linear_query.sql -- see that file's header
-- for the full rationale (SyntheticProducer.java as the shared data
-- source, _rw_kafka_timestamp watermark as the processing-time proxy,
-- INTERVAL unit note).
--
-- Filter ordering/thresholds match every other engine's ChainedFiltersTopology
-- exactly: window-agg happens BEFORE the filter chain (not a RisingWave-
-- specific reordering), thresholds THRESHOLDS = {0.52, 0.50} for n=2,
-- val < threshold (selectivity ~0.5 per filter, confirmed against the
-- Flink/Kafka Streams ports' own THRESHOLDS array).
--
-- Each filter is its own CREATE MATERIALIZED VIEW (not folded into one
-- WHERE with AND) so each stage gets its own independently-templated
-- streaming_parallelism value and its own throughput/selectivity numbers
-- in rw_catalog -- matching the other 3 engines' one-operator-per-filter
-- convention, needed for per-stage Tier 3 metrics.
--
-- Validated locally end-to-end (2026-10-17, local Docker RisingWave
-- v3.0.1 + Kafka): the windowAvg -> filter -> sink pattern (shared with
-- every other shape in this directory) confirmed against a live feed,
-- correct threshold-matched selectivity observed on the filter stage.

CREATE SOURCE IF NOT EXISTS two_filters_source (
    k VARCHAR,
    val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    topic = 'TwoFiltersIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE MATERIALIZED VIEW IF NOT EXISTS two_filters_agg_mv AS
SELECT
    k,
    window_start,
    AVG(val) AS val
FROM TUMBLE(two_filters_source, _rw_kafka_timestamp, INTERVAL '1 second')
GROUP BY k, window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS two_filters_filter1_mv AS
SELECT k, window_start, val
FROM two_filters_agg_mv
WHERE val < 0.52;

CREATE MATERIALIZED VIEW IF NOT EXISTS two_filters_filter2_mv AS
SELECT k, window_start, val
FROM two_filters_filter1_mv
WHERE val < 0.50;

CREATE SINK IF NOT EXISTS two_filters_sink
FROM two_filters_filter2_mv
WITH (
    connector = 'kafka',
    topic = 'TwoFiltersOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
