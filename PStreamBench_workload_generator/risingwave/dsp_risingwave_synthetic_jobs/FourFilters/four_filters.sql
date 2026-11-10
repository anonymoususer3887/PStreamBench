-- FourFilters (synthetic shape 4 of 9): source -> windowed average ->
-- filter1 -> filter2 -> filter3 -> filter4 -> sink. Same conventions as
-- two_filters.sql (see its header) -- query=4, THRESHOLDS = {0.52, 0.50,
-- 0.48, 0.46} matching the Flink/Spark/Kafka Streams ports' own array
-- exactly.

CREATE SOURCE IF NOT EXISTS four_filters_source (
    k VARCHAR,
    val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    topic = 'FourFiltersIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE MATERIALIZED VIEW IF NOT EXISTS four_filters_agg_mv AS
SELECT
    k,
    window_start,
    AVG(val) AS val
FROM TUMBLE(four_filters_source, _rw_kafka_timestamp, INTERVAL '1 second')
GROUP BY k, window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS four_filters_filter1_mv AS
SELECT k, window_start, val
FROM four_filters_agg_mv
WHERE val < 0.52;

CREATE MATERIALIZED VIEW IF NOT EXISTS four_filters_filter2_mv AS
SELECT k, window_start, val
FROM four_filters_filter1_mv
WHERE val < 0.50;

CREATE MATERIALIZED VIEW IF NOT EXISTS four_filters_filter3_mv AS
SELECT k, window_start, val
FROM four_filters_filter2_mv
WHERE val < 0.48;

CREATE MATERIALIZED VIEW IF NOT EXISTS four_filters_filter4_mv AS
SELECT k, window_start, val
FROM four_filters_filter3_mv
WHERE val < 0.46;

CREATE SINK IF NOT EXISTS four_filters_sink
FROM four_filters_filter4_mv
WITH (
    connector = 'kafka',
    topic = 'FourFiltersOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
