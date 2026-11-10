-- ThreeFilters (synthetic shape 3 of 9): source -> windowed average ->
-- filter1 -> filter2 -> filter3 -> sink. Same conventions as
-- two_filters.sql (see its header) -- query=3, THRESHOLDS = {0.52, 0.50,
-- 0.48} matching the Flink/Spark/Kafka Streams ports' own array exactly.

CREATE SOURCE IF NOT EXISTS three_filters_source (
    k VARCHAR,
    val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    topic = 'ThreeFiltersIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE MATERIALIZED VIEW IF NOT EXISTS three_filters_agg_mv AS
SELECT
    k,
    window_start,
    AVG(val) AS val
FROM TUMBLE(three_filters_source, _rw_kafka_timestamp, INTERVAL '1 second')
GROUP BY k, window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS three_filters_filter1_mv AS
SELECT k, window_start, val
FROM three_filters_agg_mv
WHERE val < 0.52;

CREATE MATERIALIZED VIEW IF NOT EXISTS three_filters_filter2_mv AS
SELECT k, window_start, val
FROM three_filters_filter1_mv
WHERE val < 0.50;

CREATE MATERIALIZED VIEW IF NOT EXISTS three_filters_filter3_mv AS
SELECT k, window_start, val
FROM three_filters_filter2_mv
WHERE val < 0.48;

CREATE SINK IF NOT EXISTS three_filters_sink
FROM three_filters_filter3_mv
WITH (
    connector = 'kafka',
    topic = 'ThreeFiltersOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
