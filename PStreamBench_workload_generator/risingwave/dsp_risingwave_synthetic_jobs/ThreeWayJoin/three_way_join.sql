-- ThreeWayJoin (synthetic shape 6 of 9): 3 sources -> pre-filter (0.5) ->
-- 2 chained window-bucket joins -> windowed average -> sink. Same
-- conventions as two_way_join.sql (see its header for the full
-- rationale) -- query=3, N<=3 so every source is pre-filtered.

CREATE SOURCE IF NOT EXISTS three_way_join_source0 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'ThreeWayJoinIn0',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS three_way_join_source1 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'ThreeWayJoinIn1',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS three_way_join_source2 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'ThreeWayJoinIn2',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE MATERIALIZED VIEW IF NOT EXISTS three_way_join_filter0_mv AS
SELECT k, val, window_start FROM TUMBLE(three_way_join_source0, _rw_kafka_timestamp, INTERVAL '1 second') WHERE val < 0.5;

CREATE MATERIALIZED VIEW IF NOT EXISTS three_way_join_filter1_mv AS
SELECT k, val, window_start FROM TUMBLE(three_way_join_source1, _rw_kafka_timestamp, INTERVAL '1 second') WHERE val < 0.5;

CREATE MATERIALIZED VIEW IF NOT EXISTS three_way_join_filter2_mv AS
SELECT k, val, window_start FROM TUMBLE(three_way_join_source2, _rw_kafka_timestamp, INTERVAL '1 second') WHERE val < 0.5;

-- join-1: filter0 x filter1
CREATE MATERIALIZED VIEW IF NOT EXISTS three_way_join1_mv AS
SELECT a.k AS k, (a.val + b.val) / 2.0 AS val, a.window_start AS window_start
FROM three_way_join_filter0_mv a
JOIN three_way_join_filter1_mv b
ON a.k = b.k AND a.window_start = b.window_start;

-- join-2: join1 x filter2
CREATE MATERIALIZED VIEW IF NOT EXISTS three_way_join2_mv AS
SELECT j.k AS k, (j.val + c.val) / 2.0 AS val, j.window_start AS window_start
FROM three_way_join1_mv j
JOIN three_way_join_filter2_mv c
ON j.k = c.k AND j.window_start = c.window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS three_way_join_agg_mv AS
SELECT k, window_start, AVG(val) AS val
FROM three_way_join2_mv
GROUP BY k, window_start;

CREATE SINK IF NOT EXISTS three_way_join_sink
FROM three_way_join_agg_mv
WITH (connector = 'kafka', topic = 'ThreeWayJoinOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
