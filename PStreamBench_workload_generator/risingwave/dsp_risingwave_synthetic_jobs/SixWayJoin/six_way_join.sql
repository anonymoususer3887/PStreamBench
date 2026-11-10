-- SixWayJoin (synthetic shape 9 of 9): 6 sources (no pre-filter --
-- N>=4 omits it, matching every other engine's JoinTopology convention)
-- -> 5 chained window-bucket joins -> windowed average -> sink. Same
-- conventions as two_way_join.sql (see its header for the full
-- rationale) -- query=6.

CREATE SOURCE IF NOT EXISTS six_way_join_source0 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'SixWayJoinIn0',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS six_way_join_source1 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'SixWayJoinIn1',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS six_way_join_source2 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'SixWayJoinIn2',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS six_way_join_source3 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'SixWayJoinIn3',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS six_way_join_source4 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'SixWayJoinIn4',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS six_way_join_source5 (
    k VARCHAR, val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (connector = 'kafka', topic = 'SixWayJoinIn5',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_src0_mv AS
SELECT k, val, window_start FROM TUMBLE(six_way_join_source0, _rw_kafka_timestamp, INTERVAL '1 second');

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_src1_mv AS
SELECT k, val, window_start FROM TUMBLE(six_way_join_source1, _rw_kafka_timestamp, INTERVAL '1 second');

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_src2_mv AS
SELECT k, val, window_start FROM TUMBLE(six_way_join_source2, _rw_kafka_timestamp, INTERVAL '1 second');

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_src3_mv AS
SELECT k, val, window_start FROM TUMBLE(six_way_join_source3, _rw_kafka_timestamp, INTERVAL '1 second');

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_src4_mv AS
SELECT k, val, window_start FROM TUMBLE(six_way_join_source4, _rw_kafka_timestamp, INTERVAL '1 second');

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_src5_mv AS
SELECT k, val, window_start FROM TUMBLE(six_way_join_source5, _rw_kafka_timestamp, INTERVAL '1 second');

-- join-1: src0 x src1
CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_join1_mv AS
SELECT a.k AS k, (a.val + b.val) / 2.0 AS val, a.window_start AS window_start
FROM six_way_join_src0_mv a
JOIN six_way_join_src1_mv b
ON a.k = b.k AND a.window_start = b.window_start;

-- join-2: join1 x src2
CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_join2_mv AS
SELECT j.k AS k, (j.val + s.val) / 2.0 AS val, j.window_start AS window_start
FROM six_way_join_join1_mv j
JOIN six_way_join_src2_mv s
ON j.k = s.k AND j.window_start = s.window_start;

-- join-3: join2 x src3
CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_join3_mv AS
SELECT j.k AS k, (j.val + s.val) / 2.0 AS val, j.window_start AS window_start
FROM six_way_join_join2_mv j
JOIN six_way_join_src3_mv s
ON j.k = s.k AND j.window_start = s.window_start;

-- join-4: join3 x src4
CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_join4_mv AS
SELECT j.k AS k, (j.val + s.val) / 2.0 AS val, j.window_start AS window_start
FROM six_way_join_join3_mv j
JOIN six_way_join_src4_mv s
ON j.k = s.k AND j.window_start = s.window_start;

-- join-5: join4 x src5
CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_join5_mv AS
SELECT j.k AS k, (j.val + s.val) / 2.0 AS val, j.window_start AS window_start
FROM six_way_join_join4_mv j
JOIN six_way_join_src5_mv s
ON j.k = s.k AND j.window_start = s.window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS six_way_join_agg_mv AS
SELECT k, window_start, AVG(val) AS val
FROM six_way_join_join5_mv
GROUP BY k, window_start;

CREATE SINK IF NOT EXISTS six_way_join_sink
FROM six_way_join_agg_mv
WITH (connector = 'kafka', topic = 'SixWayJoinOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
