-- TwoWayJoin (synthetic shape 5 of 9): 2 sources -> pre-filter (threshold
-- 0.5, selectivity ~0.5) -> 1-hop window-bucket join -> windowed average
-- -> sink.
--
-- RisingWave counterpart of JoinTopology (dsp_flink_synthetic_jobs /
-- dsp_spark_synthetic_jobs / dsp_kafkastreams_synthetic_jobs), query=2.
-- N<=3 pre-filters each source (matching every other engine's JoinTopology
-- convention exactly: "N<=3 each source is pre-filtered before joining");
-- N>=4 omits it (see four_way_join.sql).
--
-- Join semantics: a window-BUCKET join (equi-join on key AND discrete
-- TUMBLE window_start), matching Flink's/Spark's window()-based join --
-- NOT RisingWave's interval-join primitive (which has no bucket-equality
-- analog and is closer to Kafka Streams' JoinWindows). This is a
-- deliberate, already-established cross-engine difference in this
-- project (Kafka Streams' JoinTopology.java javadoc documents the same
-- choice the other direction), not an oversight.
--
-- Each filter-N_mv does the TUMBLE (1-second bucket, matching the other
-- engines' cfg.windowSize=1000 default -- see linear_query.sql's header
-- for the full rationale/INTERVAL-syntax note) AND the threshold filter
-- together in one MV, so filter-0/filter-1 each stay their own
-- independently-templated-parallelism stage (matching every other
-- shape's one-operator-per-filter convention) while still exposing a
-- plain window_start column the join below can equi-join on directly --
-- a derived MV loses RisingWave's watermark/time-attribute, so a second
-- TUMBLE call on top of it is not an option; joining on the
-- already-bucketed window_start column is (validated locally 2026-10-17
-- against a live 3-source chain -- see two_way_join_mv below and this
-- directory's three/four/five/six_way_join.sql, which all chain the same
-- way).
--
-- Known, expected (not RisingWave-specific) cardinality note: since each
-- source emits MANY records per key within the same 1-second window, an
-- equi-join on (k, window_start) naturally fans out to every matching
-- pair within that bucket -- the same multiplicative cardinality-
-- explosion mechanism already extensively analyzed for Spark's 5-way/
-- 6-way joins in SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md section 11, observed
-- here too on a live 3-source chain during local validation. Expected to
-- be the dominant driver of output volume for the higher-N join shapes
-- here as well -- not re-derived per engine, the mechanism is identical
-- (window-bucket equi-join over a high-rate per-key stream), independent
-- of which engine executes it.

CREATE SOURCE IF NOT EXISTS two_way_join_source0 (
    k VARCHAR,
    val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka', topic = 'TwoWayJoinIn0',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE SOURCE IF NOT EXISTS two_way_join_source1 (
    k VARCHAR,
    val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka', topic = 'TwoWayJoinIn1',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__', scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

CREATE MATERIALIZED VIEW IF NOT EXISTS two_way_join_filter0_mv AS
SELECT k, val, window_start
FROM TUMBLE(two_way_join_source0, _rw_kafka_timestamp, INTERVAL '1 second')
WHERE val < 0.5;

CREATE MATERIALIZED VIEW IF NOT EXISTS two_way_join_filter1_mv AS
SELECT k, val, window_start
FROM TUMBLE(two_way_join_source1, _rw_kafka_timestamp, INTERVAL '1 second')
WHERE val < 0.5;

CREATE MATERIALIZED VIEW IF NOT EXISTS two_way_join_mv AS
SELECT a.k AS k, (a.val + b.val) / 2.0 AS val, a.window_start AS window_start
FROM two_way_join_filter0_mv a
JOIN two_way_join_filter1_mv b
ON a.k = b.k AND a.window_start = b.window_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS two_way_join_agg_mv AS
SELECT k, window_start, AVG(val) AS val
FROM two_way_join_mv
GROUP BY k, window_start;

CREATE SINK IF NOT EXISTS two_way_join_sink
FROM two_way_join_agg_mv
WITH (
    connector = 'kafka',
    topic = 'TwoWayJoinOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
