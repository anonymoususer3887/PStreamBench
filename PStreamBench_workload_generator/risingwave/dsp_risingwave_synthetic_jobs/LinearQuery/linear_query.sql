-- LinearQuery (synthetic shape 1 of 9): source -> windowed average -> sink.
--
-- RisingWave counterpart of SyntheticTopology/LinearQueryTopology in
-- dsp_flink_synthetic_jobs / dsp_spark_synthetic_jobs /
-- dsp_kafkastreams_synthetic_jobs -- same (key,value) data model, same
-- single-stage shape, no filter/join involved.
--
-- Data model (shared across all 5 engines' synthetic ports): each input
-- record is a "key,value" CSV string -- key = random int in
-- [0, keySpace), value = random double in [0,1) -- with the Kafka record
-- key set to the same key column. Unlike Flink (in-process
-- DataGeneratorSource) and Spark ("rate" format), RisingWave has no
-- in-process synthetic source -- like Kafka Streams, it reads from a real
-- Kafka topic, so this reuses dsp_kafkastreams_synthetic_jobs'
-- SyntheticProducer.java as the data generator for ALL synthetic
-- RisingWave shapes (just pointed at this shape's topic name), rather than
-- writing a second, redundant producer.
--
-- Window choice: tumbling 1-second window, matching the other 3 engines'
-- cfg.windowSize=1000 (ms) default for every synthetic shape (not swept --
-- only parallelism is swept across the uniform/random/ds2/dhalion sweeps).
-- RisingWave has no in-process "processing time" primitive the way
-- Flink's TumblingProcessingTimeWindows or Spark's window() do; this uses
-- the Kafka broker's own per-record produce timestamp
-- (_rw_kafka_timestamp, a RisingWave Kafka-source hidden column) as the
-- time attribute instead, with a 1-second-lag watermark. This is the
-- closest RisingWave-native equivalent to processing-time windowing (it
-- advances with wall-clock/broker time, not an embedded event-time field
-- the producer doesn't send) -- confirmed queryable live against this
-- project's local RisingWave v3.0.1 instance (word_count.sql's own header
-- already established _rw_kafka_timestamp as queryable; this is its first
-- use as a watermark source for TUMBLE rather than a plain SELECT column).
--
-- INTERVAL unit note: RisingWave's keyword-unit form
-- (INTERVAL '<n>' MILLISECOND) is documented elsewhere in this project
-- (smart_grid.sql) as failing to parse; the literal form with the unit
-- INSIDE the quoted string (INTERVAL '1 second') is the form confirmed
-- working, used here and in every other file in this directory.
--
-- __KAFKA_BOOTSTRAP_SERVER__ is substituted by the submission tooling the
-- same way word_count.sql's placeholder is (controller job templating, or
-- the local-docker submit script's build_sql_with_parallelism reuse) --
-- NOT optional cosmetics, see word_count.sql's own extensive comment on
-- why a hardcoded "localhost" breaks once compute runs on a different
-- physical node than the Kafka broker.
--
-- Validated locally end-to-end (2026-10-17, local Docker RisingWave
-- v3.0.1 + Kafka, single-node playground mode): real per-key windowed
-- averages observed, correct values, confirmed via a live feed from
-- SyntheticProducer.java into LinearQueryIn and reading LinearQueryOut
-- back via kafka-console-consumer.

CREATE SOURCE IF NOT EXISTS linear_query_source (
    k VARCHAR,
    val DOUBLE PRECISION,
    WATERMARK FOR _rw_kafka_timestamp AS _rw_kafka_timestamp - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    topic = 'LinearQueryIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (delimiter = ',', without_header = 'true');

-- avg_kafka_latency_seconds: per-window average of (processing time minus
-- each input row's own Kafka produce timestamp) -- unlike
-- actorMetrics.barrier_inflight_latency_avg (meta_barrier_duration_seconds,
-- a periodic control signal injected fresh at the source operator, AFTER
-- Kafka consumption -- see getActorMetrics()'s docstring), this is a real
-- per-row quantity rolled up per window, so it genuinely includes however
-- long each row sat in the Kafka topic before this source operator got
-- around to consuming it -- the dominant latency contributor under real
-- overload (growing consumer lag), which the barrier metric is structurally
-- blind to. now() in a standing RisingWave query re-evaluates as the query
-- advances (RisingWave's own documented pattern for streaming freshness
-- filters/watermark-relative conditions -- confirmed live, 2026-10-25, two
-- separate SELECT now() calls 2.6s apart returned different, advancing
-- timestamps), not a value frozen at MV-creation time.
CREATE MATERIALIZED VIEW IF NOT EXISTS linear_query_mv AS
SELECT
    k,
    window_start,
    AVG(val) AS val,
    AVG(EXTRACT(EPOCH FROM (now() - _rw_kafka_timestamp))) AS avg_kafka_latency_seconds
FROM TUMBLE(linear_query_source, _rw_kafka_timestamp, INTERVAL '1 second')
GROUP BY k, window_start;

CREATE SINK IF NOT EXISTS linear_query_sink
FROM linear_query_mv
WITH (
    connector = 'kafka',
    topic = 'LinearQueryOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
