-- Word Count (Q1): stateless split + GROUP BY/COUNT.
--
-- Mirrors com.kom.dsp.wordcount.WordCountJob (Flink) / WordCountTopology
-- (Kafka Streams): source reads raw text lines from Kafka topic
-- WordCountIn, splits each line on whitespace, keys by word, maintains a
-- running count, sinks (word, count) pairs to WordCountOut.
--
-- The input topic carries raw UTF-8 text lines with no schema (produced by
-- custom_producer.py's wordcount() mode), so the source is declared with
-- ENCODE BYTES and decoded to text in the first stage, unlike the other 13
-- workloads' structured JSON event payloads.
--
-- Validated end-to-end against a live RisingWave v3.0.1 instance
-- (2026-07-21, single-node `playground` mode on CloudLab). Two things had
-- to change from the first draft:
--   - RisingWave has no regexp_split_to_table; Postgres's LATERAL-table-
--     function-call syntax isn't accepted either ("expected subquery after
--     LATERAL"). Fix: call a set-returning function directly as a FROM item
--     (no LATERAL keyword needed -- it's implicit) via
--     unnest(string_to_array(...)). Confirmed working inside a streaming
--     CREATE MATERIALIZED VIEW, not just batch queries.
--   - com.kom.dsp.wordcount.Splitter (the class WordCountJob.java actually
--     uses) splits on a literal single space and does NOT lowercase --
--     matched that exactly here (string_to_array(text, ' '), no lower())
--     rather than the lowercased/regex-whitespace version in this file's
--     first draft, to keep output comparable with the Flink/Kafka Streams
--     word-count semantics.
--   - __KAFKA_BOOTSTRAP_SERVER__ gets substituted by the controller's
--     per-run job templating (_risingwave_config_manipulation() /
--     runner.py::_run_risingwave_job) with the master node's real
--     hostname, the same way sample-spark-defaults.conf's __MASTER_HOST__
--     placeholder is substituted. This is NOT optional cosmetics: in a
--     genuine multi-node deployment (meta+frontend on master, compute on
--     worker nodes, per Part B.1), a hardcoded "localhost:9092" breaks --
--     confirmed live (2026-07-21) that the compute node's actor (running on
--     a different physical machine than the Kafka broker) gets
--     "AllBrokersDown" / connection-refused errors trying to reach
--     "localhost:9092" from its own machine, since that's not where Kafka
--     lives.
--   - word_count_mv is a retract stream (COUNT's running total changes as
--     new events arrive), and a plain Kafka sink is append-only -- the sink
--     needs force_append_only='true', which drops DELETE/retraction
--     messages and turns each UPDATE into a fresh INSERT. This actually
--     matches Flink/Kafka Streams' own behavior here: their sum()/count()
--     operators likewise emit a new output tuple per input event rather
--     than a compacted final state, so WordCountOut stays comparable across
--     engines.
--
-- End-to-end verified manually: 3 lines produced into WordCountIn ("the
-- quick brown fox" / "the lazy dog barks" / "the fox runs fast") yielded
-- exactly {"the":3, "fox":2, everything else:1} read back from
-- WordCountOut -- correct.
--
-- Also re-verified against the *actual* wire format custom_producer.py's
-- wordcount() mode sends: each event is `json.dumps(event)`, not a raw
-- line, so the Kafka message bytes look like `" the quick brown fox
-- jumps"` -- JSON-quoted, with a leading space baked into the event string
-- itself. Splitting that on literal " " (matching Splitter.java exactly)
-- produces two artifacts: the last real word gets a trailing `"` stuck to
-- it (e.g. "jumps\""), and a lone `"` character shows up as its own
-- pseudo-word. Confirmed this is not a RisingWave-specific bug -- Flink's
-- SimpleStringSchema + Splitter.java would decode the identical bytes and
-- produce the identical artifacts, so it's a pre-existing quirk of this
-- benchmark's Word Count data pipeline, faithfully reproduced here for
-- comparability rather than "fixed."
--
-- Tier 1/2/3 metrics confirmed working via getprom_risingwave.py (Tier 1
-- node_exporter, Tier 2 stream_barrier_inflight_duration_seconds, Tier 3
-- per-fragment stream_actor_in/out_record_cnt / stream_source_output_rows_
-- counts / stream_sink_input_row_count, resolved to pipeline stage names via
-- a live rw_catalog.rw_fragments lookup since fragment ids are opaque and
-- query-plan-dependent). True per-event end-to-end latency (via the
-- _rw_kafka_timestamp hidden source column, confirmed queryable) doesn't
-- carry through this workload's COUNT aggregation -- only relevant for a
-- future non-aggregating workload.
--
-- Also validated against a genuine 2-node distributed deployment (meta+
-- frontend on one node, compute-node on a separate physical node, joined via
-- --meta-address, both confirmed RUNNING in rw_catalog.rw_worker_nodes) --
-- not just single-node `playground` mode. This is what surfaced the
-- __KAFKA_BOOTSTRAP_SERVER__ localhost bug above, plus a second one worth
-- recording: Kafka's own advertised.listeners must also be set to the
-- broker's real hostname, not localhost -- otherwise Kafka's metadata
-- response tells every consumer (including ones on other machines) to
-- reconnect to "localhost", which fails from any node that isn't the broker
-- itself. Both fixes are required together; fixing only the RisingWave-side
-- placeholder still failed until advertised.listeners was corrected too.
--
-- A third, more fundamental issue surfaced testing a genuine 5-node cluster
-- (1 master + 4 compute workers, 2026-07-22): the meta/compute Hummock
-- state store MUST be a shared, network-reachable object store (MinIO),
-- not a local-filesystem path (hummock+fs://.../rw-data/hummock). Confirmed
-- failure: source+MV created fine, but CREATE SINK failed with
-- "database 1 report failure from N", and the actual compute node log
-- showed "risingwave_object_store::object: read failed error=NotFound ...
-- entity not found" -- an actor on one worker tried to read a Hummock SST
-- file that had only ever been written to a *different* worker's own local
-- disk, since hummock+fs is per-node local storage, not shared/replicated.
-- This is invisible in single-node `playground` mode (everything shares one
-- filesystem) and only shows up once compute actually runs across multiple
-- physical machines. Fixed by adding MinIO on the master node (see
-- risingwave-minio-start.yaml) and switching --state-store to
-- hummock+minio://... -- matches RisingWave's own docker-compose-
-- distributed.yml reference architecture, which uses MinIO for the same
-- reason. Confirmed working end-to-end (correct word counts) on the real
-- 5-node cluster after this fix.

CREATE SOURCE IF NOT EXISTS word_count_source (
    line BYTEA
) WITH (
    connector = 'kafka',
    topic = 'WordCountIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW word_count_mv AS
SELECT
    word,
    COUNT(*) AS cnt
FROM word_count_source,
     unnest(string_to_array(convert_from(line, 'UTF8'), ' ')) AS word
GROUP BY word;

CREATE SINK IF NOT EXISTS word_count_sink
FROM word_count_mv
WITH (
    connector = 'kafka',
    topic = 'WordCountOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
