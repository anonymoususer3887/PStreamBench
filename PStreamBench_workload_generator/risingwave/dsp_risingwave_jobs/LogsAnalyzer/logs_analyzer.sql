-- Log Processing (Q1): count log entries per log-timestamp over a sliding
-- window ("volume counter").
--
-- Mirrors com.kom.dsp.LogsAnalyzer.LogsAnalyzer (Flink) query 1 (see
-- LogParser.java / VolumeCounter.java): parses a raw access-log line by
-- splitting on literal single spaces, extracting the log timestamp
-- (4th space-separated token, minus its leading "[") and, for query 2 only
-- (not implemented here yet -- see below), the HTTP status code (9th
-- token). Keys by log_time and counts occurrences per key over a sliding
-- processing-time window -- structurally the same
-- split-and-GROUP-BY-and-COUNT shape as Word Count, just keyed by an
-- extracted substring instead of a whitespace-split word.
--
-- Only Query 1 (VolumeCounter: count per log_time) is translated so far,
-- matching the phased single-query-first approach used for every other
-- workload here. Query 2 (StatusCounter: count per (log_time, status_code)
-- pair, see StatusCounter.java) is not yet implemented -- a single .sql
-- file can't branch on a runtime "-query" argument the way one Java class
-- can, so a second query variant would need its own .sql file (e.g.
-- logs_analyzer_q2.sql) and its own constants_risingwave.py PROGRAM entry
-- when it's built.
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as smart_grid.sql. Same open questions apply
-- (HOP argument order, whether a plain COUNT(*) -- unlike percentile_cont --
-- should be uncontroversial as a streaming aggregate, so this one is lower
-- risk than Smart Grid's median).
--
-- Sample log line this expects (space-separated, standard combined access
-- log format):
--   54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /path HTTP/1.1" 200 30577 "-" "Mozilla/5.0" "-"
-- Token 4 (1-indexed) = "[22/Jan/2019:03:56:14" -> strip leading "[" -> log_time.
-- Token 9 (1-indexed) = "200" -> status_code (query 2 only).

CREATE SOURCE IF NOT EXISTS log_processing_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'LogIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW log_processing_mv AS
WITH parsed AS (
    SELECT
        pt,
        substring(split_part(convert_from(line, 'UTF8'), ' ', 4), 2) AS log_time
    FROM log_processing_source
)
SELECT
    log_time,
    COUNT(*) AS visit_count
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY log_time, window_start, window_end;

CREATE SINK IF NOT EXISTS log_processing_sink
FROM log_processing_mv
WITH (
    connector = 'kafka',
    topic = 'LogOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
