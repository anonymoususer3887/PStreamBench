-- Log Processing (Q2): count log entries per (log-timestamp, status-code)
-- pair over a sliding window.
--
-- Mirrors com.kom.dsp.LogsAnalyzer.LogsAnalyzer (Flink) query 2 (see
-- StatusCounter.java) -- same parsing/window shape as Q1 (see
-- logs_analyzer.sql for the full field-order documentation), but also
-- extracts the HTTP status code (9th space-separated token, 1-indexed)
-- and groups by (log_time, status_code) instead of log_time alone.
--
-- Reuses log_processing_source/_mv/_sink object names (not
-- log_processing_q2_*) -- only one query variant runs per job submission
-- (see runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here.

CREATE SOURCE IF NOT EXISTS log_processing_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'LogIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW log_processing_mv AS
WITH parsed AS (
    SELECT
        pt,
        substring(split_part(convert_from(line, 'UTF8'), ' ', 4), 2) AS log_time,
        split_part(convert_from(line, 'UTF8'), ' ', 9) AS status_code
    FROM log_processing_source
)
SELECT
    log_time,
    status_code,
    COUNT(*) AS status_count
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY log_time, status_code, window_start, window_end;

CREATE SINK IF NOT EXISTS log_processing_sink
FROM log_processing_mv
WITH (
    connector = 'kafka',
    topic = 'LogOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
