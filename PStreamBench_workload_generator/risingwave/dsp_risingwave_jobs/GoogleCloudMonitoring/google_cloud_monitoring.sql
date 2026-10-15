-- Google Cloud Monitoring (Q1): total CPU usage per task category, over a
-- sliding window.
--
-- Mirrors com.kom.dsp.GoogleCloudMonitoring.GoogleCloudMonitoring (Flink)
-- query 1 (see TaskEventParser.java / CPUPerCategoryCalculator.java).
-- Input is a comma-separated Google cluster-trace task_events-style
-- record; TaskEventParser.java pulls (0-indexed): jobId(0), taskId(1),
-- timestamp(2), category(3), machineId(4), priority(7), cpu(8), ram(9),
-- disk(12), userId(15), constraints(17), eventType(18) -- only
-- timestamp/category/cpu are used by this query. CPUPerCategoryCalculator
-- keys by category, windows with a sliding PROCESSING-time window, and
-- emits (MIN(timestamp) in the window, category, SUM(cpu)).
--
-- OBJECT NAMING NOTE: the object prefix below is "googlecloud_monitoring",
-- not the more intuitive "google_cloud_monitoring" -- this follows
-- directly from how runner.py derives it
-- (`constants.GOOGLE_CLOUD_MONITORING.replace(" ", "")` then
-- underscore-before-each-capital-letter). The constant's value is
-- "Google cloud Monitoring" (lowercase "cloud", consistent across every
-- other engine's constants_*.py in this project, not a typo unique to
-- RisingWave), so after space-stripping ("GooglecloudMonitoring") only the
-- "G" and "M" are uppercase, producing "googlecloud_monitoring" -- matching
-- this exactly is required for runner.py's is_terminated()/terminate() to
-- find the right rw_catalog objects.
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. Structurally
-- one of the lower-risk workloads (HOP window + SUM/MIN, both standard
-- streaming aggregates, same shape as Smart Grid Q2/Log Processing).

CREATE SOURCE IF NOT EXISTS googlecloud_monitoring_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'GoogleIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW googlecloud_monitoring_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS BIGINT) AS event_timestamp,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS INT) AS category,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 9) AS DOUBLE PRECISION) AS cpu
    FROM googlecloud_monitoring_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 19
)
SELECT
    category,
    MIN(event_timestamp) AS min_timestamp,
    SUM(cpu) AS total_cpu
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY category, window_start, window_end;

CREATE SINK IF NOT EXISTS googlecloud_monitoring_sink
FROM googlecloud_monitoring_mv
WITH (
    connector = 'kafka',
    topic = 'GoogleOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
