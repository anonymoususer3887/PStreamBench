-- Google Cloud Monitoring (Q2): average CPU usage per job, over a sliding
-- window.
--
-- Mirrors com.kom.dsp.GoogleCloudMonitoring.GoogleCloudMonitoring (Flink)
-- query 2 (see TaskEventParser.java / CPUPerJobCalculator.java). Same
-- input record and field parsing as Q1 (see google_cloud_monitoring.sql),
-- but CPUPerJobCalculator keys by jobId instead of category, and emits
-- (MIN(timestamp) in the window, jobId, AVG(cpu)) instead of a sum --
-- Java computes this as `sum / count`, which is exactly what SQL's AVG()
-- aggregate does.
--
-- Reuses googlecloud_monitoring_source/_mv/_sink object names (see
-- google_cloud_monitoring.sql's header comment for why the prefix is
-- "googlecloud_monitoring" rather than "google_cloud_monitoring") -- only
-- one query variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here.

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
        CAST(split_part(convert_from(line, 'UTF8'), ',', 1) AS BIGINT) AS job_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS BIGINT) AS event_timestamp,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 9) AS DOUBLE PRECISION) AS cpu
    FROM googlecloud_monitoring_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 19
)
SELECT
    job_id,
    MIN(event_timestamp) AS min_timestamp,
    AVG(cpu) AS avg_cpu
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY job_id, window_start, window_end;

CREATE SINK IF NOT EXISTS googlecloud_monitoring_sink
FROM googlecloud_monitoring_mv
WITH (
    connector = 'kafka',
    topic = 'GoogleOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
