-- Click Analytics (Q3): total + unique visit counts per URL, batched by a
-- COUNT-based window instead of Q1's time-based one.
--
-- Mirrors com.kom.dsp.ClickAnalytics.ClickAnalytics (Flink) query 3 (see
-- RepeatVisitCountOperator.java): identical logic to Q1's
-- RepeatVisitOperator, but Flink's `.countWindow(size, slide)` batches by
-- NUMBER OF EVENTS per key rather than by time.
--
-- DELIBERATE SIMPLIFICATION (documented, not silent): RisingWave's
-- windowing is strictly time-based (HOP/TUMBLE) -- there is no
-- count-based window primitive to translate against directly. Approximated
-- here via ROW_NUMBER() OVER (PARTITION BY client_key ORDER BY pt),
-- bucketed into non-overlapping groups of 10000 rows per
-- client_key (integer-divided row number) -- this is a TUMBLING
-- approximation of Flink's sliding count-window (Flink's countWindow(size,
-- slide) can overlap when slide < size; this cannot), and reuses the
-- window-size/slide placeholders as plain row counts, not milliseconds
-- (Flink's own ClickAnalytics.java does the same double-duty with its
-- "size"/"slide" CLI params, reusing them as either ms or event-counts
-- depending on which query is selected).
--
-- Reuses click_analytics_source/_mv/_sink object names -- only one query
-- variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass. Specific open question beyond the usual list:
-- whether ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) is supported
-- as a streaming analytic function at all in a materialized view -- this
-- is the single least-certain piece of syntax used anywhere in this
-- project's RisingWave translations so far, more uncertain than
-- percentile_cont or ordered ARRAY_AGG (both of which are at least
-- standard aggregate functions, just with uncertain streaming-incremental
-- support -- ROW_NUMBER() OVER is a genuinely different kind of construct,
-- an analytic/window function rather than an aggregate, and streaming SQL
-- engines generally support these more narrowly than batch engines do).

CREATE SOURCE IF NOT EXISTS click_analytics_source (
    url VARCHAR,
    ip VARCHAR,
    "clientKey" VARCHAR,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'CAIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW click_analytics_mv AS
WITH parsed AS (
    SELECT pt, url, "clientKey" AS client_key
    FROM click_analytics_source
),
numbered AS (
    SELECT
        url,
        client_key,
        (ROW_NUMBER() OVER (PARTITION BY client_key ORDER BY pt) - 1) / 10000 AS bucket
    FROM parsed
)
SELECT
    url,
    bucket,
    COUNT(*) AS total_visits,
    COUNT(DISTINCT client_key) AS unique_visits
FROM numbered
GROUP BY url, bucket;

CREATE SINK IF NOT EXISTS click_analytics_sink
FROM click_analytics_mv
WITH (
    connector = 'kafka',
    topic = 'CAOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
