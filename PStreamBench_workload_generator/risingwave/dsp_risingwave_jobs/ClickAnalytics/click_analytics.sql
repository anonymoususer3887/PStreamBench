-- Click Analytics (Q1): per-URL total + unique visit counts over a sliding
-- window.
--
-- Mirrors com.kom.dsp.ClickAnalytics.ClickAnalytics (Flink) query 1 (see
-- ClickLogParser.java / RepeatVisitOperator.java / SumReducer.java).
-- Unlike every other workload translated so far, this one's input topic
-- carries genuine JSON (parsed via Gson in ClickLogParser.java: {"url":
-- ..., "ip": ..., "clientKey": ...}), so the source below uses
-- FORMAT PLAIN ENCODE JSON directly instead of the raw-bytes-plus-manual-
-- split_part parsing every CSV-based workload here needs.
--
-- DELIBERATE SIMPLIFICATION from the reference semantics (documented, not
-- silent): RepeatVisitOperator.java's "unique visit" tracking uses a
-- HashMap<(url,clientKey), Boolean> initialized ONCE in open() -- i.e. it
-- persists for the operator's entire lifetime, across every window, not
-- reset per window. So Flink's "unique visit" really means "have we EVER
-- seen this (url, clientKey) pair before, since this job started", not
-- "unique within this window". Replicating that exact semantic in SQL
-- would need something like a global first-seen dedup (e.g.
-- ROW_NUMBER() OVER (PARTITION BY url, client_key ORDER BY pt) = 1),
-- and streaming analytic OVER() window functions are not confirmed
-- supported the way plain aggregates are. Implemented here instead as
-- COUNT(DISTINCT client_key) per (url, window) -- "unique visitors within
-- this window" -- a standard, well-supported streaming aggregate with the
-- same practical intent (distinguishing repeat visits from unique
-- visitors) but a different exact definition of "unique" (per-window vs.
-- all-time). Flag this explicitly if byte-exact comparability with Flink's
-- output is ever needed.
--
-- NOT IMPLEMENTED: Query 2 (Geography -- GeographyOperator.java resolves
-- visitor IP to city/country via an external MaxMind GeoLite2 database
-- download + lookup library; there's no SQL equivalent short of writing a
-- custom UDF that embeds a GeoIP database, which is a substantially larger
-- undertaking than a query translation) and Query 3 (uses Flink's
-- countWindow(), a count-based window with no RisingWave equivalent --
-- RisingWave's windowing is strictly time-based HOP/TUMBLE).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. Additional
-- open question specific to this file: it's the first workload here using
-- FORMAT PLAIN ENCODE JSON directly on the source (every CSV-based
-- workload instead reads ENCODE BYTES and manually splits the line) --
-- this needs its own live check that RisingWave maps the JSON keys to the
-- declared columns as expected, including the quoted "clientKey" column
-- (camelCase, needs double-quoting since SQL identifiers are lowercased by
-- default).

CREATE SOURCE IF NOT EXISTS click_analytics_source (
    url VARCHAR,
    ip VARCHAR,
    "clientKey" VARCHAR,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'CAIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW click_analytics_mv AS
WITH parsed AS (
    SELECT pt, url, "clientKey" AS client_key
    FROM click_analytics_source
)
SELECT
    url,
    COUNT(*) AS total_visits,
    COUNT(DISTINCT client_key) AS unique_visits
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY url, window_start, window_end;

CREATE SINK IF NOT EXISTS click_analytics_sink
FROM click_analytics_mv
WITH (
    connector = 'kafka',
    topic = 'CAOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
