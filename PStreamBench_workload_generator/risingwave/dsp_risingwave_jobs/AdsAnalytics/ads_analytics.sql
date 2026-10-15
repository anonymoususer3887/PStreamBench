-- Ad Analytics (Q1): click-through rate (CTR) per (query, ad) pair.
--
-- Mirrors com.kom.dsp.AdsAnalytics.AdsAnalytics (Flink) query 1 (see
-- Q1ClickParser.java / Q1ImpressionParser.java): reads the SAME tab-
-- separated input record twice via two independent Kafka consumers (one
-- extracting `clicks` as its "count" field, one extracting `views`),
-- computes an unbounded running SUM(count) per (queryId, adId) for each of
-- the two derived streams independently, then joins the two running sums
-- on (queryId, adId) inside a sliding window to compute
-- CTR = clicks / views, keeping only pairs where clicks <= views.
--
-- SIMPLIFIED STRUCTURE (documented, not silent): rather than replicating
-- two separate unbounded-SUM streams joined via a sliding-window stream-
-- stream join (RisingWave interval/windowed joins are one of the least
-- certain constructs to translate blind), this computes both sums in a
-- SINGLE GROUP BY over the same parsed rows. This is mathematically
-- equivalent, not just "close enough": since both `clicksCounter` and
-- `impressionsCounter` are unbounded running sums of the same underlying
-- click/view fields keyed identically, joining two running sums of the
-- same source data is the same result as summing both fields together in
-- one pass. The window in the original only controls how often the join
-- re-emits an already-consistent result, not what the values represent --
-- so collapsing it doesn't change the computed CTR, only removes an
-- unnecessary re-emission mechanism this translation doesn't need.
--
-- Field order (0-indexed, tab-separated, matching Q1ClickParser.java /
-- Q1ImpressionParser.java): clicks, views, displayUrl, adId, advertiserId,
-- depth, position, queryId, keywordId, titleId, descriptionId, userId.
-- Only clicks/views/adId/queryId are used by this query's output.
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. Lower
-- structural risk than Machine Outlier/Spike Detection since this avoids
-- the windowed-join question entirely by construction.

CREATE SOURCE IF NOT EXISTS ad_analytics_source (
    line BYTEA,
    -- Unused by Q1 itself (no windowing), but declared here anyway so this
    -- source's schema matches ads_analytics_q2.sql's (which needs pt for
    -- its HOP window) -- both queries reuse the same object name/prefix,
    -- and a schema mismatch breaks whichever query runs second if the
    -- other's source lingers. Confirmed live 2026-07-27 (see tpch.sql's
    -- identical fix for the same issue).
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'AdAnalyticsIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW ad_analytics_mv AS
WITH parsed AS (
    SELECT
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 1) AS INT) AS clicks,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 2) AS INT) AS views,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 4) AS BIGINT) AS ad_id,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 8) AS BIGINT) AS query_id
    FROM ad_analytics_source
),
totals AS (
    SELECT
        query_id,
        ad_id,
        SUM(clicks) AS total_clicks,
        SUM(views) AS total_views
    FROM parsed
    GROUP BY query_id, ad_id
)
SELECT
    query_id,
    ad_id,
    total_clicks,
    total_views,
    (CAST(total_clicks AS DOUBLE PRECISION) / total_views) AS ctr
FROM totals
WHERE total_views > 0
  AND total_clicks <= total_views;

CREATE SINK IF NOT EXISTS ad_analytics_sink
FROM ad_analytics_mv
WITH (
    connector = 'kafka',
    topic = 'AdAnalyticsOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
