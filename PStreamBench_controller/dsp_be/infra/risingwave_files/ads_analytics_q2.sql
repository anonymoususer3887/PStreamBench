-- Ad Analytics (Q2): windowed click-through rate (CTR) per (query, ad) pair,
-- from a differently-ordered input record than Q1.
--
-- Mirrors com.kom.dsp.AdsAnalytics.AdsAnalytics (Flink) query 2 (see
-- Q2AdEventParser.java / Q2AdEventCounter.java / Q2RollingAverage.java).
-- Q2AdEventParser.java splits on `"\\s+"` (Java regex for "one or more
-- whitespace characters," which matches tabs too, not literal spaces
-- only), with a DIFFERENT field order from Q1: clicks(0), views(1),
-- displayUrl(2), adId(3), queryId(4), depth(5), position(6),
-- advertiserId(7), keywordId(8), titleId(9), descriptionId(10), userId(11)
-- -- note queryId/advertiserId swap positions (4/7) relative to Q1's
-- layout (7/4).
--
-- WIRE FORMAT NOTE: the shared producer (custom_producer.py's
-- read_data_file(), AD_ANALYTICS branch) always serializes with
-- `to_csv(sep='\t')` -- i.e. the real data on the wire is TAB-separated,
-- regardless of query number (there is no Q1/Q2-specific producer mode).
-- Q2AdEventParser.java's `\s+` still parses this correctly since it
-- matches any whitespace including tabs, but RisingWave's split_part()/
-- string_to_array() take a LITERAL delimiter string, not a regex -- so
-- this file splits on an actual tab character (E'\t'), not a literal
-- space, to match the real wire format. Confirmed by cross-checking
-- custom_producer.py before running this live (a literal-space split
-- against tab-separated data would have silently produced zero matching
-- rows -- every row's array_length would be 1, failing the row filter --
-- rather than an error, so this would have looked exactly like the kind
-- of "no data reached the sink" null-metrics failure this test pass is
-- meant to catch).
--
-- TWO DELIBERATE, DOCUMENTED DIVERGENCES from the literal reference:
--
-- 1. Q2AdEventCounter.java maintains running click/impression counts per
--    (queryId, adId) in a HashMap, but contains a genuine bug: when
--    updating the impression counter it reads from `clickCounts` instead
--    of `impressionCounts`:
--      impressionCounts.put(key, clickCounts.getOrDefault(key, 0) + views)
--    Reproducing this exactly in declarative SQL would require modeling
--    mutable, order-dependent HashMap iteration, which SQL's set-based
--    execution model cannot faithfully express. This translation
--    implements the INTENDED behavior (independent running sums of clicks
--    and views), not the buggy one.
--
-- 2. Q2RollingAverage.java computes a sliding-window AVERAGE of per-event
--    CTR ratios (each event's own clicks/views, averaged over the window),
--    where each event's ratio is itself computed against an ever-growing,
--    all-time running total -- making the resulting metric drift over the
--    life of the job rather than settle into a stable rate. This
--    translation instead computes a windowed RATIO OF SUMS
--    (SUM(clicks)/SUM(views) per window), which is the conventional,
--    stable definition of CTR and avoids depending on unbounded
--    all-time state inside a bounded window aggregate.
--
-- Reuses ad_analytics_source/_mv/_sink object names -- only one query
-- variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here.

CREATE SOURCE IF NOT EXISTS ad_analytics_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'AdAnalyticsIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW ad_analytics_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 1) AS INT) AS clicks,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 2) AS INT) AS views,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 4) AS BIGINT) AS ad_id,
        CAST(split_part(convert_from(line, 'UTF8'), E'\t', 5) AS BIGINT) AS query_id
    FROM ad_analytics_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), E'\t'), 1) = 12
)
SELECT
    query_id,
    ad_id,
    SUM(clicks) AS total_clicks,
    SUM(views) AS total_views,
    (CAST(SUM(clicks) AS DOUBLE PRECISION) / SUM(views)) AS ctr
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
WHERE views > 0
GROUP BY query_id, ad_id, window_start, window_end
HAVING SUM(views) > 0;

CREATE SINK IF NOT EXISTS ad_analytics_sink
FROM ad_analytics_mv
WITH (
    connector = 'kafka',
    topic = 'AdAnalyticsOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
