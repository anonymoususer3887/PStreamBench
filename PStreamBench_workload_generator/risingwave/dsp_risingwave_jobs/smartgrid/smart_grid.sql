-- Smart Grid (Q1): per-house sliding-window median energy load.
--
-- Mirrors com.kom.dsp.smartgrid.SmartGridJob (Flink) query 1
-- (see HouseEventParser.java / ProcessWindowQ1.java): parses a
-- comma-separated house-event line (literal double-quotes stripped first,
-- matching HouseEventParser's `s.replace("\"", "")`), keys by house, and
-- computes the MEDIAN of the "value" field over a sliding processing-time
-- window per house. Field order (0-indexed, matching HouseEvent's
-- constructor): index, change, value, property, plugs, households, house.
-- Flink's own field name for the result is "gal" (Global Average Load)
-- despite actually computing a median, not a mean -- kept the same field
-- name here for continuity across engines, not because it's well-named.
--
-- LIVE-CONFIRMED FIXES (2026-07-24), applying to every windowed workload in
-- this project, not just this file:
--   - proctime() CANNOT be called inline in a SELECT/CTE at all (not even
--     on the raw source) -- confirmed error: "Function PROCTIME() is only
--     allowed in CREATE TABLE/SOURCE". Fixed by declaring it as a generated
--     column directly on CREATE SOURCE (`pt TIMESTAMP AS proctime()`, see
--     below) and referencing the `pt` column by name everywhere else.
--   - INTERVAL '<n>' MILLISECOND (keyword form) fails to parse specifically
--     inside HOP()'s argument list ("expected ), found: MILLISECOND"),
--     even though it parses (silently wrong -- computes as if SECOND) as a
--     bare SELECT expression. A computed interval like (n/1000)*INTERVAL
--     '1 second' also doesn't work -- HOP() requires a literal interval.
--     Fixed by having _risingwave_job_manipulation() convert the
--     millisecond window/slide values to whole seconds before substitution
--     and using the literal form INTERVAL '<seconds> second' (unit inside
--     the quoted string) -- confirmed working live, including as a HOP()
--     argument and with percentile_cont as the aggregate.
--   - percentile_cont(0.5) WITHIN GROUP (ORDER BY x) IS confirmed to work
--     as an incrementally-maintained streaming aggregate inside a
--     HOP-windowed CREATE MATERIALIZED VIEW (not just in a one-shot batch
--     query) -- verified live with this exact query shape.
--
-- NOT YET VALIDATED end-to-end with real data flowing through (the fixes
-- above were confirmed via a minimal reproduction, not this exact file) --
-- next step is a full live run.

CREATE SOURCE IF NOT EXISTS smart_grid_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'SmartGridIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW smart_grid_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(replace(convert_from(line, 'UTF8'), '"', ''), ',', 7) AS BIGINT) AS house,
        CAST(split_part(replace(convert_from(line, 'UTF8'), '"', ''), ',', 3) AS DOUBLE PRECISION) AS value
    FROM smart_grid_source
)
SELECT
    house,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY value) AS gal
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY house, window_start, window_end;

CREATE SINK IF NOT EXISTS smart_grid_sink
FROM smart_grid_mv
WITH (
    connector = 'kafka',
    topic = 'SmartGridOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
