-- Bargain Index (Q1): per-symbol VWAP + bargain-index filter over a
-- sliding window.
--
-- Mirrors com.kom.dsp.BargainIndex.BargainIndex (Flink): parses a
-- comma-separated stock-quote line (symbol, date, open, high, low, close,
-- adjClose, volume -- see QuoteDataParser.java), keys by symbol, and over a
-- sliding processing-time window computes:
--   vwap        = SUM(close * volume) / SUM(volume)   (VWAPCalculator.java)
--   quote_price = the close price of the LAST reading in the window, by
--                 arrival order (VWAPCalculator.java's loop just keeps
--                 overwriting `quotePrice` with each record's close as it
--                 iterates the window's Iterable, so the final value is
--                 whichever reading the runtime iterated last)
--   bargain_index = quote_price / vwap, but only if quote_price > vwap,
--                 else 0 (BargainIndexCalculator.java) -- and only emitted
--                 if bargain_index > threshold.
-- Only symbol/close/volume are used by the actual computation --
-- open/high/low/adjClose/date are parsed by QuoteDataParser.java but never
-- read downstream (BargainIndex.java's pipeline only touches
-- QuoteData::getClose/getVolume/getSymbol), so this translation only
-- extracts those three fields; no need to replicate QuoteDataParser's own
-- unrelated bugs (e.g. it parses `high` from the same field index as
-- `open`) since `high` never reaches this workload's actual output.
--
-- 5 is substituted the same way ms1121:9092 is,
-- from job_threshold -- unlike Machine Outlier's hardcoded-and-ignored
-- threshold quirk, BargainIndexCalculator.java's constructor genuinely
-- does use the CLI-configured popularityThreshold, so this one is a real
-- parameter, not a bug to preserve.
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as the other Phase 1 workloads. Specific
-- open questions for this file:
--   - "last close by arrival order" is approximated as
--     (ARRAY_AGG(close ORDER BY pt DESC))[1] -- ordered ARRAY_AGG as a
--     streaming aggregate is not independently confirmed live (same
--     category of risk as Smart Grid's percentile_cont: an exact-order
--     aggregate, not a simple incrementally-mergeable one like SUM/COUNT).
--   - HOP argument order (as in every other windowed workload here).

CREATE SOURCE IF NOT EXISTS bargain_index_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'BIIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW bargain_index_mv AS
WITH parsed AS (
    SELECT
        pt,
        split_part(convert_from(line, 'UTF8'), ',', 1) AS symbol,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 6) AS DOUBLE PRECISION) AS close,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS BIGINT) AS volume
    FROM bargain_index_source
),
windowed AS (
    SELECT symbol, pt, close, volume, window_start, window_end
    FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
),
vwap AS (
    SELECT
        symbol,
        window_start,
        window_end,
        SUM(close * volume) / SUM(volume) AS vwap,
        SUM(volume) AS total_volume,
        (ARRAY_AGG(close ORDER BY pt DESC))[1] AS quote_price
    FROM windowed
    GROUP BY symbol, window_start, window_end
)
SELECT
    symbol,
    (quote_price / vwap) AS bargain_index,
    total_volume
FROM vwap
WHERE quote_price > vwap
  AND (quote_price / vwap) > 5;

CREATE SINK IF NOT EXISTS bargain_index_sink
FROM bargain_index_mv
WITH (
    connector = 'kafka',
    topic = 'BIOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
