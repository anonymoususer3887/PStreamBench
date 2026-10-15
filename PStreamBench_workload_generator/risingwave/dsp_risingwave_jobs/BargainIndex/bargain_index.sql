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
-- __THRESHOLD__ is substituted the same way __KAFKA_BOOTSTRAP_SERVER__ is,
-- from job_threshold -- unlike Machine Outlier's hardcoded-and-ignored
-- threshold quirk, BargainIndexCalculator.java's constructor genuinely
-- does use the CLI-configured popularityThreshold, so this one is a real
-- parameter, not a bug to preserve.
--
-- LIVE-CONFIRMED BUG (2026-07-26, found via local testing): the CTE
-- computing VWAP was originally named `vwap` AND contained a column also
-- named `vwap` (`SUM(close * volume) / SUM(volume) AS vwap`). Referencing
-- that column from the outer query (`quote_price / vwap`) silently
-- produced ZERO rows -- no error, just an empty materialized view --
-- despite the identical logic working fine once the CTE was renamed to
-- `vwap_calc` (a plain name/column collision, not a bug in the math
-- itself). Confirmed by bisecting: HOP() alone produced rows, the GROUP BY
-- aggregation alone produced rows, but the outer SELECT referencing the
-- same-named CTE produced nothing. Renamed the CTE (not the column) to
-- avoid this -- worth checking for the same pattern (a CTE and one of its
-- own columns sharing a name) in any future workload.
--
-- LIVE-CONFIRMED (2026-07-26): with that fix, this file produces real,
-- correct output locally (RisingWave 2.8.0, single-node) -- ordered
-- ARRAY_AGG as a streaming aggregate works fine, same as HOP + GROUP BY +
-- division. Not yet re-validated against the actual 5-node distributed
-- CloudLab cluster with this specific fix.

CREATE SOURCE IF NOT EXISTS bargain_index_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'BIIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
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
vwap_calc AS (
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
FROM vwap_calc
WHERE quote_price > vwap
  AND (quote_price / vwap) > __THRESHOLD__;

CREATE SINK IF NOT EXISTS bargain_index_sink
FROM bargain_index_mv
WITH (
    connector = 'kafka',
    topic = 'BIOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
