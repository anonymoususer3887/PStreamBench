-- Smart Grid (Q2): local average load per (house, households, plugs) key
-- over a sliding window.
--
-- Mirrors com.kom.dsp.smartgrid.SmartGridJob (Flink) query 2 (see
-- ProcessWindowQ2.java) -- same parsing/window shape as Q1 (see
-- smart_grid.sql for the full field-order/quirk documentation), but keyed
-- by the composite (house, households, plugs) tuple instead of house
-- alone, and computing a plain AVG(value) ("lal" = Local Average Load)
-- instead of Q1's percentile_cont-based median ("gal"). This is actually a
-- SIMPLER, lower-risk translation than Q1 since AVG is one of the safest
-- streaming aggregates here (same category as Spike Detection/Bargain
-- Index's SUM-based aggregates), unlike Q1's percentile_cont.
--
-- Reuses smart_grid_source/_mv/_sink object names (not smart_grid_q2_*) --
-- only one query variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling), so there's no
-- need for this file's objects to coexist with Q1's.
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here.

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
        CAST(split_part(replace(convert_from(line, 'UTF8'), '"', ''), ',', 6) AS BIGINT) AS households,
        CAST(split_part(replace(convert_from(line, 'UTF8'), '"', ''), ',', 5) AS BIGINT) AS plugs,
        CAST(split_part(replace(convert_from(line, 'UTF8'), '"', ''), ',', 3) AS DOUBLE PRECISION) AS value
    FROM smart_grid_source
)
SELECT
    house,
    households,
    plugs,
    AVG(value) AS lal
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY house, households, plugs, window_start, window_end;

CREATE SINK IF NOT EXISTS smart_grid_sink
FROM smart_grid_mv
WITH (
    connector = 'kafka',
    topic = 'SmartGridOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
