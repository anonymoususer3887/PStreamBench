-- TPCH (Q2): count of high-priority orders per priority level, keyed by
-- discount, over a sliding window.
--
-- Mirrors com.kom.dsp.TPCH.TPCH (Flink) query 2 (see FilterCalculator.java):
-- same field parsing as Q1 (see tpch.sql), but instead of Q1's unbounded
-- running count, this keys by `discount` (a double -- an unusual grouping
-- key for a real query, more a Flink-API-showcasing artifact than a
-- meaningful business grouping, but replicated faithfully since that's
-- what the reference implementation actually does) over a sliding
-- processing-time window, re-applying the same orderPriority > 2 filter
-- inside the window.
--
-- Reuses t_p_c_h_source/_mv/_sink object names (not tpch_q2_*) -- only one
-- query variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling). See tpch.sql's
-- header comment for why the prefix is "t_p_c_h" rather than "tpch".
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here.

CREATE SOURCE IF NOT EXISTS t_p_c_h_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'TPIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW t_p_c_h_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS INT) AS order_priority,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 6) AS DOUBLE PRECISION) AS discount
    FROM t_p_c_h_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 6
)
SELECT
    discount,
    order_priority,
    COUNT(*) AS priority_count
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
WHERE order_priority > 2
GROUP BY discount, order_priority, window_start, window_end;

CREATE SINK IF NOT EXISTS t_p_c_h_sink
FROM t_p_c_h_mv
WITH (
    connector = 'kafka',
    topic = 'TPOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
