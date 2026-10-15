-- TPCH (Q1): count of high-priority orders per priority level -- a running
-- (unbounded, not windowed) aggregate, same shape as Word Count.
--
-- Mirrors com.kom.dsp.TPCH.TPCH (Flink) query 1 (see TPCHEventParser.java /
-- PriorityMapper.java): parses a comma-separated order record (only lines
-- with exactly 6 fields are processed -- orderKey=field 1, cname=field 2,
-- caddress=field 3, orderPriority=field 4 (int), extendedPrice=field 5,
-- discount=field 6 -- all 1-indexed; only orderPriority is actually used
-- by this query's output), filters to orderPriority > 2, keys by
-- orderPriority, and maintains a running COUNT per priority level --
-- `.sum(1)` on a keyed stream with NO window at all, i.e. an
-- ever-accumulating total since the job started, exactly like Word Count's
-- counter. This is one of the simplest translations here as a result --
-- no HOP/window_start/window_end needed anywhere.
--
-- Query 2 (discount-keyed, HOP-windowed) is implemented separately in
-- tpch_q2.sql, reusing these same object names.
--
-- Malformed input lines (not exactly 6 comma-separated fields) are
-- filtered out directly via a WHERE clause, same simplification as Traffic
-- Monitoring, rather than replicating TPCHEventParser.java's actual
-- behavior of emitting an all-default TPCHEvent for such lines.
--
-- OBJECT NAMING NOTE: the object prefix below is "t_p_c_h", not the more
-- intuitive "tpch" -- this follows directly from how runner.py derives it
-- (`constants.TPCH.replace(" ", "")` then underscore-before-each-capital-
-- letter, see runner.py::_run_risingwave_job). The constant's value is the
-- literal string "TPCH" (no spaces, all uppercase), so every one of its 4
-- letters is uppercase and gets its own leading underscore inserted:
-- "TPCH" -> "_T_P_C_H" -> lowercase -> "_t_p_c_h" -> strip leading "_" ->
-- "t_p_c_h". Matching this exactly is required for runner.py's
-- is_terminated()/terminate() to find the right rw_catalog objects.
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. Lower risk
-- than most: no windowing, no joins, just a filter + GROUP BY + COUNT,
-- structurally identical to the already-validated Word Count.

CREATE SOURCE IF NOT EXISTS t_p_c_h_source (
    line BYTEA,
    -- Unused by Q1 itself (no windowing), but declared here anyway so this
    -- source's schema is IDENTICAL to tpch_q2.sql's (which needs pt for its
    -- HOP window). Both queries reuse the same object name/prefix -- if the
    -- schemas differ and one file's source lingers (e.g. during manual
    -- testing without going through the controller's normal
    -- create-then-tear-down job lifecycle), the other query fails with
    -- "Invalid column: pt". Confirmed live 2026-07-27.
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
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS INT) AS order_priority
    FROM t_p_c_h_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 6
)
SELECT
    order_priority,
    COUNT(*) AS priority_count
FROM parsed
WHERE order_priority > 2
GROUP BY order_priority;

CREATE SINK IF NOT EXISTS t_p_c_h_sink
FROM t_p_c_h_mv
WITH (
    connector = 'kafka',
    topic = 'TPOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
