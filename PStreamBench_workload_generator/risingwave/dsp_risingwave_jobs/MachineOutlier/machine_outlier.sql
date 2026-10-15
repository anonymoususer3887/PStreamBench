-- Machine Outlier (Q1): per-machine, per-window outlier detection by
-- distance from a reference profile.
--
-- The REAL Flink implementation (MachineOutlier.java -> keyBy(machineId) ->
-- SlidingProcessingTimeWindows -> MachineOutlierWindowFunction) is NOT a
-- simple threshold filter, despite that being the original assumption in
-- KAFKA_STREAMS_RISINGWAVE_PLAN.md's Phase 1 list. It runs a custom BFPRT
-- (median-of-medians) selection algorithm to pick the ~75th-percentile-by-
-- arrival-order reading (capped at the first 4 readings per machine per
-- window -- see MachineOutlierWindowFunction.java's `count==4 break`, which
-- reads as an implementation artifact/limitation rather than a deliberate
-- design choice), then flags any reading in that same small sample whose
-- Euclidean distance (across cpuUtil/memUtil/memBW/mkpi/netIn/netOut/
-- diskIO) from that reference exceeds a threshold.
--
-- This SQL is a deliberate APPROXIMATION, not a faithful port (confirmed
-- with the project owner, 2026-07-22, given BFPRT-based selection has no
-- direct SQL equivalent short of a custom UDF):
--   - Reference profile = percentile_cont(0.75) of each dimension, computed
--     per (machine_id, window) over ALL readings in the window -- same
--     "~75th percentile" intent as the original's kth-element selection,
--     but a true aggregate over the full window rather than a
--     median-of-medians pick from (at most) 4 readings. This is a more
--     statistically stable reference, at the cost of not being a literal
--     port of the original algorithm.
--   - Every reading in the window is compared against this reference
--     (not just the first 4 per machine).
--
-- Faithfully preserved quirk: MachineOutlierWindowFunction.java reads a
-- CLI-configurable `popularityThreshold` in MachineOutlier.java's main()
-- but never actually passes it in -- the window function hardcodes
-- `double threshold = 0.5` regardless of what's configured. Replicated
-- here as a literal 0.5 (not a __THRESHOLD__ placeholder) to match the
-- reference engine's *actual* behavior rather than its apparent intent,
-- consistent with how this project treats other engines' quirks (e.g.
-- Word Count's stray-quote artifact) -- faithfully reproduced, not
-- silently "fixed", since the benchmark compares real behavior.
--
-- Field order (0-indexed, matching MachineUsageParser.java): machine_id,
-- timestamp, cpuUtilPercentage, memUtilPercentage, memBW,
-- missPerThousandInstructions, netIn, netOut, diskIO. memBW/mkpi default to
-- 0 if empty in the source data (matching the parser's null-handling),
-- checked independently per field here rather than only when *both* are
-- empty together (a minor simplification from the original's exact
-- condition).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as Smart Grid/Log Processing. Additional open
-- question specific to this file: whether a windowed self-join (raw HOP
-- window joined to its own per-window aggregate) works cleanly as two
-- separate materialized views in RisingWave, or needs restructuring.

CREATE SOURCE IF NOT EXISTS machine_outlier_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'MOIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

-- Reference profile: 75th percentile of each usage dimension, per machine,
-- per window.
CREATE MATERIALIZED VIEW machine_outlier_reference AS
WITH parsed AS (
    SELECT
        pt,
        split_part(convert_from(line, 'UTF8'), ',', 1) AS machine_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS DOUBLE PRECISION) AS cpu_util,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS DOUBLE PRECISION) AS mem_util,
        CAST(COALESCE(NULLIF(split_part(convert_from(line, 'UTF8'), ',', 5), ''), '0') AS DOUBLE PRECISION) AS mem_bw,
        CAST(COALESCE(NULLIF(split_part(convert_from(line, 'UTF8'), ',', 6), ''), '0') AS DOUBLE PRECISION) AS mkpi,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 7) AS DOUBLE PRECISION) AS net_in,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS DOUBLE PRECISION) AS net_out,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 9) AS DOUBLE PRECISION) AS disk_io
    FROM machine_outlier_source
)
SELECT
    machine_id,
    window_start,
    window_end,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY cpu_util) AS ref_cpu,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY mem_util) AS ref_mem,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY mem_bw) AS ref_membw,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY mkpi) AS ref_mkpi,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY net_in) AS ref_netin,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY net_out) AS ref_netout,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY disk_io) AS ref_diskio
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY machine_id, window_start, window_end;

-- Flag readings whose distance from their machine+window's reference
-- profile exceeds 0.5 (see header comment re: hardcoded threshold).
CREATE MATERIALIZED VIEW machine_outlier_mv AS
WITH parsed AS (
    SELECT
        pt,
        split_part(convert_from(line, 'UTF8'), ',', 1) AS machine_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 2) AS DOUBLE PRECISION) AS ts,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS DOUBLE PRECISION) AS cpu_util,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS DOUBLE PRECISION) AS mem_util,
        CAST(COALESCE(NULLIF(split_part(convert_from(line, 'UTF8'), ',', 5), ''), '0') AS DOUBLE PRECISION) AS mem_bw,
        CAST(COALESCE(NULLIF(split_part(convert_from(line, 'UTF8'), ',', 6), ''), '0') AS DOUBLE PRECISION) AS mkpi,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 7) AS DOUBLE PRECISION) AS net_in,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS DOUBLE PRECISION) AS net_out,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 9) AS DOUBLE PRECISION) AS disk_io
    FROM machine_outlier_source
),
windowed AS (
    SELECT * FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
)
SELECT
    w.machine_id,
    w.ts,
    w.cpu_util,
    w.mem_util,
    w.mkpi,
    w.net_in,
    w.net_out,
    w.disk_io
FROM windowed w
JOIN machine_outlier_reference r
    ON w.machine_id = r.machine_id
   AND w.window_start = r.window_start
   AND w.window_end = r.window_end
WHERE sqrt(
    power(w.cpu_util - r.ref_cpu, 2) +
    power(w.mem_util - r.ref_mem, 2) +
    power(w.mem_bw - r.ref_membw, 2) +
    power(w.mkpi - r.ref_mkpi, 2) +
    power(w.net_in - r.ref_netin, 2) +
    power(w.net_out - r.ref_netout, 2) +
    power(w.disk_io - r.ref_diskio, 2)
) > 0.5;

CREATE SINK IF NOT EXISTS machine_outlier_sink
FROM machine_outlier_mv
WITH (
    connector = 'kafka',
    topic = 'MOOut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
