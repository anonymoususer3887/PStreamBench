-- Linear Road (Q2): accident detection -- flag groups of vehicles reported
-- at the exact same (segment, lane, position) within the same window.
--
-- Mirrors com.kom.dsp.LinearRoad.LinearRoad (Flink) query 2 (see
-- AccidentDetectionProcess.java): keys events by their own `time` field
-- (an arbitrary reported value, not a natural grouping key -- preserved
-- faithfully below since it's directly expressible and doesn't change the
-- semantics of the accident check itself, see note below), windows with a
-- sliding PROCESSING-time window, discards events with segment = -1, then
-- within each (time-key, window) group looks for two or more vehicles
-- sharing the same (segment, lane, position) -- a stopped-vehicle pileup --
-- and emits ONE notification per such group (Java's findAccidents() only
-- collects vehicles AFTER the first one at a given key, then main only
-- emits vehiclesInvolved.get(0)).
--
-- SIMPLIFICATION (documented, not silent): rather than replicating "collect
-- duplicates, then emit only the first one," this groups by
-- (time-key, window, segment, lane, position) and filters HAVING COUNT(*) >
-- 1. This is equivalent for every field that matters: every row within
-- such a group shares the same segment/lane/position/time-key by
-- construction (that's what grouping guarantees), so any single
-- representative row carries exactly the same notification content the
-- Java code would have emitted -- the only information lost is which
-- specific duplicate vehicle happened to be "first," which the original
-- output (lane/position/time/segment only, no vehicleId) never surfaces
-- anyway.
--
-- Reuses linear_road_source/_mv/_sink object names -- only one query
-- variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here.

CREATE SOURCE IF NOT EXISTS linear_road_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'LRIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW linear_road_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 2) AS BIGINT) AS event_time,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 6) AS INT) AS lane,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS INT) AS segment,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 9) AS INT) AS position
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
)
SELECT
    lane,
    position,
    event_time,
    segment
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
WHERE segment != -1
GROUP BY event_time, window_start, window_end, segment, lane, position
HAVING COUNT(*) > 1;

CREATE SINK IF NOT EXISTS linear_road_sink
FROM linear_road_mv
WITH (
    connector = 'kafka',
    topic = 'LROut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
