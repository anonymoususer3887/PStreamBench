-- Linear Road (Q3): running total toll expenditure per vehicle, reported on
-- demand by a distinct "query" event type.
--
-- Mirrors com.kom.dsp.LinearRoad.LinearRoad (Flink) query 3 (see
-- DailyExpenditureCalculator.java): the input stream mixes two event kinds
-- by `type` field -- type=0 position reports silently accumulate toll
-- (identical segment-transition/pricing logic as Q1, see linear_road.sql)
-- into a running per-vehicle total, while type=2 "query" events emit that
-- vehicle's current running total instead of accumulating anything.
--
-- SIMPLIFICATION (documented, not silent): the Java code has a genuine
-- bug/quirk on the type=2 branch -- `state.get(id) == 0` auto-unboxes a
-- possibly-null Long, so an unset vehicle (null) throws NPE and is caught
-- to report "Total Expenditure: 0", while a vehicle that legitimately has
-- an accumulated total of exactly 0 takes the *other* branch and emits an
-- empty string instead -- two different vehicles in the same real state
-- (no toll yet) can produce different output depending on exception
-- control flow. This is not reproducible declaratively (it depends on
-- Java's null-vs-zero unboxing order, not on any actual data value) and is
-- not behaviorally meaningful, so this translation implements the clearly
-- INTENDED behavior instead: report the vehicle's current total (0 if it
-- has none yet) for every type=2 event.
--
-- Reuses the Q1 toll-accrual pipeline (vehicle_latest dedup ->
-- segment_stats -> per-transition toll), then GROUP BY vehicle_id SUM(toll)
-- to get a live running total per vehicle (RisingWave materialized views
-- maintain this incrementally, so it always reflects the latest state --
-- no explicit "running sum" bookkeeping needed). Type=2 query events are
-- then joined against that running total via a TEMPORAL JOIN
-- (`FOR SYSTEM_TIME AS OF PROCTIME()`, same mechanism as Q1's segment-stats
-- lookup), LEFT-joined so a vehicle with no toll history yet still reports
-- 0 rather than being dropped.
--
-- Reuses linear_road_source/_mv/_sink object names -- only one query
-- variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass. Carries the same temporal-join/ROW_NUMBER-dedup
-- risk as Q1 (see linear_road.sql's header comment), plus an additional,
-- as-yet-unconfirmed assumption that RisingWave's temporal join supports
-- LEFT JOIN (used here so vehicles with zero toll history still emit a
-- row) -- Q1 only needed an INNER temporal join.

CREATE SOURCE IF NOT EXISTS linear_road_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'LRIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

-- Live "latest known segment/speed" snapshot per vehicle (same as Q1).
CREATE MATERIALIZED VIEW linear_road_vehicle_latest AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS INT) AS speed,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS INT) AS segment
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
      AND CAST(split_part(convert_from(line, 'UTF8'), ',', 1) AS INT) = 0
),
ranked AS (
    SELECT
        vehicle_id, speed, segment,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY pt DESC) AS rn
    FROM parsed
)
SELECT vehicle_id, speed, segment
FROM ranked
WHERE rn = 1;

-- Live per-segment occupancy stats (same as Q1).
CREATE MATERIALIZED VIEW linear_road_segment_stats AS
SELECT
    segment,
    AVG(speed) AS avg_speed,
    COUNT(*) AS num_vehicles
FROM linear_road_vehicle_latest
GROUP BY segment;

-- Per-transition toll events (same pricing logic as Q1), type=0 only.
CREATE MATERIALIZED VIEW linear_road_toll_events AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS INT) AS segment
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
      AND CAST(split_part(convert_from(line, 'UTF8'), ',', 1) AS INT) = 0
),
transitions AS (
    SELECT
        vehicle_id,
        segment,
        LAG(segment) OVER (PARTITION BY vehicle_id ORDER BY pt) AS prev_segment
    FROM parsed
)
SELECT
    t.vehicle_id,
    CASE WHEN s.avg_speed >= 20
         THEN CAST(TRUNC(s.avg_speed * s.num_vehicles / 3.0) AS INT)
         ELSE 2
    END AS toll
FROM transitions t
JOIN linear_road_segment_stats FOR SYSTEM_TIME AS OF PROCTIME() AS s
    ON t.segment = s.segment
WHERE t.prev_segment IS NOT NULL
  AND t.segment != t.prev_segment;

-- Live running total toll per vehicle, incrementally maintained.
CREATE MATERIALIZED VIEW linear_road_expenditure AS
SELECT
    vehicle_id,
    SUM(toll) AS total_expenditure
FROM linear_road_toll_events
GROUP BY vehicle_id;

CREATE MATERIALIZED VIEW linear_road_mv AS
WITH parsed AS (
    SELECT
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
      AND CAST(split_part(convert_from(line, 'UTF8'), ',', 1) AS INT) = 2
)
SELECT
    p.vehicle_id,
    COALESCE(e.total_expenditure, 0) AS total_expenditure
FROM parsed p
LEFT JOIN linear_road_expenditure FOR SYSTEM_TIME AS OF PROCTIME() AS e
    ON p.vehicle_id = e.vehicle_id;

CREATE SINK IF NOT EXISTS linear_road_sink
FROM linear_road_mv
WITH (
    connector = 'kafka',
    topic = 'LROut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
