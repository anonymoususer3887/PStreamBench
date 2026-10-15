-- Linear Road (Q1): toll notification when a vehicle enters a new segment,
-- priced by that segment's current average speed and occupancy.
--
-- Mirrors com.kom.dsp.LinearRoad.LinearRoad (Flink) query 1 (see
-- VehicleEventParser.java / TollNotificationMapper.java). Input is a
-- comma-separated 15-field position report (0-indexed: type, time,
-- vehicleId, speed, xway, lane, direction, segment, position, qid, sinit,
-- send, dow, tod, day -- only vehicleId/speed/segment are used here).
--
-- TollNotificationMapper keeps two pieces of mutable state per vehicle:
-- (1) that vehicle's last 4 events (used only to look up its previous
-- segment), and (2) a live "which vehicles currently occupy this segment"
-- set per segment, updated by adding the vehicle to its new segment's set
-- and removing it from its old segment's set every time it moves. A toll
-- notification fires only when a vehicle's segment actually changes (not
-- on its very first-ever event), priced from the average speed and count
-- of vehicles CURRENTLY occupying the new segment (including the vehicle
-- that just arrived, since it's added to the set before the average is
-- computed).
--
-- TRANSLATION STRATEGY: "vehicles currently occupying segment X" is
-- exactly "the set of vehicles whose most recently reported segment is X"
-- -- a live snapshot, not a windowed aggregate. This is built with
-- RisingWave's standard "latest row per key" dedup pattern
-- (ROW_NUMBER() OVER (PARTITION BY key ORDER BY time DESC) = 1, RisingWave's
-- own documented idiom for this), then grouped by segment to get live
-- average-speed/occupancy stats. Segment-change detection uses
-- LAG(segment) OVER (PARTITION BY vehicle_id ORDER BY pt) against the raw
-- event stream (not the deduped view, which already reflects the
-- post-move state) -- NULL previous segment means "this vehicle's first
-- event ever," matching the Java code's "events == null -> store and
-- return null, no notification" branch.
--
-- The toll-eligible transition is then joined against the live segment
-- stats via a TEMPORAL JOIN (`FOR SYSTEM_TIME AS OF PROCTIME()`) --
-- RisingWave's built-in mechanism for "look up the latest value in this
-- table as of now," used here instead of a windowed join since there is no
-- time window in the reference logic at all, just current state.
--
-- Toll formula matches calculateToll() exactly, including its Java `(int)`
-- cast semantics (which TRUNCATES toward zero, not rounds -- hence
-- explicit TRUNC() below rather than a bare CAST, since PostgreSQL/
-- RisingWave's numeric-to-int CAST rounds).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. This is the
-- first workload to use a temporal join (`FOR SYSTEM_TIME AS OF
-- PROCTIME()`) and the ROW_NUMBER()-based latest-row dedup pattern over an
-- UNBOUNDED partition (Click Analytics Q3 used ROW_NUMBER() too, but over
-- a bucketed/bounded partition) -- both are real, documented RisingWave
-- features, not guesses, but neither has been exercised live in this
-- project yet, so this carries meaningfully more structural risk than the
-- single-MV workloads.

CREATE SOURCE IF NOT EXISTS linear_road_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'LRIn',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

-- Live "latest known segment/speed" snapshot per vehicle.
CREATE MATERIALIZED VIEW linear_road_vehicle_latest AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS INT) AS speed,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS INT) AS segment
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
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

-- Live per-segment occupancy stats, derived from the snapshot above.
CREATE MATERIALIZED VIEW linear_road_segment_stats AS
SELECT
    segment,
    AVG(speed) AS avg_speed,
    COUNT(*) AS num_vehicles
FROM linear_road_vehicle_latest
GROUP BY segment;

CREATE MATERIALIZED VIEW linear_road_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 8) AS INT) AS segment
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
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
    t.segment,
    CASE WHEN s.avg_speed >= 20
         THEN CAST(TRUNC(s.avg_speed * s.num_vehicles / 3.0) AS INT)
         ELSE 2
    END AS toll,
    s.avg_speed AS average_speed,
    s.num_vehicles AS num_vehicles
FROM transitions t
JOIN linear_road_segment_stats FOR SYSTEM_TIME AS OF PROCTIME() AS s
    ON t.segment = s.segment
WHERE t.prev_segment IS NOT NULL
  AND t.segment != t.prev_segment;

CREATE SINK IF NOT EXISTS linear_road_sink
FROM linear_road_mv
WITH (
    connector = 'kafka',
    topic = 'LROut',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP_SERVER__'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
