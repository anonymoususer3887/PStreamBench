-- Linear Road (Q4): travel time since a vehicle's first-ever report,
-- emitted for every event (non-zero only on that vehicle's type=4 event).
--
-- Mirrors com.kom.dsp.LinearRoad.LinearRoad (Flink) query 4 (see
-- VehicleReportsMapper.java): tracks each vehicle's very first recorded
-- event (used only for its `time` field), and for EVERY subsequent event
-- (not just type=4 ones) emits a row -- type=4 events report
-- `event.time - firstEvent.time`, every other type reports 0. This is a
-- stateless-looking pass-through with one small piece of running state
-- (the vehicle's start time), not an aggregation -- the output has the
-- same cardinality as the input.
--
-- TRANSLATION: MIN(event_time) per vehicle_id is a live, incrementally
-- maintained "first ever event time" snapshot (equivalent to the Java
-- code's `events.get(0).getTime()`, since MIN over an ever-growing set
-- converges to the true first value once at least the first row has been
-- seen). Each raw event is then looked up against that snapshot via a
-- TEMPORAL JOIN (`FOR SYSTEM_TIME AS OF PROCTIME()`, same mechanism as Q1/
-- Q3), LEFT-joined so a vehicle's own very first event (which hasn't
-- necessarily been reflected in the MIN aggregate by the time this join
-- evaluates it) still emits a row rather than being dropped.
--
-- Reuses linear_road_source/_mv/_sink object names -- only one query
-- variant runs per job submission (see
-- runner.py::_run_risingwave_job's query_number handling).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass. Same temporal-join-support risk as Q1/Q3, but
-- structurally the simplest of the four Linear Road queries (a single
-- MIN() aggregate, no windowing, no dedup-by-latest-row pattern needed).

CREATE SOURCE IF NOT EXISTS linear_road_source (
    line BYTEA
) WITH (
    connector = 'kafka',
    topic = 'LRIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

-- Live "first ever event time" snapshot per vehicle.
CREATE MATERIALIZED VIEW linear_road_vehicle_start AS
SELECT
    CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id,
    MIN(CAST(split_part(convert_from(line, 'UTF8'), ',', 2) AS BIGINT)) AS start_time
FROM linear_road_source
WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
GROUP BY vehicle_id;

CREATE MATERIALIZED VIEW linear_road_mv AS
WITH parsed AS (
    SELECT
        CAST(split_part(convert_from(line, 'UTF8'), ',', 1) AS INT) AS type,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 2) AS BIGINT) AS event_time,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 3) AS INT) AS vehicle_id
    FROM linear_road_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 15
)
SELECT
    p.vehicle_id,
    CASE WHEN p.type = 4 THEN p.event_time - COALESCE(s.start_time, p.event_time) ELSE 0 END AS time_travelled
FROM parsed p
LEFT JOIN linear_road_vehicle_start FOR SYSTEM_TIME AS OF PROCTIME() AS s
    ON p.vehicle_id = s.vehicle_id;

CREATE SINK IF NOT EXISTS linear_road_sink
FROM linear_road_mv
WITH (
    connector = 'kafka',
    topic = 'LROut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
