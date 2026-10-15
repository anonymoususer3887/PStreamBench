-- Traffic Monitoring (Q1): average vehicle speed per road segment over a
-- sliding window.
--
-- Mirrors com.kom.dsp.TrafficMonitoring.TrafficMonitoring (Flink) query 1
-- (see TrafficEventParser.java / RoadMatcher.java / Helper.java /
-- AverageSpeedCalculator.java): parses a comma-separated GPS/speed reading
-- (only lines with exactly 7 fields are processed -- carId=field 1,
-- timestamp=field 3, latitude=field 4, longitude=field 5, speed=field 6,
-- bearing=field 7, all 1-indexed), matches each reading to a road segment,
-- keys by that road segment, and computes AVG(speed) over a sliding
-- processing-time window, suppressing windows whose average is exactly 0.
--
-- DELIBERATE SIMPLIFICATION (documented, not silent): the reference
-- RoadMatcher.java resolves an actual road_id by loading a GIS shapefile
-- (Helper.loadShapeFile()) and matching each reading's (lat, lon) --
-- rounded to 2 decimal places -- against that road network's own
-- coordinate table. That shapefile dataset isn't available in this
-- translation pass, and there's no SQL equivalent short of a custom UDF or
-- a pre-loaded spatial lookup table (same category of external-data
-- dependency as Click Analytics Q2's GeoIP lookup). Approximated here by
-- using the rounded (lat, lon) pair itself as a synthetic road-segment key
-- -- same 2-decimal rounding precision as the original RoadMatcher, same
-- "readings near the same point cluster together" intent, but a coordinate
-- grid cell rather than a real matched road_id. Revisit if the actual
-- shapefile data becomes available, using a static RisingWave TABLE loaded
-- from it and joining against that instead.
--
-- Also simplified: malformed input lines (not exactly 7 comma-separated
-- fields) are filtered out directly via a WHERE clause on the raw line,
-- rather than replicating TrafficEventParser.java's actual behavior of
-- emitting an all-default/all-null TrafficEvent object for such lines (a
-- reference-implementation quirk that reads as unintentional, not a
-- meaningful behavior worth preserving).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. Additional
-- open question: whether filtering on
-- array_length(string_to_array(line, ','), 1) = 7 works as expected inside
-- a streaming source's derived CTE.

CREATE SOURCE IF NOT EXISTS traffic_monitoring_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'TMIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

CREATE MATERIALIZED VIEW traffic_monitoring_mv AS
WITH parsed AS (
    SELECT
        pt,
        round(CAST(split_part(convert_from(line, 'UTF8'), ',', 4) AS DOUBLE PRECISION), 2) AS lat_bucket,
        round(CAST(split_part(convert_from(line, 'UTF8'), ',', 5) AS DOUBLE PRECISION), 2) AS lon_bucket,
        CAST(split_part(convert_from(line, 'UTF8'), ',', 6) AS DOUBLE PRECISION) AS speed
    FROM traffic_monitoring_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ','), 1) = 7
)
SELECT
    lat_bucket,
    lon_bucket,
    AVG(speed) AS avg_speed
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY lat_bucket, lon_bucket, window_start, window_end
HAVING AVG(speed) != 0;

CREATE SINK IF NOT EXISTS traffic_monitoring_sink
FROM traffic_monitoring_mv
WITH (
    connector = 'kafka',
    topic = 'TMOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
