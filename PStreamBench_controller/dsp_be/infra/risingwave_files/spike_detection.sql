-- Spike Detection (Q1): flag sensor readings whose voltage exceeds their
-- own sensor's sliding-window average.
--
-- Mirrors com.kom.dsp.SpikeDetection.SpikeDetection (Flink) (see
-- SensorParser.java / AverageValueCalculator.java): parses a
-- WHITESPACE-separated sensor reading line, keys by sensorId, computes
-- AVG(voltage) over a sliding processing-time window, and -- for EVERY
-- reading in that window, not just one row per window -- emits (sensorId,
-- thatReading'sOwnVoltage, windowAverageVoltage), then keeps only readings
-- whose own voltage exceeds the window average ("spikes"). This "one
-- output row per input row, each paired with its window's aggregate"
-- shape is the same structural pattern as Machine Outlier's
-- reference-profile join, just with a plain AVG instead of a percentile.
--
-- FIELD-OFFSET CORRECTION (documented, not silent): the real data (Intel
-- Berkeley Research Lab sensor dataset, the same one used to build this
-- project's spikeDetection_sensors.dat) has EIGHT whitespace-separated
-- columns: date, time, an epoch/reading-sequence counter, sensorId,
-- temperature, humidity, light, voltage. com.kom.dsp.SpikeDetection.
-- SensorParser.java's `fields.split("\\s+")` only reads seven fields --
-- sensorId=fields[2], temperature=fields[3], humidity=fields[4],
-- light=fields[5], voltage=fields[6] -- silently skipping the epoch
-- column, which shifts every field it reads one position early (it
-- actually reads the epoch counter as sensorId, the real sensorId as
-- temperature, temperature as humidity, humidity as light, and light as
-- voltage). This looks like an overlooked indexing bug in the reference
-- Java, not an intentional simplification (unlike, say, Machine Outlier's
-- genuinely-unused threshold parameter) -- so rather than faithfully
-- reproducing the same off-by-one shift, this translation reads the
-- CORRECT columns directly: sensorId=field 4, temperature=field 5,
-- humidity=field 6, light=field 7, voltage=field 8 (all 1-indexed; date
-- and time in fields 1-2 are parsed as a timestamp but not used
-- downstream, same as before, so still skipped here).
--
-- The real dataset also has a handful of malformed/short lines (missing
-- trailing readings, e.g. a dropped light or voltage value) -- filtered
-- out via an array_length = 8 check, same malformed-row-filtering
-- convention used elsewhere in this project (Traffic Monitoring, TPCH).
--
-- NOT YET VALIDATED against a live RisingWave instance -- deferred to a
-- later end-to-end pass, same as every other workload here. This one's
-- core aggregate (AVG) is one of the safest/most standard streaming
-- aggregates translated so far (unlike percentile_cont or ordered
-- ARRAY_AGG), so the main residual risk is the windowed self-join pattern
-- itself (same open question as Machine Outlier) rather than the
-- aggregate function.

CREATE SOURCE IF NOT EXISTS spike_detection_source (
    line BYTEA,
    pt TIMESTAMP AS proctime()
) WITH (
    connector = 'kafka',
    topic = 'SpikeIn',
    properties.bootstrap.server = 'ms1121:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE BYTES;

-- Per-sensor, per-window average voltage.
CREATE MATERIALIZED VIEW spike_detection_average AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ' ', 4) AS INT) AS sensor_id,
        CAST(split_part(convert_from(line, 'UTF8'), ' ', 8) AS DOUBLE PRECISION) AS voltage
    FROM spike_detection_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ' '), 1) = 8
)
SELECT
    sensor_id,
    window_start,
    window_end,
    AVG(voltage) AS avg_voltage
FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
GROUP BY sensor_id, window_start, window_end;

-- Each reading joined against its own window's average, filtered to spikes.
CREATE MATERIALIZED VIEW spike_detection_mv AS
WITH parsed AS (
    SELECT
        pt,
        CAST(split_part(convert_from(line, 'UTF8'), ' ', 4) AS INT) AS sensor_id,
        CAST(split_part(convert_from(line, 'UTF8'), ' ', 8) AS DOUBLE PRECISION) AS voltage
    FROM spike_detection_source
    WHERE array_length(string_to_array(convert_from(line, 'UTF8'), ' '), 1) = 8
),
windowed AS (
    SELECT * FROM HOP(parsed, pt, INTERVAL '__WINDOW_SLIDE_SEC__ second', INTERVAL '__WINDOW_SIZE_SEC__ second')
)
SELECT
    w.sensor_id,
    w.voltage AS current_value,
    a.avg_voltage AS average_value
FROM windowed w
JOIN spike_detection_average a
    ON w.sensor_id = a.sensor_id
   AND w.window_start = a.window_start
   AND w.window_end = a.window_end
WHERE w.voltage > a.avg_voltage;

CREATE SINK IF NOT EXISTS spike_detection_sink
FROM spike_detection_mv
WITH (
    connector = 'kafka',
    topic = 'SpikeOut',
    properties.bootstrap.server = 'ms1121:9092'
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
