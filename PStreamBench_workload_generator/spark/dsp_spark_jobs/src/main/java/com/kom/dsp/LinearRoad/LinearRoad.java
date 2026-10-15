package com.kom.dsp.LinearRoad;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../LinearRoad/LinearRoad.java.
 *
 * Queries 1, 3, and 4 are per-vehicle keyed RichMapFunctions with manual
 * MapState -- flatMapGroupsWithState keyed by vehicleId is Structured
 * Streaming's equivalent (see VehicleState.java for a note on Query 1/3's
 * segment-occupancy tracking, which is siloed per vehicle in the Flink
 * source too, not truly cross-vehicle). Query 2 (accident detection) has no
 * per-vehicle state at all in the Flink source -- it flags any group of
 * events sharing (segment, lane, position) within a window as a collision,
 * which maps directly onto a builtin groupBy+count.
 * Stage shape: query 1/2 use [0] source+parser, [1] mapper, [2] formatter;
 * query 3/4 use only [0], [1] (no separate formatter stage) -- same as
 * Flink.
 */
public class LinearRoad {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int mapperPar = Integer.parseInt(pd[1]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int query = Integer.parseInt(params.get("query"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Linear Road job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", mapperPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        // Queries 1/2 have a separate formatter stage; queries 3/4 emit
        // directly from the mapper stage (see class javadoc), so only
        // source/mapper are registered for those.
        if (query == Constants.QUERY_ONE || query == Constants.QUERY_TWO) {
            MetricsUtils.registerOperatorMetrics(spark, "source", "mapper", "formatter", "end_to_end_lag");
        } else {
            MetricsUtils.registerOperatorMetrics(spark, "source", "mapper", "end_to_end_lag");
        }

        Dataset<Row> source = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", input)
                .option("startingOffsets", "latest")
                .load();

        // End-to-end lag: see WordCountJob.java for why current_timestamp()
        // must be materialized into its own column before being observed.
        Dataset<Row> sourceLagColumn = source.withColumn("lag_seconds",
                functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source + Vehicle-event-parser, mirrors VehicleEventParser.java
        // (15 CSV fields; only the 9 actually read anywhere downstream are kept).
        Dataset<VehicleEvent> events = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .map((MapFunction<String, VehicleEvent>) line -> {
                    String[] f = line.split(",");
                    if (f.length != 15) {
                        return new VehicleEvent();
                    }
                    return new VehicleEvent(Integer.parseInt(f[0]), Long.parseLong(f[1]), Integer.parseInt(f[2]),
                            Integer.parseInt(f[3]), Integer.parseInt(f[4]), Integer.parseInt(f[5]),
                            Integer.parseInt(f[6]), Integer.parseInt(f[7]), Integer.parseInt(f[8]));
                }, Encoders.bean(VehicleEvent.class))
                .repartition(sourcePar)
                .observe("source", functions.count(functions.lit(1)).cast("double").as("rows"));

        Dataset<Row> formatted;

        if (query == Constants.QUERY_ONE) {
            int formatterPar = Integer.parseInt(pd[2]);

            // Stage [1]: toll-notification.
            KeyValueGroupedDataset<Integer, VehicleEvent> grouped =
                    events.groupByKey((MapFunction<VehicleEvent, Integer>) VehicleEvent::getVehicleId, Encoders.INT());

            Dataset<String> tollNotifications = grouped.flatMapGroupsWithState(
                    (FlatMapGroupsWithStateFunction<Integer, VehicleEvent, VehicleState, String>) (vehicleId, values, state) -> {
                        VehicleState s = state.exists() ? state.get() : new VehicleState();
                        List<String> results = new ArrayList<>();
                        while (values.hasNext()) {
                            VehicleEvent event = values.next();
                            int segmentId = event.getSegment();
                            boolean firstEventEver = s.recentEvents.isEmpty();
                            s.recentEvents.add(event);
                            if (s.recentEvents.size() > 4) {
                                s.recentEvents.remove(0);
                            }
                            if (!firstEventEver) {
                                List<VehicleEvent> inSegment = s.segmentVehicles.get(segmentId);
                                if (inSegment == null) {
                                    inSegment = new ArrayList<>();
                                    inSegment.add(event);
                                    s.segmentVehicles.put(segmentId, inSegment);
                                    int toll = calculateToll(segmentId, 1);
                                    results.add("VehicleId: " + vehicleId + " Segment: " + segmentId + " Toll: " + toll);
                                } else {
                                    boolean present = inSegment.stream().anyMatch(v -> v.getVehicleId() == vehicleId);
                                    if (!present) {
                                        inSegment.add(event);
                                        int toll = calculateToll(event.getSpeed(), 1);
                                        results.add("VehicleId: " + vehicleId + " Segment: " + segmentId + " Toll: " + toll);
                                    }
                                }
                            }
                        }
                        state.update(s);
                        return results.iterator();
                    },
                    OutputMode.Update(), Encoders.javaSerialization(VehicleState.class), Encoders.STRING(),
                    GroupStateTimeout.NoTimeout())
                    .observe("mapper", functions.count(functions.lit(1)).cast("double").as("rows"));

            formatted = tollNotifications.toDF("value").repartition(formatterPar)
                    .observe("formatter", functions.count(functions.lit(1)).cast("double").as("rows"));

        } else if (query == Constants.QUERY_TWO) {
            int formatterPar = Integer.parseInt(pd[2]);

            // Stage [1]: accident-notification -- events sharing (segment, lane,
            // position) within the same window indicate a collision.
            String windowDuration = slidingWindowSize + " seconds";
            String slideDuration = slidingWindowSlide + " seconds";
            Dataset<Row> collisions = events
                    .filter(functions.col("segment").notEqual(-1))
                    .withColumn("event_time", functions.current_timestamp())
                    .groupBy(
                            functions.window(functions.col("event_time"), windowDuration, slideDuration),
                            functions.col("segment"), functions.col("lane"), functions.col("position"))
                    .agg(functions.count("*").as("cnt"), functions.max("time").as("ts"))
                    .filter(functions.col("cnt").gt(1))
                    .observe("mapper", functions.count(functions.lit(1)).cast("double").as("rows"));

            formatted = collisions
                    .selectExpr("CAST(concat('Accident occurred at ---Segment:', segment, ' lane: ', lane, " +
                            "' position: ', position, ' timestamp: ', CAST(ts AS STRING)) AS STRING) AS value")
                    .repartition(formatterPar)
                    .observe("formatter", functions.count(functions.lit(1)).cast("double").as("rows"));

        } else if (query == Constants.QUERY_THREE) {
            // Stage [1]: daily-expenditure (no separate formatter stage, matching Flink).
            KeyValueGroupedDataset<Integer, VehicleEvent> grouped =
                    events.groupByKey((MapFunction<VehicleEvent, Integer>) VehicleEvent::getVehicleId, Encoders.INT());

            Dataset<String> expenditures = grouped.flatMapGroupsWithState(
                    (FlatMapGroupsWithStateFunction<Integer, VehicleEvent, VehicleState, String>) (vehicleId, values, state) -> {
                        VehicleState s = state.exists() ? state.get() : new VehicleState();
                        List<String> results = new ArrayList<>();
                        while (values.hasNext()) {
                            VehicleEvent event = values.next();
                            if (event.getType() == 2) {
                                results.add("Vehicle:" + vehicleId + ",Total Expenditure: " + s.totalExpenditure);
                            } else if (event.getType() == 0) {
                                int segmentId = event.getSegment();
                                boolean firstEventEver = s.recentEvents.isEmpty();
                                s.recentEvents.add(event);
                                if (s.recentEvents.size() > 4) {
                                    s.recentEvents.remove(0);
                                }
                                if (firstEventEver) {
                                    s.totalExpenditure = 0;
                                } else {
                                    List<VehicleEvent> inSegment = s.segmentVehicles.get(segmentId);
                                    if (inSegment == null) {
                                        inSegment = new ArrayList<>();
                                        inSegment.add(event);
                                        s.segmentVehicles.put(segmentId, inSegment);
                                        s.totalExpenditure = calculateToll(segmentId, 1);
                                    } else {
                                        boolean present = inSegment.stream().anyMatch(v -> v.getVehicleId() == vehicleId);
                                        if (!present) {
                                            inSegment.add(event);
                                            s.totalExpenditure += calculateToll(event.getSpeed(), 1);
                                        }
                                    }
                                }
                            }
                        }
                        state.update(s);
                        return results.iterator();
                    },
                    OutputMode.Update(), Encoders.javaSerialization(VehicleState.class), Encoders.STRING(),
                    GroupStateTimeout.NoTimeout())
                    .observe("mapper", functions.count(functions.lit(1)).cast("double").as("rows"));

            formatted = expenditures.toDF("value").repartition(mapperPar);

        } else if (query == 4) {
            // Stage [1]: vehicle-report-mapper (no separate formatter stage, matching Flink).
            KeyValueGroupedDataset<Integer, VehicleEvent> grouped =
                    events.groupByKey((MapFunction<VehicleEvent, Integer>) VehicleEvent::getVehicleId, Encoders.INT());

            Dataset<String> reports = grouped.flatMapGroupsWithState(
                    (FlatMapGroupsWithStateFunction<Integer, VehicleEvent, VehicleState, String>) (vehicleId, values, state) -> {
                        VehicleState s = state.exists() ? state.get() : new VehicleState();
                        List<String> results = new ArrayList<>();
                        while (values.hasNext()) {
                            VehicleEvent event = values.next();
                            if (s.firstEventTime == null) {
                                s.firstEventTime = event.getTime();
                            }
                            if (event.getType() == 4) {
                                long timeTaken = event.getTime() - s.firstEventTime;
                                results.add("VehicleID: " + vehicleId + "timeTravelled" + timeTaken);
                            } else {
                                results.add("VehicleID: " + vehicleId + "timeTravelled 0");
                            }
                        }
                        state.update(s);
                        return results.iterator();
                    },
                    OutputMode.Update(), Encoders.javaSerialization(VehicleState.class), Encoders.STRING(),
                    GroupStateTimeout.NoTimeout())
                    .observe("mapper", functions.count(functions.lit(1)).cast("double").as("rows"));

            formatted = reports.toDF("value").repartition(mapperPar);

        } else {
            throw new IllegalArgumentException("The only supported queries are \"1\", \"2\", \"3\", and \"4\".");
        }

        String checkpointLocation = "/tmp/spark-checkpoints/linearroad-" + System.currentTimeMillis();

        StreamingQuery streamingQuery = formatted.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("topic", output)
                .option("checkpointLocation", checkpointLocation)
                .outputMode("update")
                .start();

        streamingQuery.awaitTermination(secondsToWait * 1000);
        streamingQuery.stop();
    }

    // Mirrors TollNotificationMapper.java / DailyExpenditureCalculator.java exactly,
    // including passing segmentId (not a speed) as "averageSpeed" on first entry into
    // a segment -- that is what the Flink source actually does, not a translation slip.
    private static int calculateToll(double averageSpeed, int numVehicles) {
        int toll = 2;
        if (averageSpeed >= 20) {
            toll = (int) ((averageSpeed * numVehicles) / 3);
        }
        return toll;
    }
}
