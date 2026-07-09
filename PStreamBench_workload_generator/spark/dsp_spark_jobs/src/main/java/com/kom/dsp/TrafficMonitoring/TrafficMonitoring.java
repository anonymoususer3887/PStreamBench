package com.kom.dsp.TrafficMonitoring;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

public class TrafficMonitoring {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int matcherPar = Integer.parseInt(pd[2]);
        int avgPar = Integer.parseInt(pd[3]);
        int sinkPar = Integer.parseInt(pd[4]);

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
                    "Spark Traffic Monitoring job currently only supports mode=KAFKA");
        }
        if (query != Constants.QUERY_ONE) {
            throw new IllegalArgumentException("Only query 1 is implemented (matches the Flink source).");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", avgPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "roadmatcher", "avgspeed", "sink",
                "end_to_end_lag");

        Dataset<Row> source = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", input)
                .option("startingOffsets", "latest")
                .load();

        // End-to-end lag: see WordCountJob.java for why current_timestamp()
        // must be materialized into its own column before being observed.
        Dataset<Row> sourceLagColumn = source.withColumn("lag_seconds",
                current_timestamp().cast("double").minus(col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                avg(col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourcePar)
                .observe("source", count(lit(1)).cast("double").as("rows"));

        // Stage [1]: traffic-event-parser, mirrors TrafficEventParser.java's
        // field layout (lat=fields[3], lon=fields[4], speed=fields[5]);
        // malformed lines (!= 7 fields) are dropped.
        StructType eventSchema = new StructType()
                .add("latitude", "double").add("longitude", "double").add("speed", "double");
        Dataset<Row> parsed = lines
                .map((org.apache.spark.api.java.function.MapFunction<String, Row>) line -> {
                    String[] f = line.split(",");
                    if (f.length != 7) {
                        return null;
                    }
                    return RowFactory.create(Double.parseDouble(f[3]), Double.parseDouble(f[4]), Double.parseDouble(f[5]));
                }, Encoders.row(eventSchema))
                .filter(functions.col("latitude").isNotNull())
                .repartition(parserPar)
                .observe("parser", count(lit(1)).cast("double").as("rows"));

        // Stage [2]: road-matcher (map + filter, both use the same
        // parallelism index in the Flink source).
        StructType matchedSchema = new StructType().add("roadId", "long").add("speed", "double");
        Dataset<Row> matched = parsed.mapPartitions(
                (MapPartitionsFunction<Row, Row>) it -> {
                    if (Helper.roadMap.isEmpty()) {
                        try {
                            Helper.loadShapeFile();
                        } catch (Exception e) {
                            // matches the Flink source's swallow-and-continue behavior
                        }
                    }
                    DecimalFormat df = new DecimalFormat("#.##");
                    List<Row> out = new ArrayList<>();
                    while (it.hasNext()) {
                        Row r = it.next();
                        double lat = Double.parseDouble(df.format(r.getDouble(0)));
                        double lon = Double.parseDouble(df.format(r.getDouble(1)));
                        double speed = r.getDouble(2);
                        Long roadId = null;
                        for (Map.Entry<Long, Double[][]> entry : Helper.roadMap.entrySet()) {
                            for (Double[] coord : entry.getValue()) {
                                if (coord[0].equals(lon) && coord[1].equals(lat)) {
                                    roadId = entry.getKey();
                                    break;
                                }
                            }
                            if (roadId != null) {
                                break;
                            }
                        }
                        if (roadId != null) {
                            out.add(RowFactory.create(roadId, speed));
                        }
                    }
                    return out.iterator();
                }, Encoders.row(matchedSchema))
                .repartition(matcherPar)
                .observe("roadmatcher", count(lit(1)).cast("double").as("rows"));

        // Stage [3]: avg-speed, sliding processing-time window average speed per road.
        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";
        Dataset<Row> avgSpeed = matched
                .withColumn("event_time", functions.current_timestamp())
                .groupBy(
                        functions.window(functions.col("event_time"), windowDuration, slideDuration),
                        functions.col("roadId"))
                .agg(functions.avg("speed").as("avgSpeed"))
                .filter(functions.col("avgSpeed").notEqual(0))
                .observe("avgspeed", count(lit(1)).cast("double").as("rows"));

        // Stage [4]: sink, mirrors TrafficMonitoringSerializationSchema.java's
        // "roadId,avgSpeed" format (FormatterOutput.java's differently-worded
        // format is unused dead code in the Flink source).
        Dataset<Row> formatted = avgSpeed
                .selectExpr("CAST(concat(roadId, ',', CAST(avgSpeed AS STRING)) AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", count(lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/trafficmonitoring-" + System.currentTimeMillis();

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
}
