package com.kom.dsp.smartgrid;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../smartgrid/SmartGridJob.java.
 *
 * Note on HouseEvent.getValue() in the Flink source: it returns
 * `this.index`, not `this.value` (setValue()/getValue() disagree) --
 * an evident getter bug there, not a deliberate query semantic. This
 * uses the actual energy-consumption value field, since replicating that
 * bug serves no benchmarking purpose. Query 2's window uses Time.seconds
 * (not milliseconds like every other job/query) in the Flink source --
 * that IS preserved here, since it looks like a deliberate per-query choice.
 * Query 2 also skips parallelism_degree[3] (sink uses index 4), matching
 * the Flink source's own indexing exactly.
 * Stage shape: [0] source, [1] house-event-parser, [2] window aggregation,
 * [3] sink (query 1) / [4] sink (query 2).
 */
public class SmartGridJob {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int windowPar = Integer.parseInt(pd[2]);

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
                    "Spark Smart Grid job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", windowPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "window", "sink", "end_to_end_lag");

        Dataset<Row> source = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", input)
                .option("startingOffsets", "latest")
                .load();

        // End-to-end lag: see WordCountJob.java for why current_timestamp()
        // must be materialized into its own column before being observed.
        Dataset<Row> sourceWithLagColumn = source.withColumn("lag_seconds",
                functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceWithLagColumn.observe("end_to_end_lag",
                functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourcePar)
                .observe("source", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [1]: house-event-parser, mirrors HouseEventParser.java
        // (index, change, value, property, plugs, households, house).
        Dataset<Row> parsed = lines
                .map((MapFunction<String, Row>) line -> {
                    String[] v = line.replace("\"", "").split(",");
                    return org.apache.spark.sql.RowFactory.create(
                            Double.parseDouble(v[2]), Long.parseLong(v[4]),
                            Long.parseLong(v[5]), Long.parseLong(v[6]));
                }, Encoders.row(new org.apache.spark.sql.types.StructType()
                        .add("value", "double")
                        .add("plugs", "long")
                        .add("households", "long")
                        .add("house", "long")))
                .repartition(parserPar)
                .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

        Dataset<Row> formatted;
        int sinkPar;
        String windowDuration;
        String slideDuration;

        if (query == Constants.QUERY_ONE) {
            // Stage [2]: global average load -- median value per house per window.
            windowDuration = slidingWindowSize + " milliseconds";
            slideDuration = slidingWindowSlide + " milliseconds";
            Dataset<Row> q1 = parsed
                    .withColumn("event_time", functions.current_timestamp())
                    .groupBy(
                            functions.window(functions.col("event_time"), windowDuration, slideDuration),
                            functions.col("house"))
                    .agg(functions.expr("percentile(value, 0.5)").as("gal"))
                    .observe("window", functions.count(functions.lit(1)).cast("double").as("rows"));

            sinkPar = Integer.parseInt(pd[3]);
            formatted = q1
                    .selectExpr("CAST(concat('{\\'House\\':', house, ',\\'GAL\\':', CAST(gal AS STRING), '}') AS STRING) AS value")
                    .repartition(sinkPar)
                    .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));
        } else {
            // Stage [2]: local average load -- avg value per (house, households, plugs)
            // per window. Note: window unit is seconds here, matching the Flink source.
            windowDuration = slidingWindowSize + " seconds";
            slideDuration = slidingWindowSlide + " seconds";
            Dataset<Row> q2 = parsed
                    .withColumn("event_time", functions.current_timestamp())
                    .groupBy(
                            functions.window(functions.col("event_time"), windowDuration, slideDuration),
                            functions.col("house"), functions.col("households"), functions.col("plugs"))
                    .agg(functions.avg("value").as("lal"))
                    .observe("window", functions.count(functions.lit(1)).cast("double").as("rows"));

            sinkPar = Integer.parseInt(pd[4]);
            formatted = q2
                    .selectExpr("CAST(concat('{\\'House\\'', house, ',\\'HouseHold\\'', households, " +
                            "',\\'Plug\\'', plugs, ',\\'LAL\\':', CAST(lal AS STRING), '}') AS STRING) AS value")
                    .repartition(sinkPar)
                    .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));
        }

        String checkpointLocation = "/tmp/spark-checkpoints/smartgrid-" + System.currentTimeMillis();

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
