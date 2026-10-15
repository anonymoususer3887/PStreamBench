package com.kom.dsp.GoogleCloudMonitoring;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../GoogleCloudMonitoring/GoogleCloudMonitoring.java.
 *
 * Only timestamp/jobId/category/cpu are needed downstream (taskId,
 * machineId, eventType, userId, priority, ram, disk, constraints are parsed
 * by TaskEventParser.java but never read by either query), so only those
 * fields are extracted here. Query 1 sums cpu per category per window,
 * query 2 averages cpu per jobId per window -- both emit
 * (minTimestamp, key, value) the same way the Flink calculators do.
 * Stage shape: [0] source, [1] parser, [2] average-cpu-per-category/job,
 * [3] sink -- same as Flink for both queries.
 */
public class GoogleCloudMonitoring {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int windowPar = Integer.parseInt(pd[2]);
        int sinkPar = Integer.parseInt(pd[3]);

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
                    "Spark Google Cloud Monitoring job currently only supports mode=KAFKA");
        }
        if (query != Constants.QUERY_ONE && query != Constants.QUERY_TWO) {
            throw new IllegalArgumentException("The only supported queries are \"1\" and \"2\".");
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
        Dataset<Row> sourceLagColumn = source.withColumn("lag_seconds",
                functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourcePar)
                .observe("source", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [1]: parser, mirrors TaskEventParser.java's field layout
        // (jobId=values[0], category=values[3], cpu=values[8]).
        Dataset<Row> parsed = lines
                .map((org.apache.spark.api.java.function.MapFunction<String, Row>) line -> {
                    String[] v = line.split(",");
                    return org.apache.spark.sql.RowFactory.create(
                            Long.parseLong(v[0]), Integer.parseInt(v[3]), Float.parseFloat(v[8]));
                }, Encoders.row(new org.apache.spark.sql.types.StructType()
                        .add("jobId", "long")
                        .add("category", "integer")
                        .add("cpu", "float")))
                .repartition(parserPar)
                .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [2]: average-cpu-per-category (query 1) / average-cpu-per-job (query 2).
        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";
        String keyField = (query == Constants.QUERY_TWO) ? "jobId" : "category";
        String aggExpr = (query == Constants.QUERY_TWO) ? "avg" : "sum";
        Dataset<Row> agg = parsed
                .withColumn("event_time", functions.current_timestamp())
                .groupBy(
                        functions.window(functions.col("event_time"), windowDuration, slideDuration),
                        functions.col(keyField))
                .agg(functions.expr(aggExpr + "(cpu)").as("totalCpu"),
                        functions.min("event_time").as("minTimestamp"))
                .observe("window", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [3]: sink, mirrors CPUPerCatgory.toString() / CPUPerJob.toString().
        String typeName = (query == Constants.QUERY_TWO) ? "CPUPerJob" : "CPUPerCatgory";
        String keyLabel = (query == Constants.QUERY_TWO) ? "jobId" : "category";
        String valueLabel = (query == Constants.QUERY_TWO) ? "averageCpu" : "totalCpu";
        Dataset<Row> formatted = agg
                .selectExpr("CAST(concat('" + typeName + "{timestamp=', " +
                        "CAST(unix_timestamp(minTimestamp) AS STRING), ', " + keyLabel + "=', " +
                        "CAST(" + keyField + " AS STRING), ', " + valueLabel + "=', " +
                        "CAST(totalCpu AS STRING), '}') AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/googlecloudmonitoring-" + System.currentTimeMillis();

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
