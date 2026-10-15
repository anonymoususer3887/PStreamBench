package com.kom.dsp.LogsAnalyzer;

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

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../LogsAnalyzer/LogsAnalyzer.java.
 *
 * Simplification vs. the Flink source: VolumeCounter/StatusCounter there use
 * a hand-rolled per-key ValueState/MapState counter that increments once per
 * input event (shared across overlapping sliding windows, since Flink keyed
 * state is not itself window-scoped) and emits one output line per event.
 * That is an artifact of manual state management, not a deliberate query
 * semantic -- the natural Structured Streaming equivalent (and what this
 * class implements) is a per-key sliding-window COUNT aggregation, emitting
 * one "key: count" line per window firing. Stage/parallelism_degree[] shape
 * is unaffected: [0] source, [1] Log-parser, [2] volume/status counter,
 * [3] sink -- same as Flink.
 */
public class LogsAnalyzer {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int counterPar = Integer.parseInt(pd[2]);
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
                    "Spark Logs Processing job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", counterPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "counter", "sink", "end_to_end_lag");

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

        // Stage [1]: Log-parser, mirrors LogParser.java exactly
        // (logTime = parts[3] minus leading '[', statusCode = parts[8]).
        String keyField = (query == 2) ? "statusCode" : "logTime";
        Dataset<Row> parsed = lines
                .map((MapFunction<String, scala.Tuple2<String, String>>) line -> {
                    String[] parts = line.split(" ");
                    String logTime = parts[3].substring(1);
                    String statusCode = parts[8];
                    return new scala.Tuple2<>(logTime, statusCode);
                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .toDF("logTime", "statusCode")
                .repartition(parserPar)
                .observe("parser", count(lit(1)).cast("double").as("rows"));

        // Stage [2]: volume-counter (query 1) / status-counter (query 2) --
        // sliding processing-time window count keyed by logTime/statusCode.
        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";
        Dataset<Row> counts = parsed
                .withColumn("event_time", functions.current_timestamp())
                .groupBy(
                        functions.window(functions.col("event_time"), windowDuration, slideDuration),
                        functions.col(keyField))
                .count()
                .observe("counter", count(lit(1)).cast("double").as("rows"));

        // Stage [3]: sink.
        Dataset<Row> formatted = counts
                .selectExpr("CAST(concat(" + keyField + ", ': ', CAST(count AS STRING)) AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", count(lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/logsanalyzer-" + System.currentTimeMillis();

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
