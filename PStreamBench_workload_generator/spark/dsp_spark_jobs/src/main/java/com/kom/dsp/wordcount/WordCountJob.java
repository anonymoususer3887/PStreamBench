package com.kom.dsp.wordcount;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

public class WordCountJob {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] parallelismDegree = parallelism.split(",");
        int sourceParallelism = Integer.parseInt(parallelismDegree[0]);
        int tokenizerParallelism = Integer.parseInt(parallelismDegree[1]);
        int counterParallelism = Integer.parseInt(parallelismDegree[2]);
        int sinkParallelism = Integer.parseInt(parallelismDegree[3]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark word count job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder()
                .getOrCreate();

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // Spark has no native per-operator instrumentation like Flink/Storm,
        // so each stage boundary is marked explicitly; see MetricsUtils).
        MetricsUtils.registerOperatorMetrics(spark, "source", "tokenizer", "counter", "sink", "end_to_end_lag");

        // Stage [2]: groupBy shuffles into exactly this many partitions.
        spark.conf().set("spark.sql.shuffle.partitions", counterParallelism);

        Dataset<Row> source = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", input)
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> sourceWithLagColumn = source.withColumn("lag_seconds",
                current_timestamp().cast("double").minus(col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceWithLagColumn.observe("end_to_end_lag",
                avg(col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag
                .selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourceParallelism)
                .observe("source", count(lit(1)).cast("double").as("rows"));

        // Stage [1]: tokenizer, mirrors Splitter.java's `sentence.split(" ")`.
        Dataset<String> tokens = lines
                .flatMap((FlatMapFunction<String, String>) line ->
                        Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
                .repartition(tokenizerParallelism)
                .observe("tokenizer", count(lit(1)).cast("double").as("rows"));

        // Stage [2]: counter (keyed aggregation).
        Dataset<Row> counts = tokens.groupBy("value").count()
                .observe("counter", count(lit(1)).cast("double").as("rows"));

        // Stage [3]: sink.
        Dataset<Row> formatted = counts
                .selectExpr("CAST(concat(value, ',', CAST(count AS STRING)) AS STRING) AS value")
                .repartition(sinkParallelism)
                .observe("sink", count(lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/wordcount-" + System.currentTimeMillis();

        StreamingQuery query = formatted.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("topic", output)
                .option("checkpointLocation", checkpointLocation)
                .outputMode("update")
                .start();

        System.out.println("[main] Query started, will run for " + secondsToWait + " seconds.");
        query.awaitTermination(secondsToWait * 1000);
        query.stop();
        System.out.println("[main] Query stopped.");
    }
}
