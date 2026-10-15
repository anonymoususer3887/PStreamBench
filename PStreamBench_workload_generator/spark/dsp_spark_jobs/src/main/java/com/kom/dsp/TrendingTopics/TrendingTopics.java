package com.kom.dsp.TrendingTopics;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../TrendingTopics/TrendingTopics.java (query 1 only,
 * matching what the Flink job itself implements).
 *
 * Stage -> parallelism_degree[] mapping, same convention as WordCountJob:
 *   [0] source, [1] twitter-parser, [2] topic-extractor + rolling-counter
 *   (shared, matching Flink's own reuse of parallelism_degree[2] for both),
 *   [3] popularity-detector, [4] sink.
 */
public class TrendingTopics {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int counterPar = Integer.parseInt(pd[2]);
        int filterPar = Integer.parseInt(pd[3]);
        int sinkPar = Integer.parseInt(pd[4]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        int topicPopularityThreshold = Integer.parseInt(params.get("popularityThreshold"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Trending Topics job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", counterPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "topicextractor", "counter", "filter",
                "sink", "end_to_end_lag");

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

        // Stage [1]: twitter-parser, mirrors TwitterParser.java's CSV field extraction
        // (id=values[1], timestamp=values[2], text=values[5]) -- only text is needed
        // downstream so that is all that is carried forward.
        Dataset<String> tweetTexts = lines
                .map((MapFunction<String, String>) line -> line.split(",")[5], Encoders.STRING())
                .repartition(parserPar)
                .observe("parser", count(lit(1)).cast("double").as("rows"));

        // Stage [2a]: topic-extractor, mirrors TopicExtractor.java exactly
        // (words starting with @ or #, symbol stripped).
        Dataset<String> topics = tweetTexts
                .flatMap((FlatMapFunction<String, String>) text -> {
                    List<String> out = new ArrayList<>();
                    for (String line : text.split("\n")) {
                        for (String word : line.split("\\s+")) {
                            if (word.startsWith("@") || word.startsWith("#")) {
                                out.add(word.substring(1));
                            }
                        }
                    }
                    return out.iterator();
                }, Encoders.STRING())
                .repartition(counterPar)
                .observe("topicextractor", count(lit(1)).cast("double").as("rows"));

        // Stage [2b]: rolling-counter, sliding *processing*-time window count per topic
        // (mirrors SlidingProcessingTimeWindows -- current_timestamp() stands in for
        // Flink's ProcessingTime characteristic since Spark windows key off a real column).
        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";
        Dataset<Row> counts = topics
                .withColumn("event_time", functions.current_timestamp())
                .groupBy(
                        functions.window(functions.col("event_time"), windowDuration, slideDuration),
                        functions.col("value").as("topic"))
                .count()
                .observe("counter", count(lit(1)).cast("double").as("rows"));

        // Stage [3]: popularity-detector.
        Dataset<Row> popular = counts
                .filter(functions.col("count").gt(topicPopularityThreshold))
                .selectExpr("topic AS value")
                .repartition(filterPar)
                .observe("filter", count(lit(1)).cast("double").as("rows"));

        // Stage [4]: sink.
        Dataset<Row> formatted = popular.repartition(sinkPar)
                .observe("sink", count(lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/trendingtopics-" + System.currentTimeMillis();

        StreamingQuery query = formatted.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("topic", output)
                .option("checkpointLocation", checkpointLocation)
                .outputMode("update")
                .start();

        query.awaitTermination(secondsToWait * 1000);
        query.stop();
    }
}
