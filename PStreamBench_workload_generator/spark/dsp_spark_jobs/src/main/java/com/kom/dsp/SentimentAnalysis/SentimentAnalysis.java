package com.kom.dsp.SentimentAnalysis;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../SentimentAnalysis/SentimentAnalysis.java.
 *
 * BasicClassifier.java/SentimentResult.java/SentimentClassifier.java are
 * reused unchanged (plain Java + Guava, no Flink dependency) --
 * AFINN-111.txt is copied into this module's resources under the same
 * classpath path they expect. TwitterAnalyser.java's map() constructs a
 * brand new SentimentClassifier (re-reading and re-parsing the AFINN
 * lexicon from the classpath) for every single tweet; this loads it once
 * per Spark partition via mapPartitions instead. That avoids the Flink
 * source's per-record file re-read, which is pure object/IO overhead, not
 * the CPU classification cost the workload is meant to stress -- the
 * per-tweet word-lookup classification work itself is unchanged.
 * No windowing/keying in either version -- this is the simplest job in the
 * suite to port. Stage shape: [0] source, [1] twitter-parser,
 * [2] twitter-analyser, [3] sink -- same as Flink.
 */
public class SentimentAnalysis {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int analyserPar = Integer.parseInt(pd[2]);
        int sinkPar = Integer.parseInt(pd[3]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Sentiment Analysis job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "analyser", "sink", "end_to_end_lag");

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

        // Stage [1]: twitter-parser, mirrors TwitterParser.java's JSON fields.
        StructType tweetSchema = new StructType()
                .add("id", "string").add("timestamp", "string").add("text", "string");
        Dataset<Row> tweets = lines
                .map((org.apache.spark.api.java.function.MapFunction<String, Row>) s -> {
                    com.google.gson.JsonObject obj = new com.google.gson.JsonParser().parse(s).getAsJsonObject();
                    return RowFactory.create(obj.get("id").getAsString(), obj.get("created_at").getAsString(),
                            obj.get("text").getAsString());
                }, Encoders.row(tweetSchema))
                .repartition(parserPar)
                .observe("parser", count(lit(1)).cast("double").as("rows"));

        // Stage [2]: twitter-analyser.
        StructType scoredSchema = new StructType()
                .add("id", "string").add("timestamp", "string").add("text", "string")
                .add("sentiment", "string").add("sentimentScore", "double");
        Dataset<Row> scored = tweets.mapPartitions(
                (MapPartitionsFunction<Row, Row>) it -> {
                    SentimentClassifier classifier = new BasicClassifier();
                    classifier.initialize();
                    List<Row> out = new ArrayList<>();
                    while (it.hasNext()) {
                        Row r = it.next();
                        SentimentResult result = classifier.classify(r.getString(2));
                        out.add(RowFactory.create(r.getString(0), r.getString(1), r.getString(2),
                                result.getSentiment().toString(), result.getScore()));
                    }
                    return out.iterator();
                }, Encoders.row(scoredSchema))
                .repartition(analyserPar)
                .observe("analyser", count(lit(1)).cast("double").as("rows"));

        // Stage [3]: sink, mirrors TweetScored.toString().
        Dataset<Row> formatted = scored
                .selectExpr("CAST(concat('TweetScored{id=\\'', id, '\\', timestamp=\\'', timestamp, " +
                        "'\\', text=\\'', text, '\\', sentiment=\\'', sentiment, '\\', sentimentScore=', " +
                        "CAST(sentimentScore AS STRING), '}') AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", count(lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/sentimentanalysis-" + System.currentTimeMillis();

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
