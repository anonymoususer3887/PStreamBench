package com.kom.dsp.AdsAnalytics;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../AdsAnalytics/AdsAnalytics.java.
 *
 * 1. Flink's Kafka source reuses the *same* KafkaSource object (same
 *    consumer group "my-group") for both the "clicks" and "impressions"
 *    reads, which in a real multi-partition topic would split partitions
 *    between the two consumers rather than have both see every record --
 *    almost certainly an unintentional artifact of the source code, not a
 *    deliberate semantic. This uses two independent consumer groups so
 *    each branch genuinely sees the full topic.
 * 2. Flink joins clicksCounter and impressionsCounter *after* each has
 *    already been aggregated (keyBy+sum). Structured Streaming does not
 *    allow joining two DataFrames where either side has already gone
 *    through a streaming aggregation, regardless of watermarks -- so that
 *    exact structure can't be replicated. An earlier version of this file
 *    instead joined the two *pre-aggregation* (raw, per-event) streams and
 *    aggregated afterwards; that join itself is legal (a plain stream-
 *    stream join with watermarks on both sides is fully supported), but it
 *    produced wrong results: joining raw events means a key with N clicks
 *    and M impressions in the watermark window produces an N*M cross-
 *    product of matched rows *before* the sum, so summing afterwards
 *    inflates both totals by a factor proportional to event volume per key
 *    -- confirmed on a live cluster, not a hypothetical concern.
 *
 *    The fix: tag each side with its event type, union them into one
 *    stream, and do a *single* windowed aggregation with conditional sums
 *    (sum of value where type='click', sum of value where type='impression',
 *    in the same groupBy) instead of a join. This computes the exact same
 *    per-key totals Flink's aggregate-then-join produces, with no fan-out,
 *    because there is no join at all -- the query no longer exercises
 *    Spark's join operator specifically, trading that off for correctness
 *    (a hand-rolled join via foreachBatch + custom accumulator state could
 *    preserve a literal join, at the cost of no built-in fault tolerance
 *    and meaningfully more custom code; not worth it for what the join
 *    operator alone would add over a groupBy of comparable shuffle cost).
 *
 *    Since there's no longer a stream-stream join, Update output mode
 *    works again (Append was only required because of the join) --
 *    finalOutputMode is unconditionally "update" now.
 *
 * Query 2 has no join in the Flink source either: Q2AdEventParser already
 * carries both clicks and views in a single record, and Q2AdEventCounter's
 * hand-rolled running HashMap-per-instance counter is replaced with a
 * stateless per-event CTR (clicks/views) feeding the same windowed average
 * Q2RollingAverage computes, consistent with the simplification pattern
 * used for LogsAnalyzer/SpikeDetection elsewhere in this module.
 */
public class AdsAnalytics {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");

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
                    "Spark Ads Analytics job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        StructType parsedSchema = new StructType()
                .add("queryId", "long").add("adId", "long").add("cnt", "integer");

        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";

        Dataset<Row> formatted;
        int sinkPar;
        // No stream-stream join anymore (see class javadoc) -- Update mode,
        // same as every other job in this module.
        String finalOutputMode = "update";

        if (query == Constants.QUERY_ONE) {
            int clickParserPar = Integer.parseInt(pd[1]);
            int impressionParserPar = Integer.parseInt(pd[2]);
            int clicksTagPar = Integer.parseInt(pd[3]);
            int impressionsTagPar = Integer.parseInt(pd[4]);
            int windowPar = Integer.parseInt(pd[5]);
            sinkPar = Integer.parseInt(pd[6]);
            spark.conf().set("spark.sql.shuffle.partitions", windowPar);

            // Per-stage row counts + end-to-end lag (Tier 3 operator metrics
            // -- see MetricsUtils / WordCountJob.java). Two independent
            // sources feed this query, so both get their own lag observation
            // rather than one; "window" and "sink" selectivity/throughput
            // are computed by hand in getprom_spark.py's Ad Analytics Query 1
            // collector instead of the generic sequential-chain helper, since
            // "window" aggregates the *union* of clicksparser+impressionsparser,
            // not a single upstream stage.
            MetricsUtils.registerOperatorMetrics(spark, "clicksparser", "impressionsparser", "window", "sink",
                    "end_to_end_lag_clicks", "end_to_end_lag_impressions");

            // Stage [1]: click-parser (own consumer group -- see class javadoc).
            Dataset<Row> clicksSource = spark.readStream().format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServer)
                    .option("subscribe", input)
                    .option("kafka.group.id", "adsanalytics-clicks")
                    .option("startingOffsets", "latest").load();
            Dataset<Row> clicksSourceLagColumn = clicksSource.withColumn("lag_seconds",
                    functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
            Dataset<Row> clicksSourceWithLag = clicksSourceLagColumn.observe("end_to_end_lag_clicks",
                    functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));
            Dataset<Row> clicksParsed = clicksSourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                    .as(Encoders.STRING())
                    .map((org.apache.spark.api.java.function.MapFunction<String, Row>) line -> {
                        String[] r = line.split("\t");
                        return RowFactory.create(Long.parseLong(r[7]), Long.parseLong(r[3]), Integer.parseInt(r[0]));
                    }, Encoders.row(parsedSchema))
                    .repartition(clickParserPar)
                    .observe("clicksparser", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [2]: impression-parser (own consumer group).
            Dataset<Row> impressionsSource = spark.readStream().format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServer)
                    .option("subscribe", input)
                    .option("kafka.group.id", "adsanalytics-impressions")
                    .option("startingOffsets", "latest").load();
            Dataset<Row> impressionsSourceLagColumn = impressionsSource.withColumn("lag_seconds",
                    functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
            Dataset<Row> impressionsSourceWithLag = impressionsSourceLagColumn.observe("end_to_end_lag_impressions",
                    functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));
            Dataset<Row> impressionsParsed = impressionsSourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                    .as(Encoders.STRING())
                    .map((org.apache.spark.api.java.function.MapFunction<String, Row>) line -> {
                        String[] r = line.split("\t");
                        return RowFactory.create(Long.parseLong(r[7]), Long.parseLong(r[3]), Integer.parseInt(r[1]));
                    }, Encoders.row(parsedSchema))
                    .repartition(impressionParserPar)
                    .observe("impressionsparser", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [3]/[4]: tag each side with its event type and unify the
            // column names (cnt -> value), so both sides can be unioned into
            // one stream and aggregated together -- see class javadoc for why
            // this replaces the previous join-then-aggregate structure.
            Dataset<Row> clicksTagged = clicksParsed
                    .withColumn("eventType", functions.lit("click"))
                    .withColumnRenamed("cnt", "value")
                    .repartition(clicksTagPar);
            Dataset<Row> impressionsTagged = impressionsParsed
                    .withColumn("eventType", functions.lit("impression"))
                    .withColumnRenamed("cnt", "value")
                    .repartition(impressionsTagPar);

            Dataset<Row> unioned = clicksTagged.unionByName(impressionsTagged)
                    .withColumn("event_time", functions.current_timestamp())
                    .withWatermark("event_time", "10 seconds");

            // Stage [5]: single windowed aggregation, conditional sums per
            // event type instead of separate sums either side of a join.
            Dataset<Row> rollingCtr = unioned
                    .groupBy(
                            functions.window(functions.col("event_time"), windowDuration, slideDuration),
                            functions.col("queryId"), functions.col("adId"))
                    .agg(
                            functions.sum(functions.when(functions.col("eventType").equalTo("click"),
                                    functions.col("value")).otherwise(0)).as("clicksSum"),
                            functions.sum(functions.when(functions.col("eventType").equalTo("impression"),
                                    functions.col("value")).otherwise(0)).as("viewsSum"))
                    .observe("window", functions.count(functions.lit(1)).cast("double").as("rows"))
                    .withColumn("ctrValue", functions.col("clicksSum").divide(functions.col("viewsSum")))
                    .filter(functions.col("clicksSum").leq(functions.col("viewsSum")));

            // Stage [6]: sink, mirrors RollingCTR.toString().
            formatted = rollingCtr.selectExpr(
                    "CAST(concat('RollingCTR{queryId=', queryId, ', adId=', adId, ', clicks=', " +
                            "CAST(clicksSum AS STRING), ', impressions=', CAST(viewsSum AS STRING), " +
                            "', ctrValue=', CAST(ctrValue AS STRING), '}') AS STRING) AS value")
                    .repartition(sinkPar)
                    .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));

        } else if (query == Constants.QUERY_TWO) {
            int parserPar = Integer.parseInt(pd[1]);
            int counterPar = Integer.parseInt(pd[2]);
            int avgPar = Integer.parseInt(pd[3]);
            sinkPar = Integer.parseInt(pd[4]);
            spark.conf().set("spark.sql.shuffle.partitions", avgPar);

            // Per-stage row counts + end-to-end lag (Tier 3 operator metrics
            // -- see MetricsUtils / WordCountJob.java for the full explanation).
            MetricsUtils.registerOperatorMetrics(spark, "parser", "counter", "rollingCTR", "sink", "end_to_end_lag");

            Dataset<Row> source = spark.readStream().format("kafka")
                    .option("kafka.bootstrap.servers", kafkaServer)
                    .option("subscribe", input)
                    .option("startingOffsets", "latest").load();

            // End-to-end lag: see WordCountJob.java for why current_timestamp()
            // must be materialized into its own column before being observed.
            Dataset<Row> sourceLagColumn = source.withColumn("lag_seconds",
                    functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
            Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                    functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

            // Stage [1]: parser, mirrors Q2AdEventParser.java
            // (clicks=record[0], views=record[1], adId=record[3], queryId=record[4]).
            StructType q2Schema = new StructType()
                    .add("queryId", "long").add("adId", "long")
                    .add("clicks", "integer").add("views", "integer");
            Dataset<Row> parsed = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                    .as(Encoders.STRING())
                    .map((org.apache.spark.api.java.function.MapFunction<String, Row>) line -> {
                        String[] r = line.trim().split("\\s+");
                        return RowFactory.create(Long.parseLong(r[4]), Long.parseLong(r[3]),
                                Integer.parseInt(r[0]), Integer.parseInt(r[1]));
                    }, Encoders.row(q2Schema))
                    .repartition(parserPar)
                    .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [2]: counter -- stateless per-event CTR (see class javadoc).
            Dataset<Row> withCtr = parsed
                    .withColumn("ctrValue", functions.col("clicks").divide(
                            functions.when(functions.col("views").equalTo(0), 1).otherwise(functions.col("views"))))
                    .repartition(counterPar, functions.col("queryId"), functions.col("adId"))
                    .observe("counter", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [3]: rollingCTR -- sliding window average CTR per (queryId, adId).
            Dataset<Row> rollingCtr = withCtr
                    .withColumn("event_time", functions.current_timestamp())
                    .groupBy(
                            functions.window(functions.col("event_time"), windowDuration, slideDuration),
                            functions.col("queryId"), functions.col("adId"))
                    .agg(functions.avg("ctrValue").as("rollingCtr"))
                    .observe("rollingCTR", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [4]: sink, mirrors RollingCTR.toString() (clicks/impressions are 0
            // in this query's output, matching Q2RollingAverage.java exactly).
            formatted = rollingCtr.selectExpr(
                    "CAST(concat('RollingCTR{queryId=', queryId, ', adId=', adId, ', clicks=0, " +
                            "impressions=0, ctrValue=', CAST(rollingCtr AS STRING), '}') AS STRING) AS value")
                    .repartition(sinkPar)
                    .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));
        } else {
            throw new IllegalArgumentException("The only supported queries are \"1\" and \"2\".");
        }

        String checkpointLocation = "/tmp/spark-checkpoints/adsanalytics-" + System.currentTimeMillis();

        StreamingQuery streamingQuery = formatted.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("topic", output)
                .option("checkpointLocation", checkpointLocation)
                .outputMode(finalOutputMode)
                .start();

        streamingQuery.awaitTermination(secondsToWait * 1000);
        streamingQuery.stop();
    }
}
