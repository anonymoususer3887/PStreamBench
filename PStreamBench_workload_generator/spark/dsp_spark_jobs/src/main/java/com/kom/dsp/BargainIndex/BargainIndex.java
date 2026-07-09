package com.kom.dsp.BargainIndex;

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

public class BargainIndex {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int vwapPar = Integer.parseInt(pd[2]);
        int bargainPar = Integer.parseInt(pd[3]);
        int sinkPar = Integer.parseInt(pd[4]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int popularityThreshold = Integer.parseInt(params.get("popularityThreshold"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Bargain Index job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", vwapPar);

        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "vwap", "bargain", "sink",
                "end_to_end_lag");

        Dataset<Row> source = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", input)
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> sourceLagColumn = source.withColumn("lag_seconds",
                functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourcePar)
                .observe("source", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [1]: quote-parser, mirrors QuoteDataParser.java's field layout
        // (symbol=fields[0], close=fields[5], volume=fields[7]).
        Dataset<Row> parsed = lines
                .map((MapFunction<String, scala.Tuple3<String, Double, Long>>) line -> {
                    String[] fields = line.split(",");
                    String symbol = fields[0];
                    double close = Double.parseDouble(fields[5]);
                    long volume = Long.parseLong(fields[7]);
                    return new scala.Tuple3<>(symbol, close, volume);
                }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE(), Encoders.LONG()))
                .toDF("symbol", "close", "volume")
                .repartition(parserPar)
                .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [2]: VWAP-operator -- vwap = sum(close*volume)/sum(volume),
        // totalVolume = sum(volume), quotePrice = last close seen in the window.
        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";
        Dataset<Row> vwap = parsed
                .withColumn("event_time", functions.current_timestamp())
                .withColumn("priceVolume", functions.col("close").multiply(functions.col("volume")))
                .groupBy(
                        functions.window(functions.col("event_time"), windowDuration, slideDuration),
                        functions.col("symbol"))
                .agg(
                        functions.sum("priceVolume").divide(functions.sum("volume")).as("vwap"),
                        functions.sum("volume").as("totalVolume"),
                        functions.last("close").as("quotePrice"))
                .observe("vwap", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [3]: bargain-index-calculator.
        Dataset<Row> bargains = vwap
                .withColumn("bargainIndex",
                        functions.when(functions.col("quotePrice").gt(functions.col("vwap")),
                                functions.col("quotePrice").divide(functions.col("vwap")))
                                .otherwise(functions.lit(0.0)))
                .filter(functions.col("bargainIndex").gt(popularityThreshold))
                .repartition(bargainPar)
                .observe("bargain", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [4]: sink, mirrors BargainSerializationSchema.java's
        // "symbol,bargainIndex,totalVolume" format.
        Dataset<Row> formatted = bargains
                .selectExpr("CAST(concat(symbol, ',', CAST(bargainIndex AS STRING), ',', CAST(totalVolume AS STRING)) AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/bargainindex-" + System.currentTimeMillis();

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
