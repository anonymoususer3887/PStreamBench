package com.kom.dsp.TPCH;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Collections;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

public class TPCH {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int filterPar = Integer.parseInt(pd[2]);
        int flatMapPar = Integer.parseInt(pd[3]);
        int counterPar = Integer.parseInt(pd[4]);
        int sinkPar = Integer.parseInt(pd[5]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int query = Integer.parseInt(params.get("query"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark TPC-H job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", counterPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "filter", "flatmapper", "counter", "sink",
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

        // Stage [1]: tpch-event-parser, mirrors TPCHEventParser.java
        // (orderKey, cname, caddress, orderPriority, extendedPrice, discount);
        // malformed lines (!= 6 fields) are dropped rather than emitting a
        // default-valued TPCHEvent, which would otherwise spuriously pass
        // the orderPriority > 2 filter below (default int is 0).
        Dataset<Row> parsed = lines
                .map((org.apache.spark.api.java.function.MapFunction<String, Row>) line -> {
                    String[] f = line.split(",");
                    if (f.length != 6) {
                        return null;
                    }
                    return org.apache.spark.sql.RowFactory.create(
                            Integer.parseInt(f[3]), Double.parseDouble(f[5]));
                }, Encoders.row(new org.apache.spark.sql.types.StructType()
                        .add("orderPriority", "integer")
                        .add("discount", "double")))
                .filter(functions.col("orderPriority").isNotNull())
                .repartition(parserPar)
                .observe("parser", count(lit(1)).cast("double").as("rows"));

        // Stage [2]: tpch-data-filter.
        Dataset<Row> filtered;
        if (query == Constants.QUERY_TWO) {
            filtered = parsed
                    .repartition(filterPar, functions.col("discount"))
                    .filter(functions.col("orderPriority").gt(2));
        } else {
            filtered = parsed
                    .filter(functions.col("orderPriority").gt(2))
                    .repartition(filterPar);
        }
        filtered = filtered.observe("filter", count(lit(1)).cast("double").as("rows"));

        // Stage [3]: tpch-flat-mapper -- (orderPriority, 1).
        Dataset<Row> mapped = filtered
                .selectExpr("orderPriority")
                .repartition(flatMapPar)
                .observe("flatmapper", count(lit(1)).cast("double").as("rows"));

        // Stage [4]: priority-counter -- running count of orders per priority.
        Dataset<Row> counts = mapped
                .groupBy(functions.col("orderPriority"))
                .count()
                .observe("counter", count(lit(1)).cast("double").as("rows"));

        // Stage [5]: sink, mirrors TPCHSerializationSchema.java's format.
        Dataset<Row> formatted = counts
                .selectExpr("CAST(concat('orderPriority : ', orderPriority, ' Number of orders: ', CAST(count AS STRING)) AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", count(lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/tpch-" + System.currentTimeMillis();

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
