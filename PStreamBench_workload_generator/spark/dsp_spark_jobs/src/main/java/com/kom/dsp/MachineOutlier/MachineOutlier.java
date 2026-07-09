package com.kom.dsp.MachineOutlier;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.ArrayList;
import java.util.List;

public class MachineOutlier {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int outlierPar = Integer.parseInt(pd[2]);
        int sinkPar = Integer.parseInt(pd[4]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int query = Integer.parseInt(params.get("query"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Machine Outlier job currently only supports mode=KAFKA");
        }
        if (query != Constants.QUERY_ONE) {
            throw new IllegalArgumentException("Only query 1 is implemented (matches the Flink source).");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", outlierPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "outlier", "sink", "end_to_end_lag");

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

        // Stage [1]: machine-usage-parser, mirrors MachineUsageParser.java
        // (memBW/missPerThousandInstructions default to 0 if fields 4/5 are blank).
        Dataset<MachineUsage> parsed = lines
                .map((MapFunction<String, MachineUsage>) s -> {
                    String[] v = s.split(",", -1);
                    double memBW = v[4].isEmpty() ? 0 : Double.parseDouble(v[4]);
                    double mkpi = v[5].isEmpty() ? 0 : Double.parseDouble(v[5]);
                    return new MachineUsage(v[0], Double.parseDouble(v[1]), Double.parseDouble(v[2]),
                            Double.parseDouble(v[3]), memBW, mkpi,
                            Double.parseDouble(v[6]), Double.parseDouble(v[7]), Double.parseDouble(v[8]));
                }, Encoders.bean(MachineUsage.class))
                .repartition(parserPar)
                .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [2]: outlier-detector.
        KeyValueGroupedDataset<String, MachineUsage> grouped =
                parsed.groupByKey((MapFunction<MachineUsage, String>) MachineUsage::getMachineId, Encoders.STRING());

        Dataset<String> outliers = grouped.flatMapGroupsWithState(
                (FlatMapGroupsWithStateFunction<String, MachineUsage, OutlierState, String>) (key, values, state) -> {
                    OutlierState s = state.exists() ? state.get() : new OutlierState();
                    while (values.hasNext()) {
                        s.usages.add(values.next());
                        if (s.usages.size() > 4) {
                            s.usages.remove(0);
                        }
                    }
                    List<String> results = new ArrayList<>();
                    if (s.usages.size() >= 2) {
                        double threshold = 0.5;
                        MachineUsage[] arr = s.usages.toArray(new MachineUsage[0]);
                        int k = (int) Math.ceil(arr.length * 0.75);
                        MachineUsage kth = BFPRTAlgorithm.bfprt(arr.clone(), 0, arr.length - 1, k);
                        for (MachineUsage m : arr) {
                            if (m.getEuclideanDistance(kth) > threshold) {
                                results.add(m.getMachineId() + "," + m.getTimestamp() + "," +
                                        m.getCpuUtilPercentage() + "," + m.getMemUtilPercentage() + "," +
                                        m.getMissPerThousandInstructions() + "," +
                                        m.getNetIn() + "," + m.getNetOut() + "," + m.getDiskIO());
                            }
                        }
                    }
                    state.update(s);
                    return results.iterator();
                },
                OutputMode.Update(),
                Encoders.javaSerialization(OutlierState.class),
                Encoders.STRING(),
                GroupStateTimeout.NoTimeout())
                .observe("outlier", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [4]: sink.
        Dataset<Row> formatted = outliers.toDF("value").repartition(sinkPar)
                .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/machineoutlier-" + System.currentTimeMillis();

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
