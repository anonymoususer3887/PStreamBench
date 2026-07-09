package com.kom.dsp.SpikeDetection;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public class SpikeDetection {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);
        int avgPar = Integer.parseInt(pd[2]);
        int filterPar = Integer.parseInt(pd[3]);
        int sinkPar = Integer.parseInt(pd[4]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Spike Detection job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", avgPar);

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "average", "spikedetector", "sink",
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
                functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourcePar)
                .observe("source", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [1]: parser, mirrors SensorParser.java's field layout
        // (date, time, epoch, moteid, temperature, humidity, light, voltage)
        // -- only moteid/voltage are used further down the pipeline. Lines
        // with fewer than 8 fields (sensor dropouts, seen in the real Intel
        // Lab data) are dropped rather than indexed blindly. moteid is
        // parsed via Double.parseDouble/truncation rather than
        // Integer.parseInt: the producer's pandas re-serialization promotes
        // the moteid column to float64 (since other rows elsewhere in the
        // file have a missing moteid, forcing the whole column to float to
        // hold NaN), so moteid arrives on the wire as e.g. "1.0", not "1".
        StructType sensorSchema = new StructType()
                .add("sensorId", "integer").add("voltage", "float");
        Dataset<Row> parsed = lines
                .map((MapFunction<String, Row>) line -> {
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length < 8) {
                        return RowFactory.create(new Object[]{null, null});
                    }
                    try {
                        int sensorId = (int) Double.parseDouble(fields[3]);
                        float voltage = Float.parseFloat(fields[7]);
                        return RowFactory.create(sensorId, voltage);
                    } catch (NumberFormatException e) {
                        return RowFactory.create(new Object[]{null, null});
                    }
                }, Encoders.row(sensorSchema))
                .filter(functions.col("sensorId").isNotNull())
                .repartition(parserPar)
                .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [2]: average-calculator, sliding processing-time window
        // average voltage per sensor (also tracks max voltage in the same
        // window, needed for the spike condition below).
        String windowDuration = slidingWindowSize + " milliseconds";
        String slideDuration = slidingWindowSlide + " milliseconds";
        Dataset<Row> windowed = parsed
                .withColumn("event_time", functions.current_timestamp())
                .groupBy(
                        functions.window(functions.col("event_time"), windowDuration, slideDuration),
                        functions.col("sensorId"))
                .agg(
                        functions.avg("voltage").as("averageValue"),
                        functions.max("voltage").as("currentValue"))
                .observe("average", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [3]: spike-detector -- a window "spikes" when its peak
        // voltage exceeds its own average voltage.
        Dataset<Row> spikes = windowed
                .filter(functions.col("currentValue").gt(functions.col("averageValue")))
                .repartition(filterPar)
                .observe("spikedetector", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [4]: sink.
        Dataset<Row> formatted = spikes
                .selectExpr("CAST(concat('AverageValue{sensorId=', sensorId, ', currentValue=', " +
                        "CAST(currentValue AS STRING), ', averageValue=', CAST(averageValue AS STRING), '}') AS STRING) AS value")
                .repartition(sinkPar)
                .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));

        String checkpointLocation = "/tmp/spark-checkpoints/spikedetection-" + System.currentTimeMillis();

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
