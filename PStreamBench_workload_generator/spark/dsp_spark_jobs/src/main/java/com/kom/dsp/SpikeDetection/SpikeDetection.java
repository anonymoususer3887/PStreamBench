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

/**
 * Spark Structured Streaming counterpart of
 * flink/dsp_flink_jobs/.../SpikeDetection/SpikeDetection.java.
 *
 * Simplification vs. the Flink source: AverageValueCalculator there emits
 * one output row per input measurement (pairing that single measurement's
 * voltage with the just-computed per-key window average), then filters rows
 * where the individual voltage exceeds the window average. Structured
 * Streaming's windowed aggregation collapses a window to one row per key, so
 * this implements the same spike condition (peak voltage in a window
 * exceeding the window's average voltage) at window granularity instead of
 * per-event granularity. Only sensorId and voltage are needed downstream
 * (temperature/humidity/light are parsed by SensorParser.java but never
 * used by average-calculator/spike-detector), so this job only extracts
 * those two fields. The real Intel Berkeley Research Lab sensors.dat has 8
 * space-separated fields (date, time, epoch, moteid, temperature, humidity,
 * light, voltage) -- moteid is fields[3] and voltage is fields[7], not
 * fields[2]/fields[6] -- and a non-trivial fraction of lines are truncated
 * (sensor dropouts), so lines with fewer than 8 fields are skipped rather
 * than indexed blindly.
 * Stage shape: [0] source, [1] parser, [2]
 * average-calculator, [3] spike-detector, [4] sink -- same as Flink.
 */
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
        //
        // `data.txt` (PStreamBench_controller/data.txt) is this project's
        // designated-authoritative 8-column source for this workload (per
        // explicit user direction -- see BUGS_AND_FIXES.md bug #8 from the
        // RisingWave translation), NOT the 7-column file the shared
        // download_datasets.sh/Kaggle path produces under the same
        // spikeDetection_sensors.dat filename. Confirmed live (2026-08-26):
        // the Spark cluster's copy of that file had the *wrong*, 7-column
        // Kaggle variant on it (every line missing the epoch column this
        // parser expects), which silently dropped every single line here
        // -- not a bug in this field-index logic itself. Fixed by
        // rederiving spikeDetection_sensors.dat from data.txt on the
        // cluster (matching what setup_local.sh already does for local
        // runs), not by changing these indices.
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
