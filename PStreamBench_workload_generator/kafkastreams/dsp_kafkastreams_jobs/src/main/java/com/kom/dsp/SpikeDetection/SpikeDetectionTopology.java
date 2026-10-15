package com.kom.dsp.SpikeDetection;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.OperatorMetricsProcessor;
import com.kom.dsp.utils.WindowConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../SpikeDetection/SpikeDetection.java and
 * spark/dsp_spark_jobs/.../SpikeDetection/SpikeDetection.java.
 *
 * Same window-granularity simplification the Spark port documents in
 * detail: Flink's AverageValueCalculator emits one row per input
 * measurement paired with the just-computed per-key window average, then
 * filters on that single measurement exceeding the average. A windowed
 * aggregation naturally collapses to one row per (window, key), so this
 * implements the same spike condition -- peak voltage in a window
 * exceeding the window's own average voltage -- at window granularity.
 * Only sensorId/voltage are needed downstream (fields[3]/fields[7] of the
 * real 8-field Intel Berkeley Research Lab format; lines under 8 fields,
 * i.e. sensor dropouts, are skipped -- see the Spark port's javadoc for the
 * full data-quality story that motivated this exact parsing).
 *
 * Stage shape: [0] source, [1] parser, [2] average, [3] spikedetector,
 * [4] sink -- same as Flink/Spark.
 */
public class SpikeDetectionTopology {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        int streamThreads = Integer.parseInt(parallelism.split(",")[1]); // operator (swept) parallelism, matching Flink/Storm/Spark sweep vector shape

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        String applicationId = params.get("application-id");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        long graceMsArg = Long.parseLong(params.get("graceMs", "-1"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams Spike Detection job currently only supports mode=KAFKA");
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        // Stage [source].
        KStream<String, String> source = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"));

        // Stage [parser]: mirrors SensorParser.java's field layout
        // (moteid=fields[3], voltage=fields[7]); dropout lines (<8 fields)
        // and unparseable ones are dropped, not indexed blindly.
        KStream<String, String> parsed = source
                .flatMap((key, line) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length >= 8) {
                        try {
                            int sensorId = (int) Double.parseDouble(fields[3]);
                            float voltage = Float.parseFloat(fields[7]);
                            out.add(new KeyValue<>(String.valueOf(sensorId), voltage + ""));
                        } catch (NumberFormatException e) {
                            // dropped, same as the Spark port's null-row filter
                        }
                    }
                    return out;
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [average]: hopping-window (sum, count, max) voltage per
        // sensor -- accumulator string is "sum,count,max".
        KTable<Windowed<String>, String> aggregated = parsed
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                        WindowConfig.Unit.MILLIS, graceMsArg, "SpikeDetection"))
                .aggregate(
                        () -> "0.0,0,-Infinity",
                        (sensorId, voltageStr, agg) -> {
                            double voltage = Double.parseDouble(voltageStr);
                            String[] accParts = agg.split(",");
                            double sum = Double.parseDouble(accParts[0]) + voltage;
                            long count = Long.parseLong(accParts[1]) + 1;
                            double max = Math.max(Double.parseDouble(accParts[2]), voltage);
                            return sum + "," + count + "," + max;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        KStream<String, String> windowed = aggregated.toStream()
                .process(OperatorMetricsProcessor.supplier("average"))
                .map((windowedKey, acc) -> {
                    String[] parts = acc.split(",");
                    double sum = Double.parseDouble(parts[0]);
                    double count = Double.parseDouble(parts[1]);
                    double max = Double.parseDouble(parts[2]);
                    double averageValue = sum / count;
                    return new KeyValue<>(windowedKey.key(), max + "," + averageValue);
                });

        // Stage [spikedetector]: a window "spikes" when its peak voltage
        // exceeds its own average voltage.
        KStream<String, String> spikes = windowed
                .filter((sensorId, value) -> {
                    String[] parts = value.split(",");
                    return Double.parseDouble(parts[0]) > Double.parseDouble(parts[1]);
                })
                .process(OperatorMetricsProcessor.supplier("spikedetector"));

        // Stage [sink].
        KStream<String, String> formatted = spikes
                .map((sensorId, value) -> {
                    String[] parts = value.split(",");
                    String line = "AverageValue{sensorId=" + sensorId + ", currentValue=" + parts[0]
                            + ", averageValue=" + parts[1] + "}";
                    return new KeyValue<>(sensorId, line);
                });

        formatted
                .process(OperatorMetricsProcessor.supplier("sink"))
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            streams.cleanUp();
            latch.countDown();
        }));

        System.out.println("[main] Starting Kafka Streams, application.id=" + applicationId);
        streams.start();

        System.out.println("[main] Will run for " + secondsToWait + " seconds.");
        Thread.sleep(secondsToWait * 1000);
        streams.close();
        // Wipes this run's local RocksDB state store (/tmp/kafka-streams/<application.id>/) -- application.id is uniquely
        // timestamped per submission (see runner.py's _run_kafkastreams_job()),
        // so nothing is ever restarted/recovered from this state; leaving it on disk
        // is pure leak. Confirmed live (2026-09-24): unremoved state from thousands of
        // prior submissions across this project's strategy sweep filled every node's
        // root filesystem to 100%, silently failing every subsequent job (Ansible
        // couldn't even create its own tmp dir) regardless of workload or parallelism.
        streams.cleanUp();
        System.out.println("[main] Streams closed.");
    }
}
