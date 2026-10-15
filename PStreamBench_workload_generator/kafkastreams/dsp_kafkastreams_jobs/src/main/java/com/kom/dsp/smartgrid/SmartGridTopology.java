package com.kom.dsp.smartgrid;

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
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../smartgrid/SmartGridJob.java and
 * spark/dsp_spark_jobs/.../smartgrid/SmartGridJob.java.
 *
 * Same getter-bug fix the Spark port documents: Flink's
 * HouseEvent.getValue() actually returns the index field, not the energy
 * value (setValue()/getValue() disagree there) -- this uses the real
 * energy-consumption value, not that bug, since replicating it serves no
 * benchmarking purpose. Query 2's window is in *seconds* (not milliseconds
 * like every other windowed job/query in this project) -- preserved here
 * since the Flink source does it deliberately per-query, not by accident.
 *
 * Query 1 (global average load) needs a per-window *median*, which Kafka
 * Streams has no built-in aggregator for (same reason Spark needed
 * percentile(value, 0.5) rather than a plain avg()); implemented the same
 * way as the accumulator pattern used elsewhere in this port: the window's
 * raw values are collected as a comma-separated string, then sorted and
 * linearly interpolated at the midpoint when the window fires -- windows
 * here are small (a few hundred ms of one workload's traffic), so this is
 * a correct, simple approach rather than a streaming-median approximation.
 * Query 2 (local average load) is a plain avg, same "sum,count" accumulator
 * pattern as BargainIndexTopology's VWAP stage.
 *
 * Stage shape: [0] source, [1] parser, [2] window, [3] sink -- same as
 * Flink/Spark (parallelism_degree index for sink differs by query in the
 * reference implementations' own CLI contract, but has no Kafka Streams
 * equivalent -- see Part A.2 of the plan doc).
 */
public class SmartGridTopology {

    private static double computeMedian(String csvValues) {
        String[] parts = csvValues.split(",");
        double[] values = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            values[i] = Double.parseDouble(parts[i]);
        }
        Arrays.sort(values);
        int n = values.length;
        double index = 0.5 * (n - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) {
            return values[lower];
        }
        double fraction = index - lower;
        return values[lower] + (values[upper] - values[lower]) * fraction;
    }

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
        int query = Integer.parseInt(params.get("query"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        long graceMsArg = Long.parseLong(params.get("graceMs", "-1"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams Smart Grid job currently only supports mode=KAFKA");
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

        // Stage [parser]: mirrors HouseEventParser.java (value=v[2],
        // plugs=v[4], households=v[5], house=v[6]); re-keyed per query
        // below.
        KStream<String, String> parsed = source
                .map((key, line) -> {
                    String[] v = line.replace("\"", "").split(",");
                    double value = Double.parseDouble(v[2]);
                    long plugs = Long.parseLong(v[4]);
                    long households = Long.parseLong(v[5]);
                    long house = Long.parseLong(v[6]);
                    return new KeyValue<String, String>(null, value + "," + plugs + "," + households + "," + house);
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        KStream<String, String> formatted;

        if (query == Constants.QUERY_ONE) {
            // Stage [window]: global average load -- median value per house
            // per (millisecond) window.
            KStream<String, String> byHouse = parsed
                    .map((key, value) -> {
                        String[] parts = value.split(",");
                        String house = parts[3];
                        String rawValue = parts[0];
                        return new KeyValue<>(house, rawValue);
                    });

            KTable<Windowed<String>, String> aggregated = byHouse
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                            WindowConfig.Unit.MILLIS, graceMsArg, "SmartGrid/Q1"))
                    .aggregate(
                            () -> "",
                            (house, rawValue, agg) -> agg.isEmpty() ? rawValue : agg + "," + rawValue,
                            Materialized.with(Serdes.String(), Serdes.String()));

            formatted = aggregated.toStream()
                    .process(OperatorMetricsProcessor.supplier("window"))
                    .map((windowedKey, csvValues) -> {
                        double gal = computeMedian(csvValues);
                        String line = "{'House':" + windowedKey.key() + ",'GAL':" + gal + "}";
                        return new KeyValue<>(windowedKey.key(), line);
                    });
        } else {
            // Stage [window]: local average load -- avg value per
            // (house, households, plugs) per (second) window.
            KStream<String, String> byHouseHouseholdsPlugs = parsed
                    .map((key, value) -> {
                        String[] parts = value.split(",");
                        String rawValue = parts[0];
                        String plugs = parts[1];
                        String households = parts[2];
                        String house = parts[3];
                        String groupKey = house + "|" + households + "|" + plugs;
                        return new KeyValue<>(groupKey, rawValue);
                    });

            KTable<Windowed<String>, String> aggregated = byHouseHouseholdsPlugs
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                            WindowConfig.Unit.SECONDS, graceMsArg, "SmartGrid/Q2"))
                    .aggregate(
                            () -> "0.0,0",
                            (groupKey, rawValue, agg) -> {
                                double value = Double.parseDouble(rawValue);
                                String[] accParts = agg.split(",");
                                double sum = Double.parseDouble(accParts[0]) + value;
                                long count = Long.parseLong(accParts[1]) + 1;
                                return sum + "," + count;
                            },
                            Materialized.with(Serdes.String(), Serdes.String()));

            formatted = aggregated.toStream()
                    .process(OperatorMetricsProcessor.supplier("window"))
                    .map((windowedKey, acc) -> {
                        String[] accParts = acc.split(",");
                        double lal = Double.parseDouble(accParts[0]) / Double.parseDouble(accParts[1]);
                        String[] groupKeyParts = windowedKey.key().split("\\|");
                        String house = groupKeyParts[0];
                        String households = groupKeyParts[1];
                        String plugs = groupKeyParts[2];
                        String line = "{'House'" + house + ",'HouseHold'" + households
                                + ",'Plug'" + plugs + ",'LAL':" + lal + "}";
                        return new KeyValue<>(windowedKey.key(), line);
                    });
        }

        // Stage [sink].
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
