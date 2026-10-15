package com.kom.dsp.LinearRoad;

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
 * Kafka Streams counterpart of flink/dsp_flink_jobs/.../LinearRoad/LinearRoad.java
 * and spark/dsp_spark_jobs/.../LinearRoad/LinearRoad.java.
 *
 * Queries 1, 3, and 4 are per-vehicleId keyed state machines (toll
 * notification, daily expenditure, vehicle report) -- TollNotificationProcessor
 * / DailyExpenditureProcessor / VehicleReportProcessor are this port's
 * equivalent of Spark's flatMapGroupsWithState / Flink's keyed RichMapFunction
 * (see each class's javadoc for its specific state-reduction reasoning).
 * Query 2 (accident detection) has no per-vehicle state in either reference
 * implementation -- it flags any group of events sharing (segment, lane,
 * position) within a sliding window as a collision, which maps directly
 * onto a plain windowed groupBy/aggregate, same as the Spark port.
 *
 * Window size/slide for query 2 are in *seconds*, not milliseconds -- this
 * matches both reference implementations exactly (Flink:
 * Time.seconds(slidingWindowSize); Spark: slidingWindowSize + " seconds"),
 * unlike every other windowed job in this module, which uses milliseconds.
 *
 * Malformed source lines (!= 15 CSV fields) are dropped rather than
 * producing a placeholder all-zero VehicleEvent (as the Spark port does,
 * since Dataset.map() can't filter) -- consistent with this port's existing
 * flatMap-and-drop convention (TPCH, Traffic Monitoring) rather than
 * introducing a phantom vehicle 0 into per-vehicle state.
 *
 * Stage shape query 1/2: [0] source, [1] mapper, [2] formatter, [3] sink.
 * Stage shape query 3/4: [0] source, [1] mapper, [2] sink (no separate
 * formatter -- matches Flink/Spark, whose mapper stage already emits fully
 * formatted strings with no further transformation needed).
 */
public class LinearRoadTopology {

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
                    "Kafka Streams Linear Road job currently only supports mode=KAFKA");
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

        // Stage [source]: mirrors VehicleEventParser.java -- 15 CSV fields,
        // only the first 9 (type,time,vehicleId,speed,xway,lane,direction,
        // segment,position) are read anywhere downstream; malformed lines
        // dropped (see class javadoc).
        KStream<String, String> events = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"))
                .flatMap((key, line) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    String[] f = line.split(",");
                    if (f.length == 15) {
                        String encoded = String.join(",", f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]);
                        out.add(new KeyValue<>(null, encoded));
                    }
                    return out;
                });

        KStream<String, String> formatted;

        if (query == Constants.QUERY_ONE) {
            KStream<String, String> notifications = events
                    .selectKey((key, value) -> value.split(",")[2])
                    .process(TollNotificationProcessor.supplier());
            formatted = notifications.process(OperatorMetricsProcessor.supplier("formatter"));

        } else if (query == Constants.QUERY_TWO) {
            // Stage [mapper]: events sharing (segment, lane, position) within
            // the same window indicate a collision.
            KTable<Windowed<String>, String> aggregated = events
                    .filter((key, value) -> Integer.parseInt(value.split(",")[7]) != -1)
                    .selectKey((key, value) -> {
                        String[] f = value.split(",");
                        return f[7] + "," + f[5] + "," + f[8];
                    })
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                            WindowConfig.Unit.SECONDS, graceMsArg, "LinearRoad/Q2"))
                    .aggregate(
                            () -> "0,0",
                            (key, value, agg) -> {
                                long time = Long.parseLong(value.split(",")[1]);
                                String[] accParts = agg.split(",");
                                long count = Long.parseLong(accParts[0]) + 1;
                                long maxTime = Math.max(Long.parseLong(accParts[1]), time);
                                return count + "," + maxTime;
                            },
                            Materialized.with(Serdes.String(), Serdes.String()));

            KStream<Windowed<String>, String> mapperOut = aggregated.toStream()
                    .filter((windowedKey, acc) -> Long.parseLong(acc.split(",")[0]) > 1)
                    .process(OperatorMetricsProcessor.supplier("mapper"));

            // Stage [formatter], mirrors AccidentNotificationFormatter.java.
            formatted = mapperOut
                    .process(OperatorMetricsProcessor.supplier("formatter"))
                    .map((windowedKey, acc) -> {
                        String[] accParts = acc.split(",");
                        String[] keyParts = windowedKey.key().split(",");
                        String out = "Accident occurred at ---Segment:" + keyParts[0] + " lane: " + keyParts[1]
                                + " position: " + keyParts[2] + " timestamp: " + accParts[1];
                        return new KeyValue<>(windowedKey.key(), out);
                    });

        } else if (query == Constants.QUERY_THREE) {
            formatted = events
                    .selectKey((key, value) -> value.split(",")[2])
                    .process(DailyExpenditureProcessor.supplier());

        } else if (query == 4) {
            formatted = events
                    .selectKey((key, value) -> value.split(",")[2])
                    .process(VehicleReportProcessor.supplier());

        } else {
            throw new IllegalArgumentException("The only supported queries are \"1\", \"2\", \"3\", and \"4\".");
        }

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

    // Mirrors TollNotificationMapper.java / DailyExpenditureCalculator.java
    // exactly, including passing segmentId (not a speed) as "averageSpeed" --
    // that is what the reference implementations actually do, not a
    // translation slip (see class javadoc).
    static int calculateToll(double averageSpeed, int numVehicles) {
        int toll = 2;
        if (averageSpeed >= 20) {
            toll = (int) ((averageSpeed * numVehicles) / 3);
        }
        return toll;
    }
}
