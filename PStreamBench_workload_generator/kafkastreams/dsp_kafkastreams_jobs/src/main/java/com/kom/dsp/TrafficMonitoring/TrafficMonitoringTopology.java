package com.kom.dsp.TrafficMonitoring;

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

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../TrafficMonitoring/TrafficMonitoring.java (query 1
 * only, matching what the Flink job itself implements) and
 * spark/dsp_spark_jobs/.../TrafficMonitoring/TrafficMonitoring.java.
 *
 * Helper.java's map-matching (exact lookup of rounded lat/lon against a
 * GeoJSON road map fetched from a fixed GitLab URL) is plain Java + Gson,
 * reused unchanged; loaded once per JVM here (lazily, on first use), the
 * same "once per parallel task, not once per record" granularity as the
 * Flink/Spark ports. Only lat, lon, speed matter downstream -- vehicleId/
 * timestamp/bearing are parsed by TrafficEventParser.java but never read
 * further.
 *
 * Stage shape: [0] source, [1] parser, [2] roadmatcher, [3] avgspeed,
 * [4] sink -- same as Flink/Spark.
 */
public class TrafficMonitoringTopology {

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
                    "Kafka Streams Traffic Monitoring job currently only supports mode=KAFKA");
        }
        if (query != Constants.QUERY_ONE) {
            throw new IllegalArgumentException("Only query 1 is implemented (matches Flink/Spark).");
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

        // Stage [parser]: mirrors TrafficEventParser.java's field layout
        // (lat=f[3], lon=f[4], speed=f[5]); malformed lines (!= 7 fields)
        // dropped.
        KStream<String, String> parsed = source
                .flatMap((key, line) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    String[] f = line.split(",");
                    if (f.length == 7) {
                        double lat = Double.parseDouble(f[3]);
                        double lon = Double.parseDouble(f[4]);
                        double speed = Double.parseDouble(f[5]);
                        out.add(new KeyValue<>(null, lat + "," + lon + "," + speed));
                    }
                    return out;
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [roadmatcher]: exact rounded lat/lon lookup against
        // Helper.roadMap, dropping unmatched points.
        KStream<String, String> matched = parsed
                .flatMap((key, value) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    Helper.ensureLoaded();
                    String[] parts = value.split(",");
                    DecimalFormat df = new DecimalFormat("#.##");
                    double lat = Double.parseDouble(df.format(Double.parseDouble(parts[0])));
                    double lon = Double.parseDouble(df.format(Double.parseDouble(parts[1])));
                    double speed = Double.parseDouble(parts[2]);
                    Long roadId = null;
                    for (Map.Entry<Long, Double[][]> entry : Helper.roadMap.entrySet()) {
                        for (Double[] coord : entry.getValue()) {
                            if (coord[0].equals(lon) && coord[1].equals(lat)) {
                                roadId = entry.getKey();
                                break;
                            }
                        }
                        if (roadId != null) {
                            break;
                        }
                    }
                    if (roadId != null) {
                        out.add(new KeyValue<>(String.valueOf(roadId), String.valueOf(speed)));
                    }
                    return out;
                })
                .process(OperatorMetricsProcessor.supplier("roadmatcher"));

        // Stage [avgspeed]: hopping-window average speed per road, dropping
        // zero-average windows.
        KTable<Windowed<String>, String> aggregated = matched
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                        WindowConfig.Unit.MILLIS, graceMsArg, "TrafficMonitoring"))
                .aggregate(
                        () -> "0.0,0",
                        (roadId, speedStr, agg) -> {
                            double speed = Double.parseDouble(speedStr);
                            String[] accParts = agg.split(",");
                            double sum = Double.parseDouble(accParts[0]) + speed;
                            long count = Long.parseLong(accParts[1]) + 1;
                            return sum + "," + count;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        KStream<String, String> avgSpeed = aggregated.toStream()
                .process(OperatorMetricsProcessor.supplier("avgspeed"))
                .flatMap((windowedKey, acc) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    String[] accParts = acc.split(",");
                    double avg = Double.parseDouble(accParts[0]) / Double.parseDouble(accParts[1]);
                    if (avg != 0) {
                        out.add(new KeyValue<>(windowedKey.key(), windowedKey.key() + "," + avg));
                    }
                    return out;
                });

        // Stage [sink], mirrors TrafficMonitoringSerializationSchema.java's
        // "roadId,avgSpeed" format (already produced above).
        avgSpeed
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
