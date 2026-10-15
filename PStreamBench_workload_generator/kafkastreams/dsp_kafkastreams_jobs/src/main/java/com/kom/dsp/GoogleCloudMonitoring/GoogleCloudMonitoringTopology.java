package com.kom.dsp.GoogleCloudMonitoring;

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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../GoogleCloudMonitoring/GoogleCloudMonitoring.java
 * and spark/dsp_spark_jobs/.../GoogleCloudMonitoring/GoogleCloudMonitoring.java.
 *
 * Only jobId/category/cpu are needed downstream (TaskEventParser.java also
 * parses taskId/machineId/eventType/userId/priority/ram/disk/constraints,
 * none of which either query reads), so only those three fields are
 * extracted here, same simplification the Spark port already made. Query 1
 * sums cpu per category per window, query 2 averages cpu per jobId per
 * window -- both share one "sum,count,minTimestamp" accumulator (query 1
 * just uses sum directly, query 2 divides by count), same pattern as
 * BargainIndexTopology's VWAP stage. minTimestamp is stamped with
 * System.currentTimeMillis() when each record is aggregated -- the Kafka
 * Streams Aggregator interface has no access to the record's own
 * timestamp, and Flink/Spark's own "window start" semantics here are
 * themselves just "roughly when this window's traffic began," not an
 * authoritative per-record timestamp, so wall-clock-at-aggregation-time is
 * an equivalent approximation.
 *
 * Stage shape: [0] source, [1] parser, [2] window, [3] sink -- same as
 * Flink/Spark for both queries.
 */
public class GoogleCloudMonitoringTopology {

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
                    "Kafka Streams Google Cloud Monitoring job currently only supports mode=KAFKA");
        }
        if (query != Constants.QUERY_ONE && query != Constants.QUERY_TWO) {
            throw new IllegalArgumentException("The only supported queries are \"1\" and \"2\".");
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

        // Stage [parser]: mirrors TaskEventParser.java's field layout
        // (jobId=v[0], category=v[3], cpu=v[8]); re-keyed by whichever
        // field this query groups on.
        KStream<String, String> parsed = source
                .map((key, line) -> {
                    String[] v = line.split(",");
                    String jobId = v[0];
                    String category = v[3];
                    double cpu = Float.parseFloat(v[8]);
                    String groupKey = (query == Constants.QUERY_TWO) ? jobId : category;
                    return new KeyValue<>(groupKey, String.valueOf(cpu));
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [window]: hopping-window sum (query 1) / avg (query 2) of
        // cpu per key, plus the window's minTimestamp.
        KTable<Windowed<String>, String> aggregated = parsed
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                        WindowConfig.Unit.MILLIS, graceMsArg, "GoogleCloudMonitoring"))
                .aggregate(
                        () -> "0.0,0," + Long.MAX_VALUE,
                        (key, cpuStr, agg) -> {
                            double cpu = Double.parseDouble(cpuStr);
                            String[] accParts = agg.split(",");
                            double sum = Double.parseDouble(accParts[0]) + cpu;
                            long count = Long.parseLong(accParts[1]) + 1;
                            long minTs = Math.min(Long.parseLong(accParts[2]), System.currentTimeMillis());
                            return sum + "," + count + "," + minTs;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        // Stage [sink], mirrors CPUPerCatgory.toString() / CPUPerJob.toString().
        String typeName = (query == Constants.QUERY_TWO) ? "CPUPerJob" : "CPUPerCatgory";
        String keyLabel = (query == Constants.QUERY_TWO) ? "jobId" : "category";
        String valueLabel = (query == Constants.QUERY_TWO) ? "averageCpu" : "totalCpu";

        KStream<String, String> formatted = aggregated.toStream()
                .process(OperatorMetricsProcessor.supplier("window"))
                .map((windowedKey, acc) -> {
                    String[] parts = acc.split(",");
                    double sum = Double.parseDouble(parts[0]);
                    double count = Double.parseDouble(parts[1]);
                    long minTimestampMs = Long.parseLong(parts[2]);
                    double totalCpu = (query == Constants.QUERY_TWO) ? (sum / count) : sum;
                    long minTimestampSec = minTimestampMs / 1000;
                    String line = typeName + "{timestamp=" + minTimestampSec + ", " + keyLabel + "="
                            + windowedKey.key() + ", " + valueLabel + "=" + totalCpu + "}";
                    return new KeyValue<>(windowedKey.key(), line);
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
