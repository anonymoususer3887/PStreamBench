package com.kom.dsp.LogsAnalyzer;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.OperatorMetricsProcessor;
import com.kom.dsp.utils.WindowConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../LogsAnalyzer/LogsAnalyzer.java (5 files:
 * LogsAnalyzer/LogEvent/LogParser/VolumeCounter/StatusCounter) and
 * spark/dsp_spark_jobs/.../LogsAnalyzer/LogsAnalyzer.java.
 *
 * Same simplification the Spark port already made and documents in detail
 * (see that class's javadoc): Flink's VolumeCounter/StatusCounter use a
 * hand-rolled per-key ValueState/MapState counter that increments once per
 * input event and emits one output line per event, sharing state across
 * overlapping windows -- an artifact of manual state management in the
 * Flink Processor API, not a deliberate query semantic. The natural
 * Structured Streaming *and* Kafka Streams equivalent is a per-key
 * hopping-window COUNT aggregation, emitting one "key: count" line per
 * window firing. Kafka Streams' TimeWindows (advanceBy) is the hopping-
 * window primitive equivalent to Flink's SlidingProcessingTimeWindows and
 * Spark's functions.window(size, slide) -- true KIP-450 SlidingWindows are
 * a different (pairwise-correlation) construct not needed here.
 *
 * Query 1 keys by logTime ("volume counter"), query 2 keys by statusCode
 * ("status counter") -- same aggregation shape either way, only the key
 * field differs, matching both reference implementations.
 *
 * Stage boundaries: [0] source, [1] parser, [2] counter, [3] sink -- same
 * as Flink/Spark.
 */
public class LogsAnalyzerTopology {

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
                    "Kafka Streams Log Processing job currently only supports mode=KAFKA");
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        // Flink runs this job under TimeCharacteristic.ProcessingTime and Spark
        // windows on current_timestamp() -- neither uses the record's own
        // embedded event timestamp for windowing. WallclockTimestampExtractor
        // matches that: windows advance on wall-clock arrival time, not on
        // any timestamp carried in the record.
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        // Stage [source].
        KStream<String, String> source = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"));

        // Stage [parser]: mirrors LogParser.java exactly (logTime = parts[3]
        // minus leading '[', statusCode = parts[8]); re-keys by whichever
        // field this query variant groups on.
        KStream<String, String> parsed = source
                .map((key, line) -> {
                    String[] parts = line.split(" ");
                    String logTime = parts[3].substring(1);
                    String statusCode = parts[8];
                    String keyField = (query == Constants.QUERY_TWO) ? statusCode : logTime;
                    return new org.apache.kafka.streams.KeyValue<>(keyField, keyField);
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [counter]: hopping-window count, one "key: count" line per
        // window firing (query 1: volume by logTime, query 2: status by
        // statusCode).
        KTable<Windowed<String>, Long> counted = parsed
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                        WindowConfig.Unit.MILLIS, graceMsArg, "LogsAnalyzer"))
                .count();

        KStream<String, String> formatted = counted.toStream()
                .map((windowedKey, count) ->
                        new org.apache.kafka.streams.KeyValue<>(windowedKey.key(), windowedKey.key() + ": " + count))
                .process(OperatorMetricsProcessor.supplier("counter"));

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
