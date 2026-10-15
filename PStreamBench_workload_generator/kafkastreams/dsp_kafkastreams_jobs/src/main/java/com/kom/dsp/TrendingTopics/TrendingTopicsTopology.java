package com.kom.dsp.TrendingTopics;

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
 * flink/dsp_flink_jobs/.../TrendingTopics/TrendingTopics.java (query 1 only,
 * matching what the Flink job itself implements) and
 * spark/dsp_spark_jobs/.../TrendingTopics/TrendingTopics.java.
 *
 * Stage boundaries, matching the Spark port's convention: [0] source,
 * [1] parser (mirrors TwitterParser's CSV field extraction, text=values[5]),
 * [2] topicextractor (words starting with @ or #, symbol stripped),
 * [3] counter (hopping-window count per topic), [4] filter (popularity
 * threshold), [5] sink.
 */
public class TrendingTopicsTopology {

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
        int topicPopularityThreshold = Integer.parseInt(params.get("popularityThreshold"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams Trending Topics job currently only supports mode=KAFKA");
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

        // Stage [parser]: mirrors TwitterParser.java (id=values[1],
        // timestamp=values[2], text=values[5]) -- only text is needed
        // downstream, so that is all that is carried forward.
        KStream<String, String> tweetTexts = source
                .mapValues(line -> line.split(",")[5])
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [topicextractor]: mirrors TopicExtractor.java exactly (words
        // starting with @ or #, symbol stripped), re-keyed by topic.
        KStream<String, String> topics = tweetTexts
                .flatMap((key, text) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    for (String line : text.split("\n")) {
                        for (String word : line.split("\\s+")) {
                            if (word.startsWith("@") || word.startsWith("#")) {
                                String topic = word.substring(1);
                                out.add(new KeyValue<>(topic, topic));
                            }
                        }
                    }
                    return out;
                })
                .process(OperatorMetricsProcessor.supplier("topicextractor"));

        // Stage [counter]: hopping-window count per topic (mirrors
        // SlidingProcessingTimeWindows -- see LogsAnalyzerTopology's javadoc
        // for why WallclockTimestampExtractor + TimeWindows.advanceBy is the
        // Kafka Streams equivalent).
        KTable<Windowed<String>, Long> counted = topics
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                        WindowConfig.Unit.MILLIS, graceMsArg, "TrendingTopics"))
                .count();

        KStream<Windowed<String>, Long> countedStream = counted.toStream()
                .process(OperatorMetricsProcessor.supplier("counter"));

        // Stage [filter]: popularity-detector.
        KStream<String, String> popular = countedStream
                .filter((windowedKey, count) -> count > topicPopularityThreshold)
                .map((windowedKey, count) -> new KeyValue<>(windowedKey.key(), windowedKey.key()))
                .process(OperatorMetricsProcessor.supplier("filter"));

        // Stage [sink].
        popular
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
