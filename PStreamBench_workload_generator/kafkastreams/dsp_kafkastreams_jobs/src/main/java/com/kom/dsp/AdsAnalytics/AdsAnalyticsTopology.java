package com.kom.dsp.AdsAnalytics;

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
 * flink/dsp_flink_jobs/.../AdsAnalytics/AdsAnalytics.java and
 * spark/dsp_spark_jobs/.../AdsAnalytics/AdsAnalytics.java.
 *
 * Both reference implementations read the *same* physical event line for
 * both the "clicks" and "impressions" branches of query 1 (each line
 * carries both a clicks count at field 0 and a views count at field 1;
 * Q1ClickParser/Q1ImpressionParser -- and Spark's own field-index comments
 * -- just pick out a different field). Flink models this by subscribing
 * twice to the same KafkaSource; Spark models it with two independent
 * consumer groups so each branch sees every record. Kafka Streams needs
 * neither trick: a single source KStream naturally fans out to any number
 * of independent downstream chains, each of which sees every record, so
 * this reads the topic once and derives clicksParsed/impressionsParsed as
 * two sibling children of that one source node.
 *
 * Flink's query 1 joins clicksCounter and impressionsCounter *after* each
 * has already been keyBy+sum aggregated. Kafka Streams (like Spark
 * Structured Streaming) doesn't support joining a stream against another
 * stream/table that has already gone through a windowed aggregation, and
 * even a raw pre-aggregation stream-stream join would fan out to an N*M
 * cross product per key per window before any sum -- the same correctness
 * problem documented in the Spark port. The fix mirrors Spark's exactly:
 * tag each side with its event type, merge (KStream.merge(), the Kafka
 * Streams equivalent of Spark's unionByName) into one stream, and run a
 * single windowed aggregation with a "clicksSum,viewsSum" accumulator
 * instead of a join.
 *
 * Query 2 has no join in either reference implementation: each raw event
 * already carries both clicks and views, and Q2AdEventCounter's hand-rolled
 * running HashMap-per-instance counter is replaced with a stateless
 * per-event CTR (clicks/views) feeding a windowed average, matching the
 * simplification Spark already made.
 *
 * Stage shape query 1: [0] source, [1] clicksparser, [1] impressionsparser,
 * [2] window, [3] sink. Stage shape query 2: [0] source, [1] parser,
 * [2] counter, [3] rollingCTR, [4] sink. Same instrumented-stage names as
 * the Spark port, so a getprom_kafkastreams.py collector can follow the
 * same query-1-uses-two-parsers / query-2-is-a-plain-chain structure as
 * getprom_spark.py's Ad Analytics collector.
 */
public class AdsAnalyticsTopology {

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
                    "Kafka Streams Ad Analytics job currently only supports mode=KAFKA");
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

        KStream<String, String> formatted;

        if (query == Constants.QUERY_ONE) {
            // Stage [clicksparser]: mirrors Q1ClickParser -- queryId=f[7],
            // adId=f[3], clicks count=f[0]. Re-keys by (queryId,adId) and
            // tags the value with its event type so both branches can be
            // merged into one stream below.
            KStream<String, String> clicksParsed = source
                    .map((key, line) -> {
                        String[] f = line.split("\t");
                        String newKey = f[7] + "," + f[3];
                        return new KeyValue<>(newKey, "click," + f[0]);
                    })
                    .process(OperatorMetricsProcessor.supplier("clicksparser"));

            // Stage [impressionsparser]: mirrors Q1ImpressionParser -- same
            // queryId/adId fields, views count=f[1].
            KStream<String, String> impressionsParsed = source
                    .map((key, line) -> {
                        String[] f = line.split("\t");
                        String newKey = f[7] + "," + f[3];
                        return new KeyValue<>(newKey, "impression," + f[1]);
                    })
                    .process(OperatorMetricsProcessor.supplier("impressionsparser"));

            KStream<String, String> merged = clicksParsed.merge(impressionsParsed);

            // Stage [window]: single windowed aggregation, conditional sums
            // per event type instead of a join -- see class javadoc.
            KTable<Windowed<String>, String> aggregated = merged
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                            WindowConfig.Unit.MILLIS, graceMsArg, "AdsAnalytics/window"))
                    .aggregate(
                            () -> "0,0",
                            (key, value, agg) -> {
                                String[] eventParts = value.split(",", 2);
                                long eventValue = Long.parseLong(eventParts[1]);
                                String[] accParts = agg.split(",");
                                long clicksSum = Long.parseLong(accParts[0]);
                                long viewsSum = Long.parseLong(accParts[1]);
                                if (eventParts[0].equals("click")) {
                                    clicksSum += eventValue;
                                } else {
                                    viewsSum += eventValue;
                                }
                                return clicksSum + "," + viewsSum;
                            },
                            Materialized.with(Serdes.String(), Serdes.String()));

            // Stage [sink], mirrors RollingCTR.toString(); drops windows
            // where clicksSum > viewsSum, same filter as Flink/Spark.
            formatted = aggregated.toStream()
                    .process(OperatorMetricsProcessor.supplier("window"))
                    .filter((windowedKey, acc) -> {
                        String[] accParts = acc.split(",");
                        return Long.parseLong(accParts[0]) <= Long.parseLong(accParts[1]);
                    })
                    .map((windowedKey, acc) -> {
                        String[] accParts = acc.split(",");
                        long clicksSum = Long.parseLong(accParts[0]);
                        long viewsSum = Long.parseLong(accParts[1]);
                        double ctrValue = (double) clicksSum / viewsSum;
                        String[] keyParts = windowedKey.key().split(",");
                        String formattedValue = "RollingCTR{queryId=" + keyParts[0] + ", adId=" + keyParts[1]
                                + ", clicks=" + clicksSum + ", impressions=" + viewsSum
                                + ", ctrValue=" + ctrValue + "}";
                        return new KeyValue<>(windowedKey.key(), formattedValue);
                    });
        } else if (query == Constants.QUERY_TWO) {
            // Stage [parser]: mirrors Q2AdEventParser -- clicks=f[0],
            // views=f[1], adId=f[3], queryId=f[4]. Re-keys by (queryId,adId).
            KStream<String, String> parsed = source
                    .map((key, line) -> {
                        String[] f = line.trim().split("\\s+");
                        String newKey = f[4] + "," + f[3];
                        return new KeyValue<>(newKey, f[0] + "," + f[1]);
                    })
                    .process(OperatorMetricsProcessor.supplier("parser"));

            // Stage [counter]: stateless per-event CTR (see class javadoc).
            KStream<String, String> withCtr = parsed
                    .mapValues(value -> {
                        String[] p = value.split(",");
                        int clicks = Integer.parseInt(p[0]);
                        int views = Integer.parseInt(p[1]);
                        double ctr = (double) clicks / (views == 0 ? 1 : views);
                        return String.valueOf(ctr);
                    })
                    .process(OperatorMetricsProcessor.supplier("counter"));

            // Stage [rollingCTR]: sliding-window average CTR per (queryId, adId).
            KTable<Windowed<String>, String> aggregated = withCtr
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                            WindowConfig.Unit.MILLIS, graceMsArg, "AdsAnalytics/rollingCTR"))
                    .aggregate(
                            () -> "0.0,0",
                            (key, ctrStr, agg) -> {
                                double ctr = Double.parseDouble(ctrStr);
                                String[] accParts = agg.split(",");
                                double sum = Double.parseDouble(accParts[0]) + ctr;
                                long count = Long.parseLong(accParts[1]) + 1;
                                return sum + "," + count;
                            },
                            Materialized.with(Serdes.String(), Serdes.String()));

            // Stage [sink], mirrors RollingCTR.toString() (clicks/impressions
            // are 0 in this query's output, matching Q2RollingAverage.java).
            formatted = aggregated.toStream()
                    .process(OperatorMetricsProcessor.supplier("rollingCTR"))
                    .map((windowedKey, acc) -> {
                        String[] accParts = acc.split(",");
                        double avgCtr = Double.parseDouble(accParts[0]) / Double.parseDouble(accParts[1]);
                        String[] keyParts = windowedKey.key().split(",");
                        String formattedValue = "RollingCTR{queryId=" + keyParts[0] + ", adId=" + keyParts[1]
                                + ", clicks=0, impressions=0, ctrValue=" + avgCtr + "}";
                        return new KeyValue<>(windowedKey.key(), formattedValue);
                    });
        } else {
            throw new IllegalArgumentException("The only supported queries are \"1\" and \"2\".");
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
}
