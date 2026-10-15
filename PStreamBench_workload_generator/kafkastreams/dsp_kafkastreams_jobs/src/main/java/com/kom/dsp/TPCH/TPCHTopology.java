package com.kom.dsp.TPCH;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.OperatorMetricsProcessor;
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
import org.apache.kafka.streams.kstream.Repartitioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of flink/dsp_flink_jobs/.../TPCH/TPCH.java and
 * spark/dsp_spark_jobs/.../TPCH/TPCH.java.
 *
 * Query 2's Flink FilterCalculator re-applies query 1's exact same
 * "orderPriority > 2" filter after an extra keyBy(discount) shuffle -- same
 * result, extra communication cost for the DoP study. Reproduced here the
 * same way the Spark port does: an explicit re-key-by-discount +
 * KStream.repartition() (forces a real network shuffle through an internal
 * topic) before the filter, for query 2 only; query 1 filters directly with
 * no artificial pre-shuffle, since the eventual groupByKey(orderPriority)
 * in the counter stage already provides a real shuffle either way.
 *
 * Malformed lines (!= 6 fields) are dropped in the parser stage rather than
 * risking a spurious pass of the orderPriority > 2 filter on a
 * default/garbage value -- same reasoning as the Spark port.
 *
 * Stage shape: [0] source, [1] parser, [2] filter, [3] flatmapper,
 * [4] counter, [5] sink -- same as Flink/Spark.
 */
public class TPCHTopology {

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

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams TPC-H job currently only supports mode=KAFKA");
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        StreamsBuilder builder = new StreamsBuilder();

        // Stage [source].
        KStream<String, String> source = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"));

        // Stage [parser]: mirrors TPCHEventParser.java (orderPriority=f[3],
        // discount=f[5]); malformed lines dropped.
        KStream<String, String> parsed = source
                .flatMap((key, line) -> {
                    List<KeyValue<String, String>> out = new ArrayList<>();
                    String[] f = line.split(",");
                    if (f.length == 6) {
                        int orderPriority = Integer.parseInt(f[3]);
                        double discount = Double.parseDouble(f[5]);
                        out.add(new KeyValue<>(null, orderPriority + "," + discount));
                    }
                    return out;
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [filter]: query 2 re-keys by discount and forces a real
        // shuffle before filtering; query 1 filters directly.
        KStream<String, String> filtered;
        if (query == Constants.QUERY_TWO) {
            filtered = parsed
                    .selectKey((key, value) -> value.split(",")[1])
                    .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                    .filter((key, value) -> Integer.parseInt(value.split(",")[0]) > 2);
        } else {
            filtered = parsed.filter((key, value) -> Integer.parseInt(value.split(",")[0]) > 2);
        }
        filtered = filtered.process(OperatorMetricsProcessor.supplier("filter"));

        // Stage [flatmapper]: select orderPriority, re-key by it.
        KStream<String, String> mapped = filtered
                .map((key, value) -> {
                    String orderPriority = value.split(",")[0];
                    return new KeyValue<>(orderPriority, orderPriority);
                })
                .process(OperatorMetricsProcessor.supplier("flatmapper"));

        // Stage [counter]: running (unwindowed) count of orders per priority.
        KTable<String, Long> counts = mapped
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count();

        // Stage [sink], mirrors TPCHSerializationSchema.java's format.
        KStream<String, String> formatted = counts.toStream()
                .process(OperatorMetricsProcessor.supplier("counter"))
                .map((orderPriority, count) -> new KeyValue<>(orderPriority,
                        "orderPriority : " + orderPriority + " Number of orders: " + count));

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
