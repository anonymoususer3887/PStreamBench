package com.kom.dsp.MachineOutlier;

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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../MachineOutlier/MachineOutlier.java and
 * spark/dsp_spark_jobs/.../MachineOutlier/MachineOutlier.java.
 *
 * MachineOutlierWindowFunction there is a hand-rolled keyed function that
 * caps accumulated readings per machineId at 4 ("count==4, break"), runs
 * BFPRT median-of-medians selection, and flags readings whose Euclidean
 * distance from the pivot exceeds a hardcoded 0.5 threshold (note: the CLI
 * "popularityThreshold" argument is parsed but never actually passed to the
 * window function in either the Flink or Spark source -- 0.5 is what
 * really runs, so 0.5 is what this uses too, same as the Spark port).
 * Kafka Streams has no declarative window() equivalent for an arbitrary
 * per-group algorithm like BFPRT (same reason Spark needed
 * flatMapGroupsWithState instead of a built-in aggregate), so this uses a
 * custom stateful Processor with an attached KeyValueStore instead -- see
 * MachineOutlierProcessor.
 *
 * Stage shape: [0] source, [1] parser, [2] outlier, [3] sink (matches the
 * Spark port's stage set; parallelism_degree[3] is unused in both Flink and
 * Spark sources, sink uses index 4 there -- Kafka Streams has no per-stage
 * parallelism control at all, see Part A.2 of the plan doc, so this
 * distinction does not carry over).
 */
public class MachineOutlierTopology {

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
                    "Kafka Streams Machine Outlier job currently only supports mode=KAFKA");
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

        StreamsBuilder builder = new StreamsBuilder();

        // Stage [source].
        KStream<String, String> source = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"));

        // Stage [parser]: mirrors MachineUsageParser.java (memBW/mkpi
        // default to 0 if fields 4/5 are blank); re-keyed by machineId so
        // the outlier stage's state store partitions correctly per machine.
        KStream<String, String> parsed = source
                .map((key, line) -> {
                    String[] v = line.split(",", -1);
                    String machineId = v[0];
                    double timestamp = Double.parseDouble(v[1]);
                    double cpu = Double.parseDouble(v[2]);
                    double mem = Double.parseDouble(v[3]);
                    double memBW = v[4].isEmpty() ? 0 : Double.parseDouble(v[4]);
                    double mkpi = v[5].isEmpty() ? 0 : Double.parseDouble(v[5]);
                    double netIn = Double.parseDouble(v[6]);
                    double netOut = Double.parseDouble(v[7]);
                    double diskIO = Double.parseDouble(v[8]);
                    String encoded = machineId + "," + timestamp + "," + cpu + "," + mem + ","
                            + memBW + "," + mkpi + "," + netIn + "," + netOut + "," + diskIO;
                    return new KeyValue<>(machineId, encoded);
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [outlier]: stateful BFPRT-based outlier detection, see
        // MachineOutlierProcessor. Registers its own Tier 3 "outlier"
        // sensor internally (forwarded-record count, not input count).
        KStream<String, String> outliers = parsed.process(MachineOutlierProcessor.supplier());

        // Stage [sink].
        outliers
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
