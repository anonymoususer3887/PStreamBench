package com.kom.dsp.wordcount;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.OperatorMetricsProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../wordcount/WordCountJob.java and
 * spark/dsp_spark_jobs/.../wordcount/WordCountJob.java, kept as close as
 * possible to the same CLI contract, topics, and per-stage tokenization so
 * the same job_pds vector / enumeration strategies apply unmodified.
 *
 * Unlike Flink (setParallelism per operator) and Spark (repartition() per
 * stage), Kafka Streams has no per-operator parallelism control: every task
 * in the topology is parallelized uniformly by input-topic partition count,
 * and num.stream.threads only bounds how many of a *single instance's*
 * tasks run concurrently -- it does not vary by pipeline stage. To keep the
 * job_pds vector shape identical to Flink/Storm/Spark's sweep configs
 * (source parallelism fixed, operator parallelism swept, sink parallelism
 * fixed at 1 -- e.g. "2,4,4,1"), parallelism_degree[1] (the swept operator
 * parallelism) is used here as num.stream.threads, since that is the value
 * actually being varied across a sweep; parallelism_degree[0] (source) and
 * the trailing sink value have no independent Kafka Streams equivalent and
 * are intentionally ignored.
 *
 * Deployment model: this class is launched as one independent JVM per
 * worker node (see kafkastreams-provide-job.yaml), all sharing the same
 * application.id so they self-organize into a single consumer group over
 * the input topic's partitions -- there is no cluster to submit to and no
 * master/coordinator role, see KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.1.
 */
public class WordCountTopology {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        int streamThreads = Integer.parseInt(parallelism.split(",")[1]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        String applicationId = params.get("application-id");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams word count job currently only supports mode=KAFKA");
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
        // Task/processor-node/RocksDB metrics (Tier 2) are gated behind
        // "debug" recording level (see KAFKA_STREAMS_RISINGWAVE_PLAN.md
        // Part A.4); our own OperatorMetricsProcessor sensors (Tier 3) are
        // registered at INFO explicitly and are unaffected either way.
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        // "latest" so a freshly (re)started instance doesn't replay a whole
        // topic's backlog -- matches KafkaSource.setStartingOffsets(latest())
        // in the Flink port.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        StreamsBuilder builder = new StreamsBuilder();

        // Stage [source]: raw sentence strings straight off the topic.
        KStream<String, String> source = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"));

        // Stage [tokenizer]: mirrors Splitter.java's `sentence.split(" ")`.
        KStream<String, String> tokens = source
                .flatMapValues(sentence -> Arrays.asList(sentence.split(" ")))
                .selectKey((key, word) -> word)
                .process(OperatorMetricsProcessor.supplier("tokenizer"));

        // Stage [counter]: keyed aggregation. count() updates its state
        // store (and emits a changelog row) once per incoming record, same
        // as Flink's keyBy().sum(1) -- one output update per input token,
        // not a micro-batched summary like Spark's groupBy().count().
        KTable<String, Long> counted = tokens
                .groupByKey()
                .count();

        KStream<String, String> formatted = counted.toStream()
                .process(OperatorMetricsProcessor.supplier("counter"))
                .mapValues((word, count) -> word + "," + count);

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
