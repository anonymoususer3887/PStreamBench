package com.kom.dsp.synthetic;

import com.kom.dsp.synthetic.utils.OperatorMetricsProcessor;
import com.kom.dsp.synthetic.utils.WindowConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Base class for the 9 fixed synthetic query shapes, Kafka Streams
 * counterpart of dsp_flink_synthetic_jobs'/dsp_spark_synthetic_jobs'
 * SyntheticTopology. Independent project, same (key: String, value:
 * Double) tuple model, represented here as a KStream&lt;String,String&gt; of
 * "key,value" CSV records (see SyntheticProducer's javadoc) since Kafka
 * record values are raw bytes/strings, not a typed tuple the way Flink's
 * DataStream or even Spark's typed Dataset columns are.
 *
 * <b>No in-process source, matching this engine's own real-workload
 * convention, not Flink/Spark's.</b> Every Kafka Streams job in this
 * project (synthetic or real) reads from and writes to actual Kafka
 * topics -- there is no DataGeneratorSource/"rate"-format equivalent. See
 * SyntheticProducer for the separate process that generates synthetic
 * traffic into those topics.
 *
 * <b>Parallelism model -- the coarsest of any of the 4 engines.</b> Flink
 * sets parallelism per operator; Spark controls narrow-stage task count
 * via repartition() and collapses all shuffle stages (joins + window-agg)
 * into one shared spark.sql.shuffle.partitions value (see
 * SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md section 11); Kafka Streams has NO
 * per-stage or per-shuffle control at all -- num.stream.threads is a
 * single JVM-wide setting bounding how many of ONE instance's tasks run
 * concurrently, and the真actual upper bound on parallelism is each
 * SOURCE TOPIC's partition count (confirmed in dsp_kafkastreams_jobs'
 * WordCountTopology javadoc, same precedent followed here: only
 * parallelism.get(1), the first "swept" CLI value, is used, as
 * num.stream.threads -- every other position in the --parallelism vector
 * is accepted for CLI-shape consistency with the other 3 engines'
 * tooling, but has no effect here).
 */
public abstract class SyntheticTopology {

    protected final ConfigArgs cfg;
    protected final StreamsBuilder builder = new StreamsBuilder();
    private KStream<String, String> finalSink;

    protected SyntheticTopology(ConfigArgs cfg) {
        this.cfg = cfg;
    }

    /** Parses this stage's "key,value" CSV records into a (String key,
     * double value) pair, re-emitting the SAME CSV format downstream so
     * every stage shares one wire format -- callers needing the parsed
     * double do so inline (see filter()/windowAvg()/pairwiseJoin()). */
    private static double parseValue(String csv) {
        return Double.parseDouble(csv.substring(csv.indexOf(',') + 1));
    }

    protected KStream<String, String> source(String topic, String name) {
        return builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier(name));
    }

    /** Keeps records with val &lt; threshold -- same direction as every
     * other engine's synthetic filter (see
     * SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md section 11.1 on why this
     * direction matters: it was found backwards once already, in the
     * Spark port). */
    protected KStream<String, String> filter(KStream<String, String> in, String name, double threshold) {
        return in.filter((key, csv) -> parseValue(csv) < threshold)
                .process(OperatorMetricsProcessor.supplier(name));
    }

    /** Tumbling-window average of the value field, keyed by the record
     * key -- same shape as every other engine's windowAvg(). Kafka
     * Streams' windowed aggregation produces a KTable&lt;Windowed&lt;String&gt;,
     * ...&gt;; converted back to a plain KStream&lt;String,String&gt; (dropping
     * the window-boundary metadata, keeping just the original key) so
     * every stage downstream shares the same (key,value) CSV shape every
     * other stage uses. */
    protected KStream<String, String> windowAvg(KStream<String, String> in, String name, int windowSizeMs) {
        TimeWindows windows = WindowConfig.buildTumbling(windowSizeMs, null);
        // Accumulator encoded as "sum,count" (reusing the plain String
        // Serde every other stage already uses) rather than a custom
        // double[] Serde -- avoids one more hand-written
        // serializer/deserializer pair for a single internal state store.
        KTable<Windowed<String>, String> averaged = in
                .groupByKey()
                .windowedBy(windows)
                .aggregate(
                        () -> "0.0,0.0",
                        (key, csv, acc) -> {
                            String[] parts = acc.split(",");
                            double sum = Double.parseDouble(parts[0]) + parseValue(csv);
                            double count = Double.parseDouble(parts[1]) + 1.0;
                            return sum + "," + count;
                        },
                        org.apache.kafka.streams.kstream.Materialized.with(Serdes.String(), Serdes.String()));
        return averaged.toStream()
                .map((windowedKey, acc) -> {
                    String[] parts = acc.split(",");
                    double sum = Double.parseDouble(parts[0]);
                    double count = Double.parseDouble(parts[1]);
                    double avg = count == 0 ? 0.0 : sum / count;
                    return KeyValue.pair(windowedKey.key(), windowedKey.key() + "," + avg);
                })
                .process(OperatorMetricsProcessor.supplier(name));
    }

    /** Stream-stream join on the record key, averaging the two "val"
     * columns -- same combine semantics as every other engine's
     * pairwiseJoin() ((x,y) -&gt; (x.key, (x.val+y.val)/2)).
     * JoinWindows.ofTimeDifferenceAndGrace gives a symmetric +/-windowSize
     * tolerance -- Kafka Streams' own native stream-stream join primitive
     * has no discrete-bucket equivalent to Flink's windowed join at all
     * (confirmed: JoinWindows is the only windowed-join construct the DSL
     * offers), so this is the closest *native* fit for this engine
     * specifically, not a reversion to the sliding-tolerance design the
     * Spark port moved away from (see WindowConfig.buildJoin's javadoc). */
    protected KStream<String, String> pairwiseJoin(KStream<String, String> left, KStream<String, String> right, String name, int windowSizeMs) {
        JoinWindows joinWindows = WindowConfig.buildJoin(windowSizeMs, null);
        KStream<String, String> joined = left.join(
                right,
                (lCsv, rCsv) -> {
                    String key = lCsv.substring(0, lCsv.indexOf(','));
                    double avg = (parseValue(lCsv) + parseValue(rCsv)) / 2.0;
                    return key + "," + avg;
                },
                joinWindows,
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));
        return joined.process(OperatorMetricsProcessor.supplier(name));
    }

    protected void sink(KStream<String, String> in, String name, String outputTopic) {
        this.finalSink = in.process(OperatorMetricsProcessor.supplier(name));
        this.finalSink.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    /** Self-cancelling lifecycle, same convention as
     * dsp_kafkastreams_jobs' WordCountTopology: streams.start(), sleep
     * waitTimeToCancel seconds, streams.close()+cleanUp() (wiping this
     * run's local RocksDB state store -- see that class's javadoc for the
     * live-confirmed disk-exhaustion bug this avoids: application.id is
     * uniquely timestamped per submission, so nothing is ever
     * restarted/recovered from leftover state). */
    protected void runAndSelfCancel() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, cfg.parallelism.get(1));
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            streams.cleanUp();
            latch.countDown();
        }));

        System.out.println("[main] Starting Kafka Streams, application.id=" + cfg.applicationId);
        streams.start();
        System.out.println("[main] Will run for " + cfg.waitTimeToCancel + " seconds.");
        Thread.sleep(cfg.waitTimeToCancel * 1000);
        streams.close();
        streams.cleanUp();
        System.out.println("[main] Streams closed.");
    }
}
