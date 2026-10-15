package com.kom.dsp.SentimentAnalysis;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
 * flink/dsp_flink_jobs/.../SentimentAnalysis/SentimentAnalysis.java and
 * spark/dsp_spark_jobs/.../SentimentAnalysis/SentimentAnalysis.java.
 *
 * BasicClassifier/SentimentResult/SentimentClassifier are reused unchanged
 * (plain Java + Guava, no engine-specific dependency) -- AFINN-111.txt is
 * copied into this module's resources under the same classpath path they
 * expect. The Flink source's TwitterAnalyser.map() constructs a brand new
 * SentimentClassifier (re-reading and re-parsing the AFINN lexicon from the
 * classpath) for *every single tweet*; this loads it once per JVM (a static
 * field, lazily initialized), same simplification the Spark port already
 * made via mapPartitions -- avoids pure object/IO overhead that isn't the
 * CPU classification cost this workload is meant to stress. The per-tweet
 * word-lookup classification work itself is unchanged.
 *
 * No windowing/keying in either reference implementation -- this is one of
 * the simplest jobs in the suite to port. Stage shape: [0] source,
 * [1] parser, [2] analyser, [3] sink -- same as Flink/Spark.
 */
public class SentimentAnalysisTopology {

    private static final SentimentClassifier CLASSIFIER = loadClassifier();

    private static SentimentClassifier loadClassifier() {
        SentimentClassifier classifier = new BasicClassifier();
        classifier.initialize();
        return classifier;
    }

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

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams Sentiment Analysis job currently only supports mode=KAFKA");
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

        // Stage [parser]: mirrors TwitterParser.java's JSON fields
        // (id, created_at, text), carried forward as "id,timestamp,text".
        KStream<String, String> parsed = source
                .mapValues(json -> {
                    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
                    String id = obj.get("id").getAsString();
                    String timestamp = obj.get("created_at").getAsString();
                    String text = obj.get("text").getAsString();
                    return id + "," + timestamp + "," + text;
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [analyser]: AFINN lexicon sentiment score/classification.
        KStream<String, String> scored = parsed
                .mapValues(value -> {
                    String[] parts = value.split(",", 3);
                    String id = parts[0];
                    String timestamp = parts[1];
                    String text = parts[2];
                    SentimentResult result = CLASSIFIER.classify(text);
                    return id + "" + timestamp + "" + text + ""
                            + result.getSentiment() + "" + result.getScore();
                })
                .process(OperatorMetricsProcessor.supplier("analyser"));

        // Stage [sink], mirrors TweetScored.toString().
        KStream<String, String> formatted = scored.mapValues(value -> {
            String[] parts = value.split("", 5);
            return "TweetScored{id='" + parts[0] + "', timestamp='" + parts[1] + "', text='" + parts[2]
                    + "', sentiment='" + parts[3] + "', sentimentScore=" + parts[4] + "}";
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
