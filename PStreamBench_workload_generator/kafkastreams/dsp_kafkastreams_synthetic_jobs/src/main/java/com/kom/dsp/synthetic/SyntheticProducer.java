package com.kom.dsp.synthetic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Standalone synthetic data generator, run as its OWN process -- Kafka
 * Streams has no in-process synthetic source the way Flink
 * (DataGeneratorSource) or Spark ("rate" format) do; every Kafka Streams
 * job unconditionally reads from a real Kafka topic (see
 * dsp_kafkastreams_jobs' WordCountTopology: "mode=KAFKA" is its only
 * supported mode). This mirrors the real-workload convention of a
 * SEPARATE producer process feeding real topics (see
 * infra/playbook/tasks/producer/producer-run.yaml), just generating
 * synthetic (key,value) pairs instead of replaying a dataset file.
 *
 * One independent producer thread per topic, each generating at the SAME
 * configured eventRate (not divided across topics) -- matches
 * Flink/Spark's source() convention exactly (confirmed in both:
 * DataGeneratorSource/the "rate" format give each independently-called
 * source its own full configured rate), so calibratedKeySpace()'s
 * assumptions (and the J_n analysis in SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md
 * section 11) carry over unchanged to this engine.
 *
 * Message format: "key,value" (CSV, one tuple per record), key column
 * repeated as the Kafka record key too (so a keyed GROUP BY/join on the
 * record key works without a separate map/selectKey step) -- same
 * (String key, Double value) tuple model as every other engine's
 * synthetic port, represented here as a plain CSV string since Kafka
 * record values are raw bytes/strings, not a typed tuple.
 */
public class SyntheticProducer {

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        if (cfg.topics == null) {
            throw new IllegalArgumentException("--topics (comma-separated) is required");
        }
        List<String> topicList = Arrays.asList(cfg.topics.split(","));
        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : 1000;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));

        Thread[] threads = topicList.stream().map(topic -> new Thread(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                Random rand = new Random();
                long periodNanos = 1_000_000_000L / Math.max(1, cfg.eventRate);
                long nextSend = System.nanoTime();
                long end = System.currentTimeMillis() + cfg.waitTimeToCancel * 1000;
                while (System.currentTimeMillis() < end && stop.getCount() > 0) {
                    String key = String.valueOf(rand.nextInt(keySpace));
                    double value = rand.nextDouble();
                    producer.send(new ProducerRecord<>(topic, key, key + "," + value));
                    nextSend += periodNanos;
                    long sleepNanos = nextSend - System.nanoTime();
                    if (sleepNanos > 0) {
                        try {
                            Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
                producer.flush();
                System.out.println("[SyntheticProducer] topic=" + topic + " finished.");
            }
        })).toArray(Thread[]::new);

        System.out.println("[SyntheticProducer] starting " + threads.length + " topic(s): " + topicList
                + " eventRate=" + cfg.eventRate + " keySpace=" + keySpace
                + " for " + cfg.waitTimeToCancel + "s");
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        System.out.println("[SyntheticProducer] all topics finished.");
    }
}
