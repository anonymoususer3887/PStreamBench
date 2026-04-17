package com.kom.dsp.common.spouts;

import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

public class CustomKafkaSpout {
    private KafkaSpoutConfig<String, String> kafkaSpoutConfig;
    private KafkaSpout<String, String> kafkaSpout;

    public CustomKafkaSpout(String bootstrapServer, String topic, String consumerGroupId) {
        kafkaSpoutConfig = KafkaSpoutConfig.builder(bootstrapServer, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp("fetch.min.bytes", "100000")
                .setProp("fetch.max.wait.ms", "50")
                .setPollTimeoutMs(2000)  // default 200
                .setOffsetCommitPeriodMs(300_000)  // default 30_000
                .setMaxUncommittedOffsets(100_000_000)  // default 10_000_000
                .setRecordTranslator(new KafkaE2ETranslator())
                .build();

        kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);
    }

    public KafkaSpout<String, String> getKafkaSpout() {
        return kafkaSpout;
    }
}
