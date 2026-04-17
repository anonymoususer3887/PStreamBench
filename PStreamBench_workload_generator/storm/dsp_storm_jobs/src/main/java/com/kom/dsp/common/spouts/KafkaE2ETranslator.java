package com.kom.dsp.common.spouts;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaE2ETranslator implements RecordTranslator<String, String> {
    public static final Fields FIELDS = new Fields("topic", "partition", "offset", "key", "value", "e2eTimestamp");

    @Override
    public List<Object> apply(ConsumerRecord<String, String> record) {
        return new Values(record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                System.currentTimeMillis());
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return FIELDS;
    }
}
