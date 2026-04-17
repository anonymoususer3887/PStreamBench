package com.kom.dsp.googleCloudMonitoring;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class CPUPerCategoryMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        CPUPerCategory o = (CPUPerCategory) tuple.getValueByField("cpuPerCategory");
        long timestamp = o.getTimestamp();
        int category = o.getCategory();
        float totalCpu = o.getTotalCpu();
        return "CPUPerCatgory{" +
                "timestamp=" + timestamp +
                ", category=" + category +
                ", totalCpu=" + totalCpu +
                '}';
    }
}
