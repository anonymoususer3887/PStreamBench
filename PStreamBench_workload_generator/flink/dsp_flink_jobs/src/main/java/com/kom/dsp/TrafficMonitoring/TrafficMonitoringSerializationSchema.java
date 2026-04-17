package com.kom.dsp.TrafficMonitoring;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class TrafficMonitoringSerializationSchema implements SerializationSchema<Tuple2<Long, Double>> {

    @Override
    public byte[] serialize(Tuple2<Long, Double> element) {
        String s = element.f0 + "," + element.f1;
        return s.getBytes();
    }
}
