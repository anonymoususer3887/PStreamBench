package com.kom.dsp.BargainIndex;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;

public class BargainSerializationSchema implements SerializationSchema<Tuple3<String, Double, Long>> {

    @Override
    public byte[] serialize(Tuple3<String, Double, Long> element) {
        String s = element.f0 + "," + element.f1 + "," + element.f2;
        return s.getBytes();
    }
}
