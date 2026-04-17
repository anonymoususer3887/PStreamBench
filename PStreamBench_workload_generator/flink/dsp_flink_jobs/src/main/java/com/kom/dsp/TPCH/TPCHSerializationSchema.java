package com.kom.dsp.TPCH;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class TPCHSerializationSchema implements SerializationSchema<Tuple2<Integer, Integer>> {

    @Override
    public byte[] serialize(Tuple2<Integer, Integer> stringDoubleTuple2) {
        Integer orderPriority = stringDoubleTuple2.f0;
        Integer numberOfOrders = stringDoubleTuple2.f1;

        String result = "orderPriority : "+ orderPriority + " Number of orders: " + numberOfOrders;
        return result.getBytes();
    }
}
