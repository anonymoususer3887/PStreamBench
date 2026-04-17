package com.kom.dsp.tpch;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class TPCHMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        int orderPriority = tuple.getIntegerByField("orderPriority");
        int numberOfOrders = tuple.getIntegerByField("count");

        return "orderPriority : "+ orderPriority + " Number of orders: " + numberOfOrders;
    }
}
