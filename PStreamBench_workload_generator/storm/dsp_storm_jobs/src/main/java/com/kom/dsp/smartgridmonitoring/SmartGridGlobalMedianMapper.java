package com.kom.dsp.smartgridmonitoring;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class SmartGridGlobalMedianMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        Long house = tuple.getLongByField("house_id");
        Double gal = tuple.getDoubleByField("globalMedian");
        return "{'House':"+house+",'GAL':"+gal+"}";
    }
}
