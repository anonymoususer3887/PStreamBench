package com.kom.dsp.smartgridmonitoring;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class SmartGridPlugMedianMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        Long house = tuple.getLongByField("house_id");
        String plug = tuple.getStringByField("plugID");
        Double lal = tuple.getDoubleByField("plugMedian");
        return "{'House'"+house+",'Plug'"+plug+",'LAL':"+lal+"}";
    }
}
