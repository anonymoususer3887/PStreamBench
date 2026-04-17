package com.kom.dsp.TrafficMonitoring;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class TrafficMonitoringMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        Long roadId = tuple.getLongByField("roadId");
        Double avgSpeed = tuple.getDoubleByField("avgSpeed");
        return String.format("Road: %s, Average Speed: %.2f",
                roadId, avgSpeed);
    }
}
