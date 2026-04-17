package com.kom.dsp.spikedetection;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class SpikeDetectorMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        int sensorId = tuple.getIntegerByField("sensorId");
        float currentValue = tuple.getFloatByField("voltage");
        float averageValue = tuple.getFloatByField("average");

        return "AverageValue{" +
            "sensorId=" + sensorId +
            ", currentValue=" + currentValue +
            ", averageValue=" + averageValue +
            '}';
    }
}
