package com.kom.dsp.clickAnalytics;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class GeographyMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        GeoStats geoStats = (GeoStats)tuple.getValueByField("geoStats");
        return geoStats.toString();
    }
}
