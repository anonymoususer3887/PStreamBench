package com.kom.dsp.clickAnalytics;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class SumReducerMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        String url = tuple.getStringByField("url");
        int totalVisits = tuple.getIntegerByField("totalVisitCounter");
        int uniqueVisits = tuple.getIntegerByField("uniqueVisitCounter");
        return url + ", " + totalVisits + ", " + uniqueVisits;
    }
}
