package com.kom.dsp.adAnalytics;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class Q2RollingCTRMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        String queryId = tuple.getStringByField("queryId");
        String adId = tuple.getStringByField("adId");
        Float rollingCTR = tuple.getFloatByField("rollingCTR");
        return "RollingCTR{" +
                "queryId=" + queryId +
                ", adId=" + adId +
                ", rollingCTR=" + rollingCTR +
                '}';
    }
}
