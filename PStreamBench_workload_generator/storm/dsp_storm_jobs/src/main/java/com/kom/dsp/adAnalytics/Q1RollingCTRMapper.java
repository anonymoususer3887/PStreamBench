package com.kom.dsp.adAnalytics;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class Q1RollingCTRMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        long queryId = tuple.getLongByField("queryId");
        long adId = tuple.getLongByField("adId");
        Integer clicks = tuple.getIntegerByField("clicks");
        Integer impressions = tuple.getIntegerByField("impressions");
        Float ctrValue = tuple.getFloatByField("ctrValue");
        return "RollingCTR{" +
                "queryId=" + queryId +
                ", adId=" + adId +
                ", clicks=" + clicks +
                ", impressions=" + impressions +
                ", ctrValue=" + ctrValue +
                '}';
    }
}
