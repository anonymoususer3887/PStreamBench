package com.kom.dsp.bargainIndex;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class BargainIndexMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        String symbol = tuple.getStringByField("symbol");
        double bargainIndex = tuple.getDoubleByField("bargainIndex");
        long totalVolume = tuple.getLongByField("totalVolume");
        return symbol + "," + bargainIndex + "," + totalVolume;
    }
}
