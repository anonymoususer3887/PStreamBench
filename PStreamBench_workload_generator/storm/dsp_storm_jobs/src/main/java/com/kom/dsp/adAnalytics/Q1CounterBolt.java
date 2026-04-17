package com.kom.dsp.adAnalytics;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Q1CounterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {

        String type = input.getStringByField("type");
        long queryId = input.getLongByField("queryId");
        long adId = input.getLongByField("adId");
        int count = input.getIntegerByField("count");

        // Build a key to identify the ad event
        String key = queryId + "-" + adId;

        counts.put(key, counts.getOrDefault(key, 0) + count);

        // Calculate CTR for the ad event
        int sum = counts.getOrDefault(key, 0);
        collector.emit(input, new Values(type, key, queryId, adId, sum, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "queryId", "adId", "count", "e2eTimestamp","processingTimestamp"));
    }
    @Override
    public void cleanup() {
        // For now, we provide an empty implementation to avoid UnsupportedOperationException
    }
}
