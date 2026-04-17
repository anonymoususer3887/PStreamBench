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

public class Q2AdEventCounterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> clickCounts;
    private Map<String, Integer> impressionCounts;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.clickCounts = new HashMap<>();
        this.impressionCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String queryId = input.getStringByField("queryId");
        String adId = input.getStringByField("adId");
        Integer clicks = input.getIntegerByField("clicks");
        Integer views = input.getIntegerByField("views");

        // Build a key to identify the ad event
        String key = queryId + "-" + adId;

        // Update click or impression counts based on the type of event
        clickCounts.put(key, clickCounts.getOrDefault(key, 0) + clicks);
        impressionCounts.put(key, clickCounts.getOrDefault(key, 0) + views);

        // Calculate CTR for the ad event
        float ctrValue = calculateCTR(clickCounts.getOrDefault(key, 0), impressionCounts.getOrDefault(key, 0));
        System.out.println("AdEventCTR for " + key + ": " + ctrValue);
        // Emit the ad event along with the calculated CTR
        collector.emit(input, new Values(queryId, adId, ctrValue, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("queryId", "adId", "ctrValue", "e2eTimestamp","processingTimestamp"));
    }
    @Override
    public void cleanup() {
        // For now, we provide an empty implementation to avoid UnsupportedOperationException
    }

    private float calculateCTR(int clicks, int impressions) {
        if (impressions == 0) {
            return 0.0f; // Avoid division by zero
        }
        return (float) clicks / impressions;
    }
}
