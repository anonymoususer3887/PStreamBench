package com.kom.dsp.adAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Q1RollingCTRCalculatorBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long queryId = input.getLongByField("impressions-counter:queryId");
        long adId = input.getLongByField("impressions-counter:adId");
        Integer clicksCount = input.getIntegerByField("clicks-counter:count");
        Integer impressionsCount = input.getIntegerByField("impressions-counter:count");
        float ctrValue = clicksCount / (float) impressionsCount;

        long e2eTimestamp = input.getLongByField("impressions-counter:e2eTimestamp");
        long processingTimestamp = input.getLongByField("impressions-counter:processingTimestamp");

        if (clicksCount <= impressionsCount) {
            collector.emit(new Values(queryId, adId, clicksCount, impressionsCount, ctrValue, e2eTimestamp, processingTimestamp));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("queryId", "adId", "clicks", "impressions", "ctrValue", "e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public void cleanup() {}
}
