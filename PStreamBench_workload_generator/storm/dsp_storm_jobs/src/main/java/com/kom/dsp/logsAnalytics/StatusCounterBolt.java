package com.kom.dsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class StatusCounterBolt extends BaseWindowedBolt {
    private Map<String, Integer> statusCounts;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.statusCounts = new HashMap<>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()) {
            String statusCode = tuple.getStringByField("statusCode");

            Integer currentCount = statusCounts.getOrDefault(statusCode, 0);
            statusCounts.put(statusCode, currentCount+1);

            collector.emit(new Values(statusCode + ": " + currentCount, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message", "e2eTimestamp", "processingTimestamp"));
    }
}
