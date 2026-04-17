package com.kom.dsp.LinearRoad;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AccidentNotificationFormatterBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Long time = tuple.getLongByField("time");
        Integer lane = tuple.getIntegerByField("lane");
        Integer segment = tuple.getIntegerByField("segment");
        Integer position = tuple.getIntegerByField("position");
        Long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
        Long processingTimestamp = tuple.getLongByField("e2eTimestamp");

        String msg = "Accident occurred at ---"+ "Segment:" + segment + " lane: "+ lane + " position: "+ position + " timestamp: "+ time;
        collector.emit(new Values(msg, e2eTimestamp, processingTimestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message", "e2eTimestamp", "processingTimestamp"));
    }
}
