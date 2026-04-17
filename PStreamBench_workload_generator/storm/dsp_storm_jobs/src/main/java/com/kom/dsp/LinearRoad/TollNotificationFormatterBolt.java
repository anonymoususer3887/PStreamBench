package com.kom.dsp.LinearRoad;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TollNotificationFormatterBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        TollNotification tollNotification = (TollNotification) tuple.getValueByField("notification");
        Long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
        Long processingTimestamp = tuple.getLongByField("e2eTimestamp");

        collector.emit(new Values(tollNotification.stringFormatter(), e2eTimestamp, processingTimestamp));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message", "e2eTimestamp", "processingTimestamp"));
    }
}
