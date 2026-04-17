package com.kom.dsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class TwitterparserBolt implements IRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp = tuple.getLongByField("e2eTimestamp");

        try {
            String line = tuple.getStringByField("value");

            String[] values = line.split(",");
            String id = values[1];
            String timestamp = values[2];
            String text = values[5];

            collector.emit(new Values(id, timestamp, text, e2eTimestamp, processingTimestamp));
            collector.ack(tuple);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "timestamp", "text","e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }
}
