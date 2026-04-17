package com.kom.dsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;


/**
 * counts the occurences of topics
 */

public class RollingCounterSlidingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;
        Map<String, Integer> topicCount = new HashMap<>();

        for (Tuple tuple : inputWindow.get()) {
            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));

            String topic = tuple.getStringByField("topic");
            int count = tuple.getIntegerByField("count");
            topicCount.put(topic, topicCount.getOrDefault(topic, 0) + count);

            collector.emit(new Values(topic, topicCount.get(topic), e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topic", "topicCount", "e2eTimestamp", "processingTimestamp"));
    }
}

