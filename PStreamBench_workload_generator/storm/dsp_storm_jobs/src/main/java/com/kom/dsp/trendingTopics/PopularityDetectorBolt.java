package com.kom.dsp.trendingTopics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class PopularityDetectorBolt extends BaseRichBolt {

    OutputCollector collector;
    int topicPopularityThreshold;

    public PopularityDetectorBolt(int topicPopularityThreshold) {
        this.topicPopularityThreshold = topicPopularityThreshold;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer topicCount = input.getIntegerByField("topicCount");

        if (topicCount > topicPopularityThreshold) {
            String topic = input.getStringByField("topic");
            collector.emit(new Values(topic, topicCount, input.getLongByField("e2eTimestamp"), input.getLongByField("processingTimestamp")));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topic", "topicCount", "e2eTimestamp", "processingTimestamp"));
    }
}
