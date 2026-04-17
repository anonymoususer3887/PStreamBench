package com.kom.dsp.adAnalytics;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.kom.dsp.utils.KafkaUtils;


public class Q1ImpressionsParserBolt implements IRichBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("Preparing ParserBolt");
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
        String line = tuple.getStringByField("value");

        String[] record = line.split("\t");
        int clicks = Integer.parseInt(record[0]);
        int views = Integer.parseInt(record[1]);
        String displayUrl = record[2];
        long adId = Long.parseLong(record[3]);
        String advertiserId = record[4];
        int depth = Integer.parseInt(record[5]);
        int position = Integer.parseInt(record[6]);
        long queryId = Long.parseLong(record[7]);
        String keywordId = record[8];
        String titleId = record[9];
        String descriptionId = record[10];
        String userId = record[11];

        collector.emit(new Values("impression", adId, queryId, views, e2eTimestamp, processingTimestamp));
//        for (int i = 0; i < views; i++) {
//            collector.emit(new Values("impression", adId, queryId, 1, e2eTimestamp, processingTimestamp));
//        }
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type", "adId", "queryId", "count","e2eTimestamp", "processingTimestamp"));
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {
    }
}
