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


public class Q2AdEventParserBolt implements IRichBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("Preparing ParserBolt");
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;
        String line;

        try {
            e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            line = tuple.getStringByField("value");
        } catch (IllegalArgumentException e) {
            String tupleStr = (String) tuple.getValue(4);
            Object[] arr = KafkaUtils.parseValue(tupleStr);
            line = (String) arr[0];
            e2eTimestamp = arr[1] != null ? (long) arr[1] : processingTimestamp;
        }

        String[] record = line.split("\\s+");;
        int clicks = Integer.parseInt(record[0]);
        int views = Integer.parseInt(record[1]);
        String displayUrl = record[2];
        String adId = record[3];
        String queryId = record[4];
        int depth = Integer.parseInt(record[5]);
        int position = Integer.parseInt(record[6]);
        String advertiserId = record[7];
        String keywordId = record[8];
        String titleId = record[9];
        String descriptionId = record[10];
        String userId = record[11];

        //AdEvent event = new AdEvent("", clicks, views, displayUrl, adId, queryId, depth, position, advertiserId, keywordId, titleId, descriptionId, userId, 1);
        collector.emit(new Values(adId, queryId, clicks, views, e2eTimestamp, processingTimestamp));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("adId", "queryId", "clicks", "views", "e2eTimestamp", "processingTimestamp"));
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {
    }
}
