package com.kom.dsp.sentimentAnalysis;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.kom.dsp.utils.KafkaUtils;

import java.util.Map;

public class TweetParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp;

        try {
            String s = input.getStringByField("value");
            e2eTimestamp = input.getLongByField("e2eTimestamp");

            JsonObject obj = JsonParser.parseString(s).getAsJsonObject();
            String id = obj.get("id").getAsString();
            String text = obj.get("text").getAsString();
            String timestamp = obj.get("created_at").getAsString();

            collector.emit(new Values(id, timestamp, text, e2eTimestamp, processingTimestamp));
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "timestamp", "text", "e2eTimestamp", "processingTimestamp"));
    }

}
