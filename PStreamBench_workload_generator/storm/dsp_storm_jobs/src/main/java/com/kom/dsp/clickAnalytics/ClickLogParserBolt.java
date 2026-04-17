package com.kom.dsp.clickAnalytics;

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

public class ClickLogParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();

        try {
            long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            String s = tuple.getStringByField("value");

            JsonObject obj = new JsonParser().parse(s).getAsJsonObject();
            String url = obj.get("url").getAsString();
            String ip = obj.get("ip").getAsString();
            String clientKey = obj.get("clientKey").getAsString();

            collector.emit(new Values(
                        clientKey,
                        new ClickLog(url, ip, clientKey),
                        e2eTimestamp,
                        processingTimestamp
                        ));
            collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clientKey", "clickLog", "e2eTimestamp", "processingTimestamp"));
    }
}
