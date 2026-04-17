package com.kom.dsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.kom.dsp.utils.KafkaUtils;

import java.util.Map;

public class LogParserBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
        String log;

        try {
            log = tuple.getStringByField("value");

            // Parse the log and extract the required fields
            String[] record = log.split(" ");
            String logTime = record[3].substring(1);
            String statusCode = record[8];
            //LogEvent event = new LogEvent(logTime, statusCode);

            collector.emit(new Values(logTime, statusCode, e2eTimestamp, processingTimestamp));
            collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(tuple);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logTime", "statusCode", "e2eTimestamp", "processingTimestamp"));
    }
}
