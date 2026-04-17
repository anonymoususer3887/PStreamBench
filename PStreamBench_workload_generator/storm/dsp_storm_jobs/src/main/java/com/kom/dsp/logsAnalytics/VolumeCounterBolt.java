package com.kom.dsp.logsAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;
/**
 * Created by Rafik Youssef on 05.06.2024.
 * This class is responsible for counting the number of visits per minute.
 */
public class VolumeCounterBolt extends BaseWindowedBolt {
    private Map<String, Integer> volumeCounts;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.volumeCounts = new HashMap<>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()) {
            String logTime = tuple.getStringByField("logTime");
            //String minute = logTime.substring(0, 17);

            Integer currentCount = volumeCounts.getOrDefault(logTime, 0);
            volumeCounts.put(logTime, currentCount+1);

            collector.emit(new Values(logTime + ": " + currentCount, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message", "e2eTimestamp", "processingTimestamp"));
    }
}
