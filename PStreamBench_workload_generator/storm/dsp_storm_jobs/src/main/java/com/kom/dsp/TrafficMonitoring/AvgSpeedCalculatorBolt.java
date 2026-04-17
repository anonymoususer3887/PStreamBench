package com.kom.dsp.TrafficMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class AvgSpeedCalculatorBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private Map<Long, Long> counts;
    private Map<Long, Double> totalSpeeds;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        counts = new HashMap<>();
        totalSpeeds = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow window) {
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        for (Tuple tuple : window.get()) {
            e2eTimestamp = Math.min(tuple.getLongByField("e2eTimestamp"), e2eTimestamp);
            processingTimestamp = Math.max(tuple.getLongByField("processingTimestamp"), processingTimestamp);

            Long roadId = tuple.getLongByField("roadId");
            TrafficEventWithRoad event = (TrafficEventWithRoad) tuple.getValueByField("roadEvent");

            counts.put(roadId, counts.getOrDefault(roadId, 0L) + 1);
            totalSpeeds.put(roadId, totalSpeeds.getOrDefault(roadId, 0.0) + event.getSpeed());
        }

        for (Long roadId : counts.keySet()) {
            long count = counts.get(roadId);
            double totalSpeed = totalSpeeds.get(roadId);

            double avgSpeed = totalSpeed / count;

            if (avgSpeed != 0) {
                collector.emit(new Values(roadId, avgSpeed, e2eTimestamp, processingTimestamp));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("roadId", "avgSpeed", "e2eTimestamp", "processingTimestamp"));
    }
}
