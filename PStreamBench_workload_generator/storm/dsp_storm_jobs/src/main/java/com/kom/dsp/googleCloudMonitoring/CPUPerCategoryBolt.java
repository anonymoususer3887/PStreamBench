package com.kom.dsp.googleCloudMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CPUPerCategoryBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        Map<Integer, Long> minTimestamps = new HashMap<>();
        Map<Integer, Float> sums = new HashMap<>();

        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        for (Tuple tuple: tuples) {
            TaskEvent taskEvent = (TaskEvent) tuple.getValueByField("taskEvent");
            int category = taskEvent.getCategory();

            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));

            if (minTimestamps.getOrDefault(category, Long.MAX_VALUE) >= taskEvent.getTimestamp()) {
                minTimestamps.put(category, taskEvent.getTimestamp());
            }

            sums.put(category, sums.getOrDefault(category, 0f) + taskEvent.getCpu());
        }

        for (int category : sums.keySet()) {
            long timestamp = minTimestamps.get(category);
            float sum = sums.get(category);
            collector.emit(new Values(new CPUPerCategory(timestamp, category, sum), e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cpuPerCategory", "e2eTimestamp", "processingTimestamp"));
    }
}
