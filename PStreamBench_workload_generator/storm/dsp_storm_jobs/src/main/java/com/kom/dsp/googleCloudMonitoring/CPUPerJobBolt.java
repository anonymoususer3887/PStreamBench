package com.kom.dsp.googleCloudMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.jcodings.util.Hash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CPUPerJobBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        Map<Long, Long> minTaskTimestamps = new HashMap<>();
        Map<Long, Float> sums = new HashMap<>();
        Map<Long, Integer> counts = new HashMap<>();

        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        for (Tuple tuple: tuples) {
            TaskEvent taskEvent = (TaskEvent) tuple.getValueByField("taskEvent");
            long jobId = taskEvent.getJobId();

            if (minTaskTimestamps.getOrDefault(jobId, Long.MAX_VALUE) >= taskEvent.getTimestamp()) {
                minTaskTimestamps.put(jobId, taskEvent.getTimestamp());
            }

            sums.put(jobId, sums.getOrDefault(jobId, 0f) + taskEvent.getCpu());
            counts.put(jobId, counts.getOrDefault(jobId, 0) + 1);

            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));
        }

        for (long jobId : sums.keySet()) {
            float avgCpu = sums.get(jobId) / counts.get(jobId);
            long timestamp = minTaskTimestamps.get(jobId);
            collector.emit(new Values(new CPUPerJob(timestamp, jobId, avgCpu), e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cpuPerJob", "e2eTimestamp", "processingTimestamp"));
    }

}
