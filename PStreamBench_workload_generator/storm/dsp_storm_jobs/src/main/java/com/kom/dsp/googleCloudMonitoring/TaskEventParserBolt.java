package com.kom.dsp.googleCloudMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TaskEventParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        long e2eTimestamp = (long) tuple.getValueByField("e2eTimestamp");
        String[] values = tuple.getStringByField("value").split(",");

        try {
            TaskEvent taskEvent = new TaskEvent(
                    Long.parseLong(values[2]),
                    Long.parseLong(values[0]),
                    Long.parseLong(values[1]),
                    Long.parseLong(values[4]),
                    Integer.parseInt(values[18]),
                    (int) Float.parseFloat(values[15]),
                    Integer.parseInt(values[3]),
                    (int) Float.parseFloat(values[7]),
                    Float.parseFloat(values[8]),
                    Float.parseFloat(values[9]),
                    Float.parseFloat(values[12]),
                    Integer.parseInt(values[17])
            );
            collector.emit(new Values(taskEvent, taskEvent.getJobId(), taskEvent.getCategory(), e2eTimestamp, processingTimestamp));
            collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("taskEvent", "jobId", "category", "e2eTimestamp", "processingTimestamp"));
    }
}
