package com.kom.dsp.spikedetection;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SpikeDetectorBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
     this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        AverageValueModel averageValue = (AverageValueModel) tuple.getValueByField("averageValue");
        int sensorId = averageValue.getSensorId();
        float voltage = averageValue.getCurrentValue();
        float average = averageValue.getAverageValue();

        if (voltage > average) {
            collector.emit(new Values(sensorId, voltage, average, tuple.getValueByField("e2eTimestamp"), tuple.getValueByField("processingTimestamp")));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sensorId", "voltage", "average", "e2eTimestamp","processingTimestamp"));
    }
}
