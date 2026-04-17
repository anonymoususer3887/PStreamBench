package com.kom.dsp.spikedetection;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class AverageCalculatorBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        Map<Integer, Float> sensorSums = new HashMap<>();
        Map<Integer, Integer> sensorCounts = new HashMap<>();
        Map<Integer, Float> averages = new HashMap<>();

        for (Tuple tuple : tupleWindow.get()) {
            e2eTimestamp = Math.min(tuple.getLongByField("e2eTimestamp"), e2eTimestamp);
            processingTimestamp = Math.max(tuple.getLongByField("processingTimestamp"), processingTimestamp);

            SensorMeasurementModel measurement = (SensorMeasurementModel) tuple.getValueByField("measurement");
            int sensorId = measurement.getSensorId();
            float voltage = measurement.getVoltage();

            sensorSums.put(sensorId, sensorSums.getOrDefault(sensorId, 0.0f) + voltage);
            sensorCounts.put(sensorId, sensorCounts.getOrDefault(sensorId, 0) + 1);
        }

        for (Tuple tuple : tupleWindow.get()) {
            SensorMeasurementModel measurement = (SensorMeasurementModel) tuple.getValueByField("measurement");
            int sensorId = measurement.getSensorId();
            float voltage = measurement.getVoltage();

            float average = sensorSums.get(sensorId) / sensorCounts.get(sensorId);

            AverageValueModel result = new AverageValueModel(sensorId, voltage, average);
            collector.emit(new Values(sensorId, result, e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sensorId", "averageValue", "e2eTimestamp", "processingTimestamp"));
    }
}
