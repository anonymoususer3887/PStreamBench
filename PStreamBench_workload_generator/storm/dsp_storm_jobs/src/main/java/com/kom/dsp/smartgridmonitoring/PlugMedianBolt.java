package com.kom.dsp.smartgridmonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlugMedianBolt extends BaseWindowedBolt {

    private CalculatorBolt calculatorBolt;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.calculatorBolt = new CalculatorBolt();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        long e2eTimestamp = 0;
        long processingTimestamp = 0;

        Map<String, List<Double>> readings = new HashMap<>();
        Map<String, Long> house_ids = new HashMap<>();

        for (Tuple tuple : tupleWindow.get()) {
            long house_id = tuple.getLongByField("house_id");
            long household_id = tuple.getLongByField("household_id");
            long plug_id = tuple.getLongByField("plug_id");

            String id = "PLUG " + house_id + household_id + plug_id;
            house_ids.put(id, house_id);

            List<Double> list = readings.getOrDefault(id, new ArrayList<>());
            list.add(tuple.getDoubleByField("energyConsumption"));
            readings.put(id, list);

            e2eTimestamp = Math.max(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            long tupleProcessingTimestamp = System.currentTimeMillis();
            processingTimestamp = Math.max(processingTimestamp, tupleProcessingTimestamp);
            collector.ack(tuple);
        }

        for (String id : readings.keySet()) {
            double median = calculatorBolt.calculateMedian(readings.get(id));
            collector.emit(new Values(house_ids.get(id), id, median, e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","plugID", "plugMedian","e2eTimestamp", "processingTimestamp"));
    }
}
