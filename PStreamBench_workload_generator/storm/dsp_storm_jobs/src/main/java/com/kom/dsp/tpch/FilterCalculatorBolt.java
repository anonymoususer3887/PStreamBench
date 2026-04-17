package com.kom.dsp.tpch;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterCalculatorBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // Get timestamps from the tuple
        long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
        long processingTimestamp = tuple.getLongByField("processingTimestamp");

        TPCHEventModel event = (TPCHEventModel) tuple.getValueByField("tpchEvent");

        // Filter the event based on the order priority
        if (event.getOrderPriority() > 2) {
            // Emit the filtered event along with timestamps
            collector.emit(new Values(event, e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event", "e2eTimestamp", "processingTimestamp"));
    }
}
