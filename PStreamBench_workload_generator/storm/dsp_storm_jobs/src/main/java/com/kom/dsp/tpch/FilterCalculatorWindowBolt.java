package com.kom.dsp.tpch;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class FilterCalculatorWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event", "e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        // Variables to hold min and max timestamps
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        for (Tuple tuple : tupleWindow.get()) {
            // Get timestamps from the tuple
            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));

            TPCHEventModel event = (TPCHEventModel) tuple.getValueByField("tpchEvent");

            // Filter the event based on the order priority
            if (event.getOrderPriority() > 2) {
                // Emit the filtered event along with timestamps
                collector.emit(new Values(event, e2eTimestamp, processingTimestamp));
            }
        }
    }
}
