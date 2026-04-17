package com.kom.dsp.clickAnalytics;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;
import java.util.HashMap;

// Todo: maybe use IStatefulWindowedBolt instead of BaseWindowedBolt
public class SumReducerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> totalVisitors = new HashMap<>();
    private Map<String, Integer> totalUniqueVisitors = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
        long processingTimestamp = tuple.getLongByField("processingTimestamp");

        String url = tuple.getStringByField("url");
        totalVisitors.put(url, totalVisitors.getOrDefault(url, 0) + tuple.getIntegerByField("totalVisitCounter"));
        totalUniqueVisitors.put(url, totalUniqueVisitors.getOrDefault(url, 0) + tuple.getIntegerByField("uniqueVisitCounter"));

        collector.emit(new Values(url, totalVisitors.get(url), totalUniqueVisitors.get(url), e2eTimestamp, processingTimestamp));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "totalVisitCounter", "uniqueVisitCounter", "e2eTimestamp", "processingTimestamp"));
    }
}
