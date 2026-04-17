package com.kom.dsp.adAnalytics;

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

public class Q1JoinBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        Map<String, List<Tuple>> left = new HashMap<>();
        List<Tuple> right = new ArrayList<>();

        for (Tuple tuple : inputWindow.get()) {
            String type = tuple.getStringByField("type");
            if (type.equals("click")) {
                String key = tuple.getStringByField("key");

                List<Tuple> clickTuples = left.getOrDefault(key, new ArrayList<>());
                clickTuples.add(tuple);
                left.put(key, clickTuples);
            } else if (type.equals("impression")) {
                right.add(tuple);
            }
        }

        for (Tuple rhs : right) {  // impressions
            String key = rhs.getStringByField("key");
            for (Tuple lhs : left.getOrDefault(key, new ArrayList<>())) {  // clicks
                long processingTimestamp = Math.max(lhs.getLongByField("processingTimestamp"), rhs.getLongByField("processingTimestamp"));
                long e2eTimestamp = Math.max(lhs.getLongByField("e2eTimestamp"), rhs.getLongByField("e2eTimestamp"));

                collector.emit(new Values(rhs.getLongByField("queryId"), rhs.getLongByField("adId"),
                        rhs.getIntegerByField("count"), lhs.getIntegerByField("count"), e2eTimestamp,
                        processingTimestamp));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("impressions-counter:queryId", "impressions-counter:adId",
                "impressions-counter:count", "clicks-counter:count", "impressions-counter:e2eTimestamp",
                "impressions-counter:processingTimestamp"));
    }

    @Override
    public void cleanup() {}
}
