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

import java.util.HashMap;
import java.util.Map;

public class RepeatVisitOperatorBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    private Map<String, Boolean> visitTracker;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //super.prepare(stormConf, context, collector);
        this.outputCollector = collector;
        this.visitTracker = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()) {
            long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            long processingTimestamp = tuple.getLongByField("processingTimestamp");

            ClickLog clickLog = (ClickLog) tuple.getValueByField("clickLog");
            String url = clickLog.getUrl();
            String clientKey = tuple.getStringByField("clientKey");

            String mapKey = url + clientKey;
            boolean visited = visitTracker.getOrDefault(mapKey, false);

            if (!visited) {
                visitTracker.put(mapKey, true);
                outputCollector.emit(new Values(url, 1, 1, e2eTimestamp, processingTimestamp));
            } else {
                outputCollector.emit(new Values(url, 1, 0, e2eTimestamp, processingTimestamp));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "totalVisitCounter", "uniqueVisitCounter", "e2eTimestamp", "processingTimestamp"));
    }
}
