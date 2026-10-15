package com.kom.dsp.synthetic;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Minimal sink for the source-probe topology: does no work beyond counting and
 * acking, so it can never be the bottleneck. Used to measure a spout's real
 * ceiling in isolation, with nothing downstream to backpressure it.
 */
public class DrainSinkBolt extends BaseRichBolt {
    private transient LocalMetrics.Stage stage;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        String id = context.getThisComponentId();
        this.stage = LocalMetrics.stage(id, context.getComponentTasks(id).size());
    }

    @Override
    public void execute(Tuple input) {
        stage.in.incrementAndGet();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // terminal sink, emits nothing
    }
}
