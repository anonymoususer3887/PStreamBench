package com.kom.dsp.synthetic;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Keeps tuples whose numeric field is below a fixed threshold. Used for the
 * synthetic filter stages; the threshold makes the selectivity fixed and
 * known (value is uniform in [0,1), so threshold t keeps about t of tuples).
 */
public class FilterBolt extends BaseRichBolt {
    private final String field;
    private final double threshold;
    private transient OutputCollector collector;
    private transient LocalMetrics.Stage stage;

    public FilterBolt(String field, double threshold) {
        this.field = field;
        this.threshold = threshold;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String id = context.getThisComponentId();
        this.stage = LocalMetrics.stage(id, context.getComponentTasks(id).size());
    }

    @Override
    public void execute(Tuple input) {
        long start = System.nanoTime();
        stage.in.incrementAndGet();
        double v = input.getDoubleByField(field);
        if (v < threshold) {
            collector.emit(input, new Values(input.getStringByField("key"), v));
            stage.out.incrementAndGet();
        }
        collector.ack(input);
        stage.busyNanos.addAndGet(System.nanoTime() - start);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", field));
    }
}
