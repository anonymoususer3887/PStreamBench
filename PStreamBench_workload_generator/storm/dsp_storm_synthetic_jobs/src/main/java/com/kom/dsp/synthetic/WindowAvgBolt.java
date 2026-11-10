package com.kom.dsp.synthetic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

/**
 * Windowed aggregation: for each window, emits the average "value" per "key".
 * The window is configured by the caller (tumbling or sliding), so the same
 * bolt serves every synthetic shape that has a window stage.
 */
public class WindowAvgBolt extends BaseWindowedBolt {
    private final String keyField;
    private final String valueField;
    private transient OutputCollector collector;
    private transient LocalMetrics.Stage stage;

    public WindowAvgBolt() {
        this("key", "value");
    }

    /** @param valueField the numeric field to average. */
    public WindowAvgBolt(String valueField) {
        this("key", valueField);
    }

    /**
     * @param keyField   grouping key field name (join outputs are qualified, e.g. "filter-0:key").
     * @param valueField the numeric field to average (join outputs are qualified, e.g. "filter-0:v0").
     */
    public WindowAvgBolt(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String id = context.getThisComponentId();
        this.stage = LocalMetrics.stage(id, context.getComponentTasks(id).size());
    }

    @Override
    public void execute(TupleWindow window) {
        // Root-caused live, 2026-10-27 (external review of stored result
        // files, not a live-cluster finding): the old version anchored
        // EVERY per-key output to window.get() -- the FULL window, every
        // key's tuples, not just this key's own. With K distinct keys in
        // the window, that's K emits each anchored to the same N tuples,
        // multiplying the acker's tracking work by K instead of the
        // anchoring being O(N) total. Under sustained load this made the
        // ackers fall behind, pushing real, flowing tuples past
        // topology.message.timeout.secs before their ack tree could
        // complete -- confirmed via spout_acked=0 with spout_failed in
        // the millions despite genuine throughput (TwoFilters/100000/
        // level4: 0 acked, 7.38M failed). Anchoring each key's output
        // only to THAT key's own input tuples makes the anchoring cost
        // O(N) total again, regardless of key cardinality.
        long start = System.nanoTime();
        Map<String, double[]> sumCount = new HashMap<>(); // key -> {sum, count}
        Map<String, List<Tuple>> tuplesByKey = new HashMap<>(); // key -> that key's own input tuples, for anchoring
        for (Tuple t : window.get()) {
            String key = t.getStringByField(keyField);
            double[] acc = sumCount.computeIfAbsent(key, k -> new double[2]);
            acc[0] += t.getDoubleByField(valueField);
            acc[1] += 1;
            tuplesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }
        stage.in.addAndGet(window.get().size());
        for (Map.Entry<String, double[]> e : sumCount.entrySet()) {
            double avg = e.getValue()[0] / e.getValue()[1];
            collector.emit(tuplesByKey.get(e.getKey()), new Values(e.getKey(), avg));
        }
        stage.out.addAndGet(sumCount.size());
        stage.busyNanos.addAndGet(System.nanoTime() - start);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "avg"));
    }
}
