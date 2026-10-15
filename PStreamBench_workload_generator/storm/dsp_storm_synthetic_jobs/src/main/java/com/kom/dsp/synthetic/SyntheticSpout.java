package com.kom.dsp.synthetic;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * In-process random tuple source for synthetic benchmark queries. Emits
 * (key, valueField) tuples at a total rate of eventRate tuples/s, split evenly
 * across all instances of this spout. The key is drawn from numKeys values,
 * and the value is uniform in [0,1). No Kafka or dataset is needed, so the same
 * topology runs unchanged on a local cluster and on CloudLab.
 */
public class SyntheticSpout extends BaseRichSpout {
    public static final int DEFAULT_KEYS = 1000;
    private static final int MAX_BATCH = 1000;

    private final int eventRate;
    private final String valueField;
    private final int numKeys;
    private transient SpoutOutputCollector collector;
    private transient Random random;
    private transient double perTaskRate;
    private transient long startNanos;
    private transient long emitted;
    private transient LocalMetrics.Stage stage;

    public SyntheticSpout(int eventRate) {
        this(eventRate, "value", DEFAULT_KEYS);
    }

    public SyntheticSpout(int eventRate, String valueField, int numKeys) {
        if (eventRate <= 0) {
            throw new IllegalArgumentException("synthetic source needs topology.eventRate > 0, got " + eventRate);
        }
        this.eventRate = eventRate;
        this.valueField = valueField;
        this.numKeys = numKeys;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        String id = context.getThisComponentId();
        int tasks = context.getComponentTasks(id).size();
        this.stage = LocalMetrics.stage(id, tasks);
        this.perTaskRate = (double) eventRate / tasks;
        this.startNanos = System.nanoTime();
        this.emitted = 0;
    }

    @Override
    public void nextTuple() {
        // Emit what the target rate has accrued since the last call, in bounded batches
        // so one call can catch up without starving the rest of the topology. With nothing
        // owed, return and let Storm's spout wait strategy back off.
        double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
        int owed = (int) Math.min(MAX_BATCH, elapsedSeconds * perTaskRate - emitted);
        if (owed > 0) {
            long start = System.nanoTime();
            LocalMetrics.markEmit(start);
            for (int i = 0; i < owed; i++) {
                String key = String.valueOf(random.nextInt(numKeys));
                double value = random.nextDouble();
                collector.emit(new Values(key, value));
            }
            emitted += owed;
            stage.out.addAndGet(owed);
            stage.busyNanos.addAndGet(System.nanoTime() - start);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", valueField));
    }
}
