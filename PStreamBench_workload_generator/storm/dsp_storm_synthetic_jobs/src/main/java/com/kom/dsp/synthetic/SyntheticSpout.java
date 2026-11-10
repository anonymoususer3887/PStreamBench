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
    private transient long nextMessageId;
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
        this.nextMessageId = 0;
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
                // Reliable emit (with a message id) -- previously a bare
                // emit(Values) with no id, which is Storm's UNRELIABLE mode:
                // nothing is ever tracked for completion regardless of
                // acker count or downstream ack() calls (FilterBolt/
                // WindowAvgBolt/the Kafka sink all already anchor+ack
                // correctly -- confirmed by reading each one -- so this was
                // the only broken link). Root cause for both completeLatency
                // always reading 0 and acked/failed counts always reading 0
                // across every single collected Storm run this project has
                // ever done (found 2026-10-25, via storm_spout_vs_bolt.csv
                // audit). msgId only needs to be unique within this task,
                // not globally, so a plain per-task counter is enough.
                collector.emit(new Values(key, value), nextMessageId++);
            }
            emitted += owed;
            stage.out.addAndGet(owed);
            stage.busyNanos.addAndGet(System.nanoTime() - start);
        }
    }

    @Override
    public void ack(Object msgId) {
        // No retry state to clean up (the tuple's own data is regenerated
        // randomly, not replayable from a durable source) -- this override
        // exists only so Storm's ack bookkeeping has somewhere to land;
        // BaseRichSpout's default no-op ack() is otherwise identical, but
        // being explicit here makes the reliable-emit intent unambiguous
        // (same reasoning for fail() below).
    }

    @Override
    public void fail(Object msgId) {
        // Nothing to replay -- see ack() above. A failed/timed-out tuple
        // is simply dropped; it does not affect this spout's own emitted
        // count, only the acked/completeLatency stats downstream consumers
        // read, which is exactly the signal this fix is restoring.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", valueField));
    }
}
