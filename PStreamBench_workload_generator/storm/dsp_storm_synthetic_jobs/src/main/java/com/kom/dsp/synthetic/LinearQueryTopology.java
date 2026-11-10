package com.kom.dsp.synthetic;

import java.util.Arrays;

import org.apache.storm.Config;

/**
 * Fixed synthetic Linear query:
 *   spout -> filter1 (value < 0.5) -> window-agg (avg per key) -> filter2 (avg < 0.25) -> sink
 * parallelism indices: 0 spout, 1 filter1, 2 window-agg, 3 filter2, 4 sink.
 */
public class LinearQueryTopology extends SyntheticTopology {
    static final double FILTER1_THRESHOLD = 0.5;
    static final double FILTER2_THRESHOLD = 0.25;

    public LinearQueryTopology(Config config) {
        super(config);
        this.application = "LinearQuery";
        this.spoutIds.add("spout");
        this.interiorStages.addAll(Arrays.asList("filter1", "window-agg", "filter2"));
        this.extraMeta.put("synthetic_variant", "fixed");
        this.extraMeta.put("filter_thresholds", "value<" + FILTER1_THRESHOLD + ",avg<" + FILTER2_THRESHOLD);
    }

    @Override
    public void buildTopology() {
        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("filter1", new FilterBolt("value", FILTER1_THRESHOLD), this.getParallelismDegree(1))
                .shuffleGrouping("spout");

        this.builder.setBolt("window-agg", windowAgg("value"), this.getParallelismDegree(2))
                .fieldsGrouping("filter1", new org.apache.storm.tuple.Fields("key"));

        this.builder.setBolt("filter2", new FilterBolt("avg", FILTER2_THRESHOLD), this.getParallelismDegree(3))
                .shuffleGrouping("window-agg");

        this.builder.setBolt(sinkId, this.getAvgSink(), this.getParallelismDegree(4))
                .shuffleGrouping("filter2");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromArgs(args);

        LinearQueryTopology topology = new LinearQueryTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        try {
            topology.runTopology(waitTimeToCancel);
            if (topology.isLocal()) {
                topology.writeLocalMetrics(waitTimeToCancel);
            }
        } catch (Throwable t) {
            // Confirmed live, 2026-10-25: the old bare "finally { System.exit(0); }"
            // below silently swallowed EVERY exception from runTopology() (e.g. a
            // submission failure) -- Java runs finally before an exception can
            // propagate to print its stack trace, and System.exit() inside that
            // finally terminates the JVM right there, so the process always exited
            // 0 with zero output regardless of whether submission actually
            // succeeded. Several real "storm jar" submissions this session looked
            // like silent no-ops (process vanished, exit code 0, nothing in any
            // log) purely because of this -- there was no actual way to tell
            // success from failure short of polling Nimbus's own topology list.
            t.printStackTrace();
            System.exit(1);
        } finally {
            // LocalCluster leaves non-daemon threads running after shutdown (and after a
            // rejected topology); exit explicitly so local runs terminate.
            System.exit(0);
        }
    }
}
