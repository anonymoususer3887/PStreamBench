package com.kom.dsp.synthetic;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;

/**
 * Fixed synthetic N-way windowed join (-query 2..6):
 *   N sources -> [filter per source, for 2- and 3-way] -> windowed join (N-1 chained joins
 *   in one Storm JoinBolt) -> window-agg (avg of v0 per key) -> sink
 * The total -eventRate is split evenly across the N sources. Keys are drawn from a key domain K
 * (see joinKeySpace): calibrated by default so the expected matches per window are a fixed
 * target for every N; -keySpace <int> fixes K instead (e.g. 1000000 for sparse output).
 * parallelism indices: 0..N-1 sources, then (if pre-filtered) N..2N-1 filters,
 * then join, window-agg, sink.
 */
public class JoinTopology extends SyntheticTopology {
    static final double PREFILTER_THRESHOLD = 0.5;
    private static final String[] NAMES = {"", "", "TwoWayJoin", "ThreeWayJoin", "FourWayJoin", "FiveWayJoin", "SixWayJoin"};
    private final int sources;
    private final boolean prefilter;

    public JoinTopology(Config config) {
        super(config);
        this.sources = Integer.parseInt((String) config.getOrDefault("topology.query", "2"));
        if (sources < 2 || sources > 6) {
            throw new IllegalArgumentException("join supports 2 to 6 sources, got " + sources);
        }
        this.prefilter = sources <= 3;
        this.application = NAMES[sources];
        for (int i = 0; i < sources; i++) {
            this.spoutIds.add("spout-" + i);
        }
        if (prefilter) {
            for (int i = 0; i < sources; i++) {
                this.interiorStages.add("filter-" + i);
            }
        }
        this.interiorStages.add("join");
        this.interiorStages.add("window-agg");
    }

    /** Fraction of tuples each pre-filter keeps (value < PREFILTER_THRESHOLD of uniform [0,1)). */
    double preFilterSelectivity() {
        return prefilter ? PREFILTER_THRESHOLD : 1.0;
    }

    /**
     * Key domain K for this join width. Calibrated (default): the target is final join output
     * matches per window, so K is chosen from the post-filter per-source tuples per window, n_w:
     * expected matches = n_w^N / K^(N-1) = target, so K = (n_w^N / target)^(1/(N-1)).
     * Fixed: use -keySpace <int> (the sparse-output variant).
     */
    int joinKeySpace(int perSourceRate) {
        String setting = (String) config.getOrDefault("topology.keySpace", "calibrated");
        if (!setting.equalsIgnoreCase("calibrated")) {
            return Integer.parseInt(setting);
        }
        double windowSeconds = Long.parseLong((String) config.getOrDefault("topology.windowSize", "0")) / 1000.0;
        double target = Double.parseDouble((String) config.getOrDefault("topology.targetMatches", "1000"));
        double postFilterPerWindow = perSourceRate * preFilterSelectivity() * windowSeconds;
        double k = Math.pow(Math.pow(postFilterPerWindow, sources) / target, 1.0 / (sources - 1));
        return (int) Math.max(2, Math.round(k));
    }

    @Override
    public void buildTopology() {
        int perSourceRate = Math.max(1, configuredEventRate() / sources);
        int keySpace = joinKeySpace(perSourceRate);
        boolean calibrated = ((String) config.getOrDefault("topology.keySpace", "calibrated")).equalsIgnoreCase("calibrated");
        extraMeta.put("synthetic_variant", calibrated ? "calibrated" : "sparse");
        extraMeta.put("join_key_space", String.valueOf(keySpace));
        extraMeta.put("target_matches_per_window",
                calibrated ? (String) config.getOrDefault("topology.targetMatches", "1000") : "none");
        extraMeta.put("filter_selectivity", String.valueOf(preFilterSelectivity()));
        extraMeta.put("effective_join_input_rate",
                String.valueOf(perSourceRate * preFilterSelectivity() * sources));
        String[] inputs = new String[sources];
        for (int i = 0; i < sources; i++) {
            String valueField = "v" + i;
            this.builder.setSpout("spout-" + i, this.getSpout(valueField, keySpace, perSourceRate),
                    this.getParallelismDegree(i));
            if (prefilter) {
                String name = "filter-" + i;
                this.builder.setBolt(name, new FilterBolt(valueField, PREFILTER_THRESHOLD),
                        this.getParallelismDegree(sources + i)).shuffleGrouping("spout-" + i);
                inputs[i] = name;
            } else {
                inputs[i] = "spout-" + i;
            }
        }

        // One JoinBolt chains the N-1 pairwise joins: each new stream joins the one before it.
        // Output: the shared key once, plus each source's own value field (v0..vN-1).
        StringBuilder select = new StringBuilder(inputs[0] + ":key");
        for (int i = 0; i < sources; i++) {
            select.append(", ").append(inputs[i]).append(":v").append(i);
        }
        JoinBolt join = new MeteredJoinBolt(inputs[0], "key");
        for (int i = 1; i < sources; i++) {
            join.join(inputs[i], "key", inputs[i - 1]);
        }
        join.select(select.toString());
        applyJoinWindow(join);

        int joinIndex = prefilter ? 2 * sources : sources;
        BoltDeclarer joinDeclarer = this.builder.setBolt("join", join, this.getParallelismDegree(joinIndex));
        for (String input : inputs) {
            joinDeclarer.fieldsGrouping(input, new Fields("key"));
        }

        // JoinBolt output fields are qualified by their source stream (e.g. "filter-0:key").
        String joinKey = inputs[0] + ":key";
        this.builder.setBolt("window-agg", windowAgg(joinKey, inputs[0] + ":v0"), this.getParallelismDegree(joinIndex + 1))
                .fieldsGrouping("join", new Fields(joinKey));

        this.builder.setBolt(sinkId, this.getAvgSink(), this.getParallelismDegree(joinIndex + 2))
                .shuffleGrouping("window-agg");
    }

    /** Tumbling if slide is unset or equal to size, sliding otherwise (same rule as windowAgg). */
    private void applyJoinWindow(JoinBolt join) {
        long size = Long.parseLong((String) config.getOrDefault("topology.windowSize", "0"));
        long slide = Long.parseLong((String) config.getOrDefault("topology.windowSlide", "0"));
        if (slide > 0 && slide != size) {
            join.withWindow(windowSize(), windowSlide());
        } else {
            join.withTumblingWindow(windowSize());
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromArgs(args);

        JoinTopology topology = new JoinTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        try {
            topology.runTopology(waitTimeToCancel);
            if (topology.isLocal()) {
                topology.writeLocalMetrics(waitTimeToCancel);
            }
        } catch (Throwable t) {
            // See LinearQueryTopology.main() for why this is here: the bare
            // "finally { System.exit(0); }" below used to swallow every
            // submission exception silently (System.exit() in finally runs
            // before Java ever gets to print an uncaught exception).
            t.printStackTrace();
            System.exit(1);
        } finally {
            // Storm's local cluster leaves non-daemon threads behind, including after a
            // rejected topology; exit explicitly so the process never hangs.
            System.exit(0);
        }
    }
}
