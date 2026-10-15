package com.kom.dsp.synthetic;

import org.apache.storm.Config;

/**
 * Fixed synthetic chained-filters query, with 2, 3 or 4 filters (-query 2/3/4):
 *   spout -> window-agg (avg per key) -> filter1 -> ... -> filterN -> sink
 * Filter i keeps avg < THRESHOLDS[i-1], so each filter removes a further share.
 * parallelism indices: 0 spout, 1 window-agg, 2..N+1 filters, N+2 sink.
 */
public class ChainedFiltersTopology extends SyntheticTopology {
    // Window averages of uniform values cluster near 0.5, so these thresholds each remove a
    // share of tuples (expected selectivities about 0.75, 0.67, 0.67, 0.67 at 100k events/s).
    // Thresholds far above 0.5 would pass everything.
    static final double[] THRESHOLDS = {0.52, 0.50, 0.48, 0.46};
    private final int filters;

    public ChainedFiltersTopology(Config config) {
        super(config);
        this.filters = Integer.parseInt((String) config.getOrDefault("topology.query", "2"));
        if (filters < 2 || filters > 4) {
            throw new IllegalArgumentException("chained filters supports 2, 3 or 4 filters, got " + filters);
        }
        this.application = filters == 2 ? "TwoFilters" : filters == 3 ? "ThreeFilters" : "FourFilters";
        this.spoutIds.add("spout");
        this.interiorStages.add("window-agg");
        for (int i = 1; i <= filters; i++) {
            this.interiorStages.add("filter" + i);
        }
        this.extraMeta.put("synthetic_variant", "fixed");
        StringBuilder thresholds = new StringBuilder();
        for (int i = 0; i < filters; i++) {
            thresholds.append(i > 0 ? "," : "").append("avg<").append(THRESHOLDS[i]);
        }
        this.extraMeta.put("filter_thresholds", thresholds.toString());
    }

    @Override
    public void buildTopology() {
        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("window-agg", windowAgg("value"), this.getParallelismDegree(1))
                .fieldsGrouping("spout", new org.apache.storm.tuple.Fields("key"));

        String previous = "window-agg";
        for (int i = 1; i <= filters; i++) {
            String name = "filter" + i;
            this.builder.setBolt(name, new FilterBolt("avg", THRESHOLDS[i - 1]), this.getParallelismDegree(i + 1))
                    .shuffleGrouping(previous);
            previous = name;
        }

        this.builder.setBolt(sinkId, this.getAvgSink(), this.getParallelismDegree(filters + 2))
                .shuffleGrouping(previous);
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromArgs(args);

        ChainedFiltersTopology topology = new ChainedFiltersTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        try {
            topology.runTopology(waitTimeToCancel);
            if (topology.isLocal()) {
                topology.writeLocalMetrics(waitTimeToCancel);
            }
        } finally {
            System.exit(0);
        }
    }
}
