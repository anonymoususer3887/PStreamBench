package com.kom.dsp.synthetic;

import org.apache.storm.Config;

/**
 * Diagnostic-only topology: spout -> drain sink, nothing else. Used to measure
 * a spout's real emission ceiling in isolation, with no filter/window/join
 * downstream that could backpressure it -- isolates the question "is this
 * source parallelism enough" from every other confound in the real shapes.
 * Not part of the 9 benchmark shapes; not registered in getprom_storm.py,
 * since this is read via Storm's own REST stats directly, not Prometheus.
 * parallelism indices: 0 spout, 1 sink.
 */
public class SourceProbeTopology extends SyntheticTopology {

    public SourceProbeTopology(Config config) {
        super(config);
        this.application = "SourceProbe";
        this.spoutIds.add("spout");
        this.sinkId = "drain";
    }

    @Override
    public void buildTopology() {
        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));
        this.builder.setBolt(sinkId, new DrainSinkBolt(), this.getParallelismDegree(1))
                .shuffleGrouping("spout");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromArgs(args);

        SourceProbeTopology topology = new SourceProbeTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        try {
            topology.runTopology(waitTimeToCancel);
        } finally {
            System.exit(0);
        }
    }
}
