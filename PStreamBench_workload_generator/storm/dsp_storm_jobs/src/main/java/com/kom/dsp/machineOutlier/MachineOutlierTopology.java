package com.kom.dsp.machineOutlier;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;

public class MachineOutlierTopology extends AbstractTopology {
    double threshold;

    public MachineOutlierTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        Integer threshold = Integer.parseInt((String)this.config.getOrDefault("topology.threshold", "0"));

        this.config.registerSerialization(com.kom.dsp.machineOutlier.MachineUsage.class);
        this.config.registerSerialization(com.kom.dsp.machineOutlier.BFPRTAlgorithm.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("machine-usage-parser-bolt", new MachineUsageParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        this.builder.setBolt("machine-outlier-bolt",
                new MachineOutlier2Bolt(threshold).withWindow(
                    new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                    new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                this.getParallelismDegree(2))
            .fieldsGrouping("machine-usage-parser-bolt", new Fields("machineId"));

        this.builder.setBolt("sink", this.getSink(new MachineOutlierMapper()), this.getParallelismDegree(4))
            .shuffleGrouping("machine-outlier-bolt");

    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        MachineOutlierTopology topology = new MachineOutlierTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
