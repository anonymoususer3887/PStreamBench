package com.kom.dsp.TrafficMonitoring;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;

public class TrafficMonitoringTopology extends AbstractTopology {
    public TrafficMonitoringTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        this.config.registerSerialization(com.kom.dsp.TrafficMonitoring.TrafficEvent.class);
        this.config.registerSerialization(com.kom.dsp.TrafficMonitoring.TrafficEventWithRoad.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("ParserBolt", new ParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        this.builder.setBolt("MapMatcherBolt", new MapMatcherBolt(), this.getParallelismDegree(2))
            .shuffleGrouping("ParserBolt");

        this.builder.setBolt("AvgSpeedCalculatorBolt",
                new AvgSpeedCalculatorBolt().withWindow(
                    new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS), 
                    new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                this.getParallelismDegree(3))
            .fieldsGrouping("MapMatcherBolt", new Fields("roadId"));

        this.builder.setBolt("sink", this.getSink(new TrafficMonitoringMapper()), this.getParallelismDegree(4))
            .shuffleGrouping("AvgSpeedCalculatorBolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        TrafficMonitoringTopology topology = new TrafficMonitoringTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
