package com.kom.dsp.spikedetection;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;

public class SpikeDetectionTopology extends AbstractTopology {

    public SpikeDetectionTopology(Config config){
        super(config);
    }

    @Override
    public void buildTopology(){
        int windowSize = this.getWindowSize();
        int windowSlide = this.getWindowSlide();

        this.config.registerSerialization(com.kom.dsp.spikedetection.AverageValueModel.class);
        this.config.registerSerialization(com.kom.dsp.spikedetection.SensorMeasurementModel.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("parser-bolt", new SensorMeasurementParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        this.builder.setBolt("average-calculator-bolt",
                new AverageCalculatorBolt().withWindow(
                    new Duration(windowSize, TimeUnit.MILLISECONDS),
                    new Duration(windowSlide, TimeUnit.MILLISECONDS)),
                this.getParallelismDegree(2))
            .fieldsGrouping("parser-bolt", new Fields("sensorId"));

        this.builder.setBolt("spike-detector-bolt", new SpikeDetectorBolt(), this.getParallelismDegree(3))
            .shuffleGrouping("average-calculator-bolt");

        //this.builder.setBolt("metrics", new QueryMetricsBolt(this.getMainIp() + ":9091").withTumblingWindow(
        //            new Duration(100, TimeUnit.MILLISECONDS)), 1).shuffleGrouping("spike-detector-bolt");

        this.builder.setBolt("sink", this.getSink(new SpikeDetectorMapper()), this.getParallelismDegree(4))
            .shuffleGrouping("spike-detector-bolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        SpikeDetectionTopology topology = new SpikeDetectionTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
