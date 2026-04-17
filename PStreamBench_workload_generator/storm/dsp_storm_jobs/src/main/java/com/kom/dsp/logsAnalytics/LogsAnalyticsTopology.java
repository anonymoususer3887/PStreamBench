package com.kom.dsp.logsAnalytics;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.Constants;
import com.kom.dsp.utils.ConfigParser;

public class LogsAnalyticsTopology extends AbstractTopology {
    public LogsAnalyticsTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("logParserBolt", new LogParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        if (Constants.QUERY_ONE == this.getQuery()) {

            this.builder.setBolt("volumeCounterBolt",
                    new VolumeCounterBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("logParserBolt", new Fields("logTime"));

            this.builder.setBolt("sink", this.getSink(), this.getParallelismDegree(3))
                .shuffleGrouping("volumeCounterBolt");

        } else if (Constants.QUERY_TWO == this.getQuery()) {

            this.builder.setBolt("statusCounterBolt",
                    new StatusCounterBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("logParserBolt", new Fields("statusCode"));

            this.builder.setBolt("sink", this.getSink(), this.getParallelismDegree(3))
                .shuffleGrouping("statusCounterBolt");

        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        LogsAnalyticsTopology topology = new LogsAnalyticsTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
