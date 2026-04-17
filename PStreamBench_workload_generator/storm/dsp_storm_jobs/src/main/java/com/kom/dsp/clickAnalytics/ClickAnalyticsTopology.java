package com.kom.dsp.clickAnalytics;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.Constants;
import com.kom.dsp.utils.ConfigParser;

public class ClickAnalyticsTopology extends AbstractTopology {
    public ClickAnalyticsTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        this.config.registerSerialization(com.kom.dsp.clickAnalytics.ClickLog.class);
        this.config.registerSerialization(com.kom.dsp.clickAnalytics.GeoStats.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("parse-click-log-bolt", new ClickLogParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        if (Constants.QUERY_ONE == this.getQuery()) {

            this.builder.setBolt("repeat-visit-operator-bolt",
                    new RepeatVisitOperatorBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("parse-click-log-bolt", new Fields("clientKey"));

            this.builder.setBolt("sum-reducer-bolt",
                    new SumReducerBolt(),
                    this.getParallelismDegree(3))
                .fieldsGrouping("repeat-visit-operator-bolt", new Fields("url"));

            this.builder.setBolt("sink", this.getSink(new SumReducerMapper()), this.getParallelismDegree(4))
                .shuffleGrouping("sum-reducer-bolt");

        } else if (Constants.QUERY_TWO == this.getQuery()) {

            this.builder.setBolt("geography-operator-bolt", new GeographyOperatorBolt(), this.getParallelismDegree(2))
                .shuffleGrouping("parse-click-log-bolt");

            this.builder.setBolt("sink", this.getSink(new GeographyMapper()), this.getParallelismDegree(3))
                .shuffleGrouping("geography-operator-bolt");

        } else if (Constants.QUERY_THREE == this.getQuery()) {

            this.builder.setBolt("repeat-visit-operator-bolt",
                    new RepeatVisitOperatorBolt().withWindow(
                        new BaseWindowedBolt.Count(this.getWindowSize()),
                        new BaseWindowedBolt.Count(this.getWindowSlide())),
                    this.getParallelismDegree(2))
                .fieldsGrouping("parse-click-log-bolt", new Fields("clientKey"));

            this.builder.setBolt("sum-reducer-bolt",
                    new SumReducerBolt(),
                    this.getParallelismDegree(3))
                .fieldsGrouping("repeat-visit-operator-bolt", new Fields("url"));

            this.builder.setBolt("sink", this.getSink(new SumReducerMapper()), this.getParallelismDegree(4))
                .shuffleGrouping("sum-reducer-bolt");
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        ClickAnalyticsTopology topology = new ClickAnalyticsTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
