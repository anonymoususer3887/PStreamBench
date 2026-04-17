package com.kom.dsp.adAnalytics;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.Constants;
import com.kom.dsp.common.bolts.NoopBolt;
import com.kom.dsp.common.bolts.QueryMetricsBolt;
import com.kom.dsp.utils.ConfigParser;

public class AdAnalyticsTopology extends AbstractTopology {

    public AdAnalyticsTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        int windowSize = this.getWindowSize();
        int windowSlide = this.getWindowSlide();

        this.config.registerSerialization(AdEvent.class);

        if (Constants.QUERY_ONE == this.getQuery()) {

            this.builder.setSpout("clicks-spout", this.getSpout("clicks"), this.getParallelismDegree(0));
            this.builder.setSpout("impressions-spout", this.getSpout("impressions"), this.getParallelismDegree(0));

            this.builder.setBolt("clicks-parser", new Q1ClicksParserBolt(), this.getParallelismDegree(1))
                .shuffleGrouping("clicks-spout");
            this.builder.setBolt("impressions-parser", new Q1ImpressionsParserBolt(), this.getParallelismDegree(2))
                .shuffleGrouping("impressions-spout");

            this.builder.setBolt("clicks-counter", new Q1CounterBolt(), this.getParallelismDegree(3))
                .fieldsGrouping("clicks-parser", new Fields("queryId", "adId"));
                //.shuffleGrouping("clicks-parser");
            this.builder.setBolt("impressions-counter", new Q1CounterBolt(), this.getParallelismDegree(4))
                .fieldsGrouping("impressions-parser", new Fields("queryId", "adId"));
                //.shuffleGrouping("impressions-parser");

            this.builder.setBolt("join", new JoinBolt("clicks-counter", "key")
                    .join("impressions-counter", "key", "clicks-counter")
                    .select("impressions-counter:queryId, impressions-counter:adId," +
                        "impressions-counter:count, clicks-counter:count," +
                        "impressions-counter:e2eTimestamp, impressions-counter:processingTimestamp")
                    .withWindow(
                        new Duration(windowSize, TimeUnit.MILLISECONDS),
                        new Duration(windowSlide, TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(5))
                .fieldsGrouping("impressions-counter", new Fields("key"))
                .fieldsGrouping("clicks-counter", new Fields("key"));
                //.shuffleGrouping("clicks-counter")
                //.shuffleGrouping("impressions-counter");

            this.builder.setBolt("rolling-ctr", new Q1RollingCTRCalculatorBolt(), this.getParallelismDegree(5))
                .shuffleGrouping("join");

            //this.builder.setBolt("metrics", new QueryMetricsBolt(this.getMainIp() + ":9091").withTumblingWindow(
            //            new Duration(100, TimeUnit.MILLISECONDS)), 1).shuffleGrouping("rolling-ctr");

            this.builder.setBolt("sink", this.getSink(new Q1RollingCTRMapper()), this.getParallelismDegree(6))
                .shuffleGrouping("rolling-ctr");

        } else if (Constants.QUERY_TWO == this.getQuery()) {

            this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

            this.builder.setBolt("parser", new Q2AdEventParserBolt(), this.getParallelismDegree(1))
                .shuffleGrouping("spout");

            this.builder.setBolt("counter", new Q2AdEventCounterBolt(), this.getParallelismDegree(2))
                .fieldsGrouping("parser", new Fields("queryId", "adId"));

            this.builder.setBolt("rolling-ctr", new Q2RollingCTRCalculatorBolt()
                    .withWindow(
                        new Duration(windowSize, TimeUnit.MILLISECONDS),
                        new Duration(windowSlide, TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(3))
                .shuffleGrouping("counter");

            this.builder.setBolt("metrics", new QueryMetricsBolt(this.getMainIp() + ":9091").withTumblingWindow(
                        new Duration(100, TimeUnit.MILLISECONDS)), 1).shuffleGrouping("rolling-ctr");

            this.builder.setBolt("sink", this.getSink(new Q2RollingCTRMapper()), this.getParallelismDegree(4))
                .shuffleGrouping("rolling-ctr");
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        AdAnalyticsTopology topology = new AdAnalyticsTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
