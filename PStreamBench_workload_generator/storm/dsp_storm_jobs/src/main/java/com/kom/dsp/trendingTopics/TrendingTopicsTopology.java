package com.kom.dsp.trendingTopics;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.LoggerBolt;
import com.kom.dsp.utils.ConfigParser;

import java.util.concurrent.TimeUnit;

public class TrendingTopicsTopology extends AbstractTopology {

    public TrendingTopicsTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        Integer threshold = Integer.parseInt((String)this.config.getOrDefault("topology.threshold", "0"));

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("twitter-parser-bolt", new TwitterparserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        this.builder.setBolt("topic-extractor-bolt", new TopicExtractorBolt(), this.getParallelismDegree(2))
            .shuffleGrouping("twitter-parser-bolt");

        this.builder.setBolt("rolling-counter-sliding-window-bolt",
                new RollingCounterSlidingWindowBolt().withWindow(
                    new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                    new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                this.getParallelismDegree(2))
            .fieldsGrouping("topic-extractor-bolt",new Fields("topic"));

        //this.builder.setBolt("intermediate-ranking-bolt", new IntermediateRankingBolt(), this.getParallelismDegree(3))
        //    .shuffleGrouping("rolling-counter-sliding-window-bolt");

        //this.builder.setBolt("final-ranking-bolt", new FinalRankingBolt(threshold), this.getParallelismDegree(3))
        //    .globalGrouping("intermediate-ranking-bolt");

        this.builder.setBolt("popularity-detector-bolt", new PopularityDetectorBolt(threshold),
                this.getParallelismDegree(3))
            .shuffleGrouping("rolling-counter-sliding-window-bolt");

        this.builder.setBolt("sink", this.getSink(new TopicMapper()), this.getParallelismDegree(4))
            .shuffleGrouping("popularity-detector-bolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        TrendingTopicsTopology topology = new TrendingTopicsTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
