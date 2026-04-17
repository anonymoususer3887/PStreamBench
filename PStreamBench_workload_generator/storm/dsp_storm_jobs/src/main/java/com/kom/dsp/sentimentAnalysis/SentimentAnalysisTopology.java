package com.kom.dsp.sentimentAnalysis;

import org.apache.storm.Config;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;

public class SentimentAnalysisTopology extends AbstractTopology {

    public SentimentAnalysisTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        builder.setBolt("parse-tweet-bolt", new TweetParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        builder.setBolt("analyze-sentiment-bolt", new TweetAnalyzerBolt(), this.getParallelismDegree(2))
            .shuffleGrouping("parse-tweet-bolt");

        builder.setBolt("sink", this.getSink(new TweetAnalyzerMapper()), this.getParallelismDegree(3))
            .shuffleGrouping("analyze-sentiment-bolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        SentimentAnalysisTopology topology = new SentimentAnalysisTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
