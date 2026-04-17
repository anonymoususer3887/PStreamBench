package com.kom.dsp.wordcount;

import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;
import com.kom.dsp.common.Constants;

public class WordCountTopology extends AbstractTopology {

    public WordCountTopology(Config config) {
        super(config);
    }

    public void buildTopology() {
        if (Constants.QUERY_ONE == this.getQuery()) {
            this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

            this.builder.setBolt("tokenizer", new SplitterBolt(), this.getParallelismDegree(1)).shuffleGrouping("spout");

            this.builder.setBolt("counter", new CounterBolt(), this.getParallelismDegree(2)).fieldsGrouping("tokenizer", new Fields("word"));
            //builder.setBolt("counter", new CounterBolt(), Integer.valueOf(parallelism_degree[2])).partialKeyGrouping("tokenizer", new Fields("word"));
            //builder.setBolt("counter", new CounterBolt(), this.getParallelismDegree(2)).shuffleGrouping("tokenizer");

            this.builder.setBolt("sink", this.getSink(), this.getParallelismDegree(3)).shuffleGrouping("counter");
        } else {
            throw new IllegalArgumentException("Unsupported query: " + this.config.get("topology.query"));
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        WordCountTopology topology = new WordCountTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
