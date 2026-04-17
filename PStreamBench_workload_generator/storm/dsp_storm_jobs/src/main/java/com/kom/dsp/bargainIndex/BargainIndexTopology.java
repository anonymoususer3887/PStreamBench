package com.kom.dsp.bargainIndex;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;

public class BargainIndexTopology extends AbstractTopology {

    public BargainIndexTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        Integer threshold = Integer.parseInt((String)this.config.getOrDefault("topology.threshold", "0"));

        this.config.registerSerialization(com.kom.dsp.bargainIndex.QuoteDataModel.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("parser-bolt", new QuoteDataParserBolt(), this.getParallelismDegree(1)).shuffleGrouping("spout");

        this.builder.setBolt("vwap-calculator-bolt", new VWAPBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)
                ), this.getParallelismDegree(2))
                .fieldsGrouping("parser-bolt", new Fields("symbol"));

        this.builder.setBolt("bargain-index-bolt", new BargainIndexCalculatorBolt(threshold), this.getParallelismDegree(3))
                .fieldsGrouping("vwap-calculator-bolt", new Fields("symbol"));

        this.builder.setBolt("sink", this.getSink(new BargainIndexMapper()), this.getParallelismDegree(4))
            .shuffleGrouping("bargain-index-bolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        BargainIndexTopology topology = new BargainIndexTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
