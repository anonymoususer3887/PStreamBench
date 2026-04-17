package com.kom.dsp.tpch;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.Constants;
import com.kom.dsp.common.LoggerBolt;
import com.kom.dsp.utils.ConfigParser;

public class TPCHTopology extends AbstractTopology {
    public TPCHTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology(){
        this.config.registerSerialization(com.kom.dsp.tpch.TPCHEventModel.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("tpchEventParserBolt", new TPCHEventParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        if (Constants.QUERY_ONE == this.getQuery()) {
            
            this.builder.setBolt("filterCalculatorBolt", new FilterCalculatorBolt(), this.getParallelismDegree(2))
                .shuffleGrouping("tpchEventParserBolt");

            this.builder.setBolt("priorityMapperBolt", new PriorityMapperBolt(), this.getParallelismDegree(3))
                .shuffleGrouping("filterCalculatorBolt");

        } else if (Constants.QUERY_TWO == this.getQuery()) {

            this.builder.setBolt("filterCalculatorWindowBolt",
                    new FilterCalculatorWindowBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS), 
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("tpchEventParserBolt", new Fields("discount"));

            this.builder.setBolt("priorityMapperBolt", new PriorityMapperBolt(), this.getParallelismDegree(3))
                .shuffleGrouping("filterCalculatorWindowBolt");
        }

        this.builder.setBolt("sumBolt", new SumBolt(), this.getParallelismDegree(4))
            .fieldsGrouping("priorityMapperBolt", new Fields("orderPriority"));

        this.builder.setBolt("sink", this.getSink(new TPCHMapper()), this.getParallelismDegree(5))
            .shuffleGrouping("sumBolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        TPCHTopology topology = new TPCHTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
