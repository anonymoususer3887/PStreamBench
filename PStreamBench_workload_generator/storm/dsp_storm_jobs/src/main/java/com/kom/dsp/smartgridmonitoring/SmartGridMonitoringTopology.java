package com.kom.dsp.smartgridmonitoring;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.utils.ConfigParser;
import com.kom.dsp.wordcount.CounterBolt;
import com.kom.dsp.common.Constants;
import com.kom.dsp.common.bolts.QueryMetricsBolt;

public class SmartGridMonitoringTopology extends AbstractTopology {

    public SmartGridMonitoringTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {

        BaseRichSpout spout = getSpout();

        int houseEventParserParallelism = this.getParallelismDegree(1);
        int globalMedianParallelism = this.getParallelismDegree(2);
        int plugMedianParallelism = this.getParallelismDegree(2);
        //int outlierDetectorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        //int houseLoadPredictorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        //int plugLoadPredictorParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        int windowSize = this.getWindowSize();
        int windowSlide = this.getWindowSlide();

        //this.parallelism = (int) Math.round((houseEventParserParallelism + globalMedianParallelism + plugMedianParallelism + outlierDetectorParallelism + houseLoadPredictorParallelism + plugLoadPredictorParallelism) / 6.0);
        this.parallelism = (int) Math.round((houseEventParserParallelism + globalMedianParallelism + plugMedianParallelism) / 3.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        this.builder.setSpout("spout", spout, this.getParallelismDegree(0));

        this.builder.setBolt("house-event-parser-bolt", new HouseEventParserBolt(), houseEventParserParallelism)
            .shuffleGrouping("spout");

        if (Constants.QUERY_ONE == this.getQuery()) {

            this.builder.setBolt("global-median-bolt",
                    new GlobalMedianBolt().withWindow(
                        new Duration(windowSize, TimeUnit.MILLISECONDS),
                        new Duration(windowSlide, TimeUnit.MILLISECONDS)),
                    globalMedianParallelism)
                .fieldsGrouping("house-event-parser-bolt", new Fields("house_id"));
                //.shuffleGrouping("house-event-parser-bolt");

            //this.builder.setBolt("metrics", new QueryMetricsBolt(this.getMainIp() + ":9091").withTumblingWindow(
            //            new Duration(100, TimeUnit.MILLISECONDS)), 1).shuffleGrouping("global-median-bolt");

            this.builder.setBolt("sink", this.getSink(new SmartGridGlobalMedianMapper()), this.getParallelismDegree(3))
                .shuffleGrouping("global-median-bolt");

        } else if (Constants.QUERY_TWO == this.getQuery()) {

            this.builder.setBolt("plug-median-bolt", new PlugMedianBolt(), plugMedianParallelism)
                .fieldsGrouping("house-event-parser-bolt", new Fields("house_id", "household_id", "plug_id"));

            //this.builder.setBolt("metrics", new QueryMetricsBolt(this.getMainIp() + ":9091").withTumblingWindow(
            //            new Duration(100, TimeUnit.MILLISECONDS)), 1).shuffleGrouping("plug-median-bolt");

            this.builder.setBolt("sink", this.getSink(new SmartGridPlugMedianMapper()), this.getParallelismDegree(3))
                .shuffleGrouping("plug-median-bolt");

        }

        ////outlier detection
        //builder.setBolt("outlier-detector-bolt", new OutlierDetectorBolt(), this.getParallelismDegree(3))
        //    .fieldsGrouping("global-median-bolt",new Fields("house_id"))
        //    .fieldsGrouping("plug-median-bolt", new Fields("house_id"));

        //// load prediction
        //builder.setBolt("house-load-predictor-bolt", new HouseLoadPredictorBolt(), this.getParallelismDegree(4))
        //        .fieldsGrouping("house-event-parser-bolt", new Fields("house_id"));
        //builder.setBolt("plug-load-predictor-bolt", new PlugLoadPredictorBolt(), this.getParallelismDegree(5))
        //        .fieldsGrouping("house-event-parser-bolt", new Fields("house_id", "household_id", "plug_id"));

        //builder.setBolt("logger-bolt", new LoggerBolt(), this.getParallelismDegree(6))
        //        .shuffleGrouping("outlier-detector-bolt")
        //        .shuffleGrouping("house-load-predictor-bolt")
        //        .shuffleGrouping("plug-load-predictor-bolt");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        SmartGridMonitoringTopology topology = new SmartGridMonitoringTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
