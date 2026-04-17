package com.kom.dsp.googleCloudMonitoring;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.Constants;
import com.kom.dsp.common.LoggerBolt;
import com.kom.dsp.utils.ConfigParser;

public class GoogleCloudMonitoringTopology extends AbstractTopology {

    public GoogleCloudMonitoringTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        //this.config.registerSerialization(com.kom.dsp.googleCloudMonitoring.CPUPerCategory.class);
        //this.config.registerSerialization(com.kom.dsp.googleCloudMonitoring.CPUPerJob.class);
        //this.config.registerSerialization(com.kom.dsp.googleCloudMonitoring.TaskEvent.class);

        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));
        // original Flink implementation takes input to determine type of query (CPU per job or CPU per category)
        // here I am calculating both anyway, but we can add a similar input to determine which one to calculate
        builder.setBolt("task-event-parser-bolt", new TaskEventParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        if (Constants.QUERY_ONE == this.getQuery()) {

            builder.setBolt("cpu-per-category-bolt",
                    new CPUPerCategoryBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("task-event-parser-bolt", new Fields("category"));

            builder.setBolt("sink", this.getSink(new CPUPerCategoryMapper()), this.getParallelismDegree(3))
                .shuffleGrouping("cpu-per-category-bolt");

        } else if (Constants.QUERY_TWO == this.getQuery()) {

            builder.setBolt("cpu-per-job-bolt",
                    new CPUPerJobBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("task-event-parser-bolt", new Fields("jobId"));

            builder.setBolt("sink", this.getSink(new CPUPerJobMapper()), this.getParallelismDegree(3))
                .shuffleGrouping("cpu-per-job-bolt");

        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        GoogleCloudMonitoringTopology topology = new GoogleCloudMonitoringTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
