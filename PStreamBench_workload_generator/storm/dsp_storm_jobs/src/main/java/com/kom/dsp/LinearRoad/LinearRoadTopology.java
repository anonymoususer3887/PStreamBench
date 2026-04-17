package com.kom.dsp.LinearRoad;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import com.kom.dsp.common.AbstractTopology;
import com.kom.dsp.common.Constants;
import com.kom.dsp.common.LoggerBolt;
import com.kom.dsp.utils.ConfigParser;

public class LinearRoadTopology extends AbstractTopology {

    int operationSpecification;

    public LinearRoadTopology(Config config) {
        super(config);
    }

    @Override
    public void buildTopology() {
        this.builder.setSpout("spout", this.getSpout(), this.getParallelismDegree(0));

        this.builder.setBolt("vehicleEvent-parser-bolt", new VehicleEventParserBolt(), this.getParallelismDegree(1))
            .shuffleGrouping("spout");

        // toll notifications
        if (Constants.QUERY_ONE == this.getQuery()) {
            this.builder.setBolt("toll-notification-mapper", new TollNotificationMapperBolt(), this.getParallelismDegree(2))
                .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("vehicleId"));

            this.builder.setBolt("toll-notification-formatter", new TollNotificationFormatterBolt(), this.getParallelismDegree(3))
                .shuffleGrouping("toll-notification-mapper");

            this.builder.setBolt("sink", this.getSink(), this.getParallelismDegree(4))
                .shuffleGrouping("toll-notification-formatter");
        }

        // accident notification
        if (Constants.QUERY_TWO == this.getQuery()) {
            this.builder.setBolt("accident-notification",
                    new AccidentNotificationMapperBolt().withWindow(
                        new BaseWindowedBolt.Duration(this.getWindowSize(), TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(this.getWindowSlide(), TimeUnit.MILLISECONDS)),
                    this.getParallelismDegree(2))
                .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("time"));

            this.builder.setBolt("accident-notification-formatter", new AccidentNotificationFormatterBolt(), this.getParallelismDegree(3))
                .shuffleGrouping("accident-notification");

            this.builder.setBolt("sink", this.getSink(), this.getParallelismDegree(4))
                .shuffleGrouping("accident-notification-formatter");
        }

        // daily expenditure
        if (Constants.QUERY_THREE == this.getQuery()) {
            this.builder.setBolt("daily-expenditure-bolt", new DailyExpenditureCalculatorBolt(), this.getParallelismDegree(2))
                    .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("vehicleId"));

            this.builder.setBolt("sink", this.getSink(), this.getParallelismDegree(3))
                .shuffleGrouping("daily-expenditure-bolt");
        }

        // vehicleReport
        if (Constants.QUERY_FOUR == this.getQuery()) {
            this.builder.setBolt("vehicle-report-mapper", new VehicleReportsMapperBolt(), this.getParallelismDegree(2))
                .fieldsGrouping("vehicleEvent-parser-bolt", new Fields("vehicleId"));


            this.builder.setBolt("logger-bolt", new LoggerBolt(), this.getParallelismDegree(3))
                    .shuffleGrouping("vehicle-report-mapper");
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigParser.fromFile("");
        config.putAll(ConfigParser.fromArgs(args));

        LinearRoadTopology topology = new LinearRoadTopology(config);
        topology.buildTopology();

        Long waitTimeToCancel = Long.parseLong((String) config.getOrDefault("topology.waitTimeToCancel", "600"));
        topology.runTopology(waitTimeToCancel);
    }
}
