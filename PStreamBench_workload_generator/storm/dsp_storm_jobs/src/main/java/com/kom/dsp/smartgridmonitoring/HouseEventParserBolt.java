package com.kom.dsp.smartgridmonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;


public class HouseEventParserBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getStringByField("value");

        long e2eTimestamp = tuple.getLongByField("e2eTimestamp");

        line = line.replace("\"", "");
        String[] fields = line.split(",");
        if (fields.length == 7) {
            long house_id = Long.parseLong(fields[6]);
            long household_id = Long.parseLong(fields[5]);
            long plug_id = Long.parseLong(fields[4]);
            double energyConsumption = Double.parseDouble(fields[2]);

            collector.emit(new Values(house_id, household_id, plug_id, energyConsumption, e2eTimestamp));
        } else {
            System.out.println("wrong input length");
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id", "household_id", "plug_id", "energyConsumption", "e2eTimestamp"));
    }
}
