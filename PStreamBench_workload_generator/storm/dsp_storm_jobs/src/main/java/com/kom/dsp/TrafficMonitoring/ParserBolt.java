package com.kom.dsp.TrafficMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ParserBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();

        try {
            long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            String line = tuple.getStringByField("value");

            // Split the data into fields
            String[] fields = line.split(",");
            TrafficEvent event;
            if(fields.length == 7){
                String carId  = fields[0];

                String timestamp = fields[2];
                boolean occ = true;
                double lat = Double.parseDouble(fields[3]);
                double lon = Double.parseDouble(fields[4]);
                double speed = Double.parseDouble(fields[5]);
                double bearing = Double.parseDouble(fields[6]);
                event = new TrafficEvent(carId, timestamp, occ, lat, lon, speed, bearing);
            } else {
                event = new TrafficEvent();
            }
            outputCollector.emit(new Values(event, e2eTimestamp, processingTimestamp));
            outputCollector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("trafficEvent", "e2eTimestamp", "processingTimestamp"));
    }
}
