package com.kom.dsp.LinearRoad;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.kom.dsp.utils.KafkaUtils;

import java.util.Map;

// Function to parse the log data into LogEvent objects
public class VehicleEventParserBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();

        try {
            String line = tuple.getStringByField("value");
            long e2eTimestamp = tuple.getLongByField("e2eTimestamp");

            String[] fields = line.split(",");
            if (fields.length == 15) {
                int type = Integer.parseInt(fields[0]);
                long time = Long.parseLong(fields[1]);
                int vehicleId = Integer.parseInt(fields[2]);
                int speed = Integer.parseInt(fields[3]);
                int xway = Integer.parseInt(fields[4]);
                int lane = Integer.parseInt(fields[5]);
                int direction = Integer.parseInt(fields[6]);
                int segment = Integer.parseInt(fields[7]);
                int position = Integer.parseInt(fields[8]);
                int qid = Integer.parseInt(fields[9]);
                int sinit = Integer.parseInt(fields[10]);
                int send = Integer.parseInt(fields[11]);
                int dow = Integer.parseInt(fields[12]);
                int tod = Integer.parseInt(fields[13]);
                int day = Integer.parseInt(fields[14]);

                //VehicleEvent event = new VehicleEvent(type, time, vehicleId, speed, xway, lane, direction, segment, position, qid, sinit, send, dow, tod, day);
                //collector.emit(new Values(vehicleId, event, e2eTimestamp, processingTimestamp));
                collector.emit(new Values(type, time, vehicleId, speed, xway, lane, direction, segment, position, qid, sinit, send, dow, tod, day, e2eTimestamp, processingTimestamp));
                collector.ack(tuple);
            } else {
                System.out.println( "wrong input length");
                collector.fail(tuple);
                //collector.emit(new Values(0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, e2eTimestamp, processingTimestamp));
            }
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("vehicleID", "event", "e2eTimestamp", "processingTimestamp"));
        declarer.declare(new Fields("type", "time", "vehicleId", "speed", "xway", "lane", "direction", "segment", "position", "qid", "sinit", "send", "dow", "tod", "day", "e2eTimestamp", "processingTimestamp"));
    }
}
