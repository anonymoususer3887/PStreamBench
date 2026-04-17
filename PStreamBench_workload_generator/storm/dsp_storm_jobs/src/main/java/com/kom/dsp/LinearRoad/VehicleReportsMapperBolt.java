package com.kom.dsp.LinearRoad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class VehicleReportsMapperBolt extends BaseRichBolt {
    private OutputCollector collector;

    Map<Integer, List<VehicleEvent>> vehicleEventsState;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        vehicleEventsState = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        //VehicleEvent event = (VehicleEvent) tuple.getValueByField("event");
        Integer type = tuple.getIntegerByField("type");
        Long time = tuple.getLongByField("time");
        Integer vehicleId = tuple.getIntegerByField("vehicleId");
        Integer speed = tuple.getIntegerByField("speed");
        Integer xway = tuple.getIntegerByField("xway");
        Integer lane = tuple.getIntegerByField("lane");
        Integer direction = tuple.getIntegerByField("direction");
        Integer segment = tuple.getIntegerByField("segment");
        Integer position = tuple.getIntegerByField("position");
        Integer qid = tuple.getIntegerByField("qid");
        Integer sinit = tuple.getIntegerByField("sinit");
        Integer send = tuple.getIntegerByField("send");
        Integer dow = tuple.getIntegerByField("dow");
        Integer tod = tuple.getIntegerByField("tod");
        Integer day = tuple.getIntegerByField("day");
        VehicleEvent event = new VehicleEvent(type, time, vehicleId, speed, xway, lane, direction, segment, position, qid, sinit, send, dow, tod, day);

        List<VehicleEvent> events = vehicleEventsState.get(vehicleId);


        if (events == null) {
            // If there are no previous events for the vehicle, create a list and store the current event
            events = new ArrayList<>();
            events.add(event);
            vehicleEventsState.put(vehicleId, events);


        } else {
            events.add(event);
            // Update the list of events for the vehicle
            vehicleEventsState.put(vehicleId, events);
        }

        if(event.getType() == 4){
            long time_taken = event.getTime() - events.get(0).getTime();
            String msg = "VehicleID: "+vehicleId+ "timeTravelled" + time_taken;
            collector.emit(new Values(msg, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            collector.ack(tuple);
        } else {
            String msg = "VehicleID: "+vehicleId+ "timeTravelled 0";
            collector.emit(new Values(msg, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message", "e2eTimestamp", "processingTimestamp"));
    }
}
