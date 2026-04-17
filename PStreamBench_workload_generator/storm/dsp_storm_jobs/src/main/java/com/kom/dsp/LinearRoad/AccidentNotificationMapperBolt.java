package com.kom.dsp.LinearRoad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class AccidentNotificationMapperBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    Map<Integer, List<VehicleEvent>> vehicleEventsState;
    Map<Integer, List<VehicleEvent>> segmentVehiclesState;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        vehicleEventsState = new HashMap<>();
        segmentVehiclesState = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow window) {

        Map<Long, List<VehicleEvent>> vehicles = new HashMap<>();

        for (Tuple tuple : window.get()) {

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

            if(event.segment==-1){
                continue;
            }

            List<VehicleEvent> listOfVehicles = vehicles.getOrDefault(time, new ArrayList<>());
            listOfVehicles.add(event);
            vehicles.put(time, listOfVehicles);
        }

        for (Long time : vehicles.keySet()) {
            List<VehicleEvent> listOfVehicles = vehicles.get(time);
            List<VehicleEvent> vehiclesInvolved = findAccidents(listOfVehicles);

            if (vehiclesInvolved.size() > 0) {
                VehicleEvent v = vehiclesInvolved.get(0);
                collector.emit(new Values(v.lane, v.position, v.time, v.segment));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lane", "position", "time", "segment", "e2eTimestamp", "processingTimestamp"));
    }

    public List<VehicleEvent>  findAccidents(List<VehicleEvent> vehicleEvents) {
        List<VehicleEvent> vehiclesInvolvedInAccident = new ArrayList<>();
        HashSet<String> uniquePositions = new HashSet<>();

        for (VehicleEvent event : vehicleEvents) {
            String key = event.segment + "-" + event.lane + "-" + event.position;

            if (uniquePositions.contains(key)) {
                // Add the vehicle to the result list if it has a duplicate segment, lane, and position
                vehiclesInvolvedInAccident.add(event);
            } else {
                uniquePositions.add(key);
            }
        }

        return vehiclesInvolvedInAccident;
    }
}
