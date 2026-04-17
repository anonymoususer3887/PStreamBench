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

public class TollNotificationMapperBolt extends BaseRichBolt {
    private OutputCollector collector;

    Map<Integer, List<VehicleEvent>> vehicleEventsState;
    Map<String, List<VehicleEvent>> segmentVehiclesState;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        vehicleEventsState = new HashMap<>();
        segmentVehiclesState = new HashMap<>();

        Helper.init();
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

        //int vehicleId = event.vehicleId;
        int segmentId = event.segment;
        String segmentKey = Integer.toString(vehicleId) + "-" + Integer.toString(segmentId);

        List<VehicleEvent> events = vehicleEventsState.getOrDefault(vehicleId, new ArrayList<>());
        List<VehicleEvent> vehiclesInSegment = segmentVehiclesState.getOrDefault(segmentKey, new ArrayList<>());


        events.add(event);
        // Update the list of events for the vehicle
        vehicleEventsState.put(vehicleId, events);

        // Keep only the last four events for the vehicle
        if (events.size() > 4) {
            events.remove(0);
        }

        if(vehiclesInSegment.size() <= 0) {
            vehiclesInSegment.add(event);
            segmentVehiclesState.put(segmentKey, vehiclesInSegment);
            TollNotification tollNotification = new TollNotification(vehicleId, segmentId, calculateToll(segmentId,1), event.speed, 1);
            collector.emit(new Values(tollNotification, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
        } else {
            boolean vehiclePresent = false;
            //Check if vehicle is not  present in the segment
            for (VehicleEvent v : vehiclesInSegment) {
                if (event.vehicleId == v.vehicleId) {
                    vehiclePresent = true;
                    break;
                }
            }
            if (!vehiclePresent) {
                vehiclesInSegment.add(event);
                segmentVehiclesState.put(segmentKey, vehiclesInSegment);
                int lastSegmentId = events.get(events.size() - 1).segment;
                String lastSegmentKey = Integer.toString(vehicleId) + "-" + Integer.toString(lastSegmentId);


                List<VehicleEvent> lastSegmentVehicle = segmentVehiclesState.get(segmentId);

                // Remove from last segment

                List<VehicleEvent> toRemove = new ArrayList<>();

                for (VehicleEvent v : lastSegmentVehicle) {
                    if (event.vehicleId == v.vehicleId) {
                        toRemove.add(v);
                        break;
                    }
                }

                lastSegmentVehicle.removeAll(toRemove);
                segmentVehiclesState.put(lastSegmentKey, lastSegmentVehicle);

                // Calculate the average speed  for the segment
                double averageSpeed = calculateAverageSpeed(segmentId);

                int numVehicles = getNumVehicles(segmentId);

                // Calculate the toll amount
                int toll = calculateToll(averageSpeed, numVehicles);
                Double totalTollOfVehicle = Helper.totalTollOfVehicle.get(vehicleId);
                if(totalTollOfVehicle == null){
                    Helper.totalTollOfVehicle.put(vehicleId, (double) toll);
                } else {
                    Helper.totalTollOfVehicle.put(vehicleId, totalTollOfVehicle + toll);
                }
                TollNotification tollNotification = new TollNotification(vehicleId, segmentId, toll, averageSpeed, numVehicles);
                collector.emit(new Values(tollNotification, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("notification", "e2eTimestamp", "processingTimestamp"));

    }

    // Helper method to calculate the average speed for a segment
    private double calculateAverageSpeed(int segmentId) {
        double sumSpeed = 0.0;

        List<VehicleEvent> vehiclesInSegment = segmentVehiclesState.get(segmentId);

        // Calculate the sum of speeds and count of vehicles in the segment
        for (VehicleEvent event : vehiclesInSegment) {
            sumSpeed += event.speed;

        }

        // Calculate the average speed
        return sumSpeed/vehiclesInSegment.size();
    }

    // Helper method to get the number of vehicles in a segment during the previous timestamp

    private int getNumVehicles(int segmentId) {
        return segmentVehiclesState.get(segmentId).size();
    }

    // Helper method to calculate the toll based on the average speed and number of vehicles
    private int calculateToll(double averageSpeed, int numVehicles) {
        // By default, the toll amount is 2
        int toll = 2;

        // Check if the average speed and number of vehicles meet the conditions for toll assessment
        if (averageSpeed >= 20 ) {
            toll = (int) ((averageSpeed * numVehicles)/3);
        }

        return toll;
    }
}
