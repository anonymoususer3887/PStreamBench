package com.kom.dsp.LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DailyExpenditureCalculatorBolt extends BaseRichBolt {

    OutputCollector collector;
    Map<Integer, List<VehicleEvent>> vehicleEventsState;
    Map<Integer, List<VehicleEvent>> segmentVehiclesState;
    Map<Integer,Long> vehicleTotalExpenditureState;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        vehicleEventsState = new HashMap<>();
        segmentVehiclesState = new HashMap<>();
        vehicleTotalExpenditureState = new HashMap<>();
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


        if(event.getType()==2) {
            try {
                if (vehicleTotalExpenditureState.get(event.vehicleId) == 0 || vehicleTotalExpenditureState.get(event.vehicleId) == null) {
                    collector.emit(new Values("", tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
                }
            } catch(NullPointerException e) {
                    String msg = "Vehicle:"+ event.vehicleId+ ",Total Expenditure: 0";
                    collector.emit(new Values(msg, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            }
            String msg = "Vehicle:"+ event.vehicleId+ ",Total Expenditure: "+vehicleTotalExpenditureState.get(event.vehicleId);
            collector.emit(new Values(msg, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
        } else if(event.getType()==0) {


            int segmentId = event.segment;


            List<VehicleEvent> events = vehicleEventsState.get(vehicleId);
            List<VehicleEvent> vehiclesInSegment = segmentVehiclesState.get(segmentId);


            if (events == null) {
                // If there are no previous events for the vehicle, create a list and store the current event
                events = new ArrayList<>();
                events.add(event);
                vehicleEventsState.put(vehicleId, events);
                vehicleTotalExpenditureState.put(vehicleId, 0L);
                return;
            } else {
                events.add(event);
                // Update the list of events for the vehicle
                vehicleEventsState.put(vehicleId, events);
            }
            // Keep only the last four events for the vehicle
            if (events.size() > 4) {
                events.remove(0);
            }

            //toll calculation and adding it into vehicletotalexpenditure state
            if (vehiclesInSegment == null) {
                vehiclesInSegment = new ArrayList<>();
                vehiclesInSegment.add(event);
                segmentVehiclesState.put(segmentId, vehiclesInSegment);
                vehicleTotalExpenditureState.put(vehicleId, (long) calculateToll(segmentId,1));
                TollNotification tollNotification = new TollNotification(vehicleId, segmentId, calculateToll(segmentId, 1), event.speed, 1);
                return;
            }
            else {
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
                    segmentVehiclesState.put(segmentId, vehiclesInSegment);
                    int lastSegmentId = events.get(events.size() - 1).segment;


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
                    segmentVehiclesState.put(lastSegmentId, lastSegmentVehicle);

                    // Calculate the average speed  for the segment
                    double averageSpeed = calculateAverageSpeed(segmentId);

                    int numVehicles = getNumVehicles(segmentId);

                    // Calculate the toll amount
                    long toll = (long)calculateToll(averageSpeed, numVehicles);
                    Long totalToll = vehicleTotalExpenditureState.get(vehicleId);
                    vehicleTotalExpenditureState.put(vehicleId, totalToll+toll);
                    return;
                }


            }
            return;
        }
        return;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message", "e2eTimestamp", "processingTimestamp"));
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

        ;

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
