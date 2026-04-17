package com.kom.dsp.TrafficMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapMatcherBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = LoggerFactory.getLogger(MapMatcherBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        try {
            Helper.loadShapeFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            TrafficEvent event = (TrafficEvent) tuple.getValueByField("trafficEvent");
            String vehicleId = event.getVehicleId();
            double latitude = event.getLatitude();
            double longitude = event.getLongitude();

            double speed = event.getSpeed();
            // Retrieve the road_id for the latitude and longitude
            Long roadId = retrieveRoadId(latitude, longitude);

            if (roadId == null) {
                collector.ack(tuple);
                return;
            }

            TrafficEventWithRoad roadEvent = new TrafficEventWithRoad(vehicleId, roadId, latitude, longitude, speed);
            collector.emit(new Values(roadId, roadEvent, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
            collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("roadId", "roadEvent", "e2eTimestamp", "processingTimestamp"));

    }

    // Retrieve the road ID for the given latitude and longitude
    private static Long retrieveRoadId(double latitude, double longitude) throws IOException {

        //LOG.info("Size:"+Helper.roadMap.size());
        DecimalFormat df = new DecimalFormat("#.##");
        latitude = Double.parseDouble(df.format(latitude));
        longitude = Double.parseDouble(df.format(longitude));

        for(Map.Entry<Long,Double[][]> entry : Helper.roadMap.entrySet()){
           Long roadId = entry.getKey();
           Double[][] coordinates = entry.getValue();
           for (Double[] coord : coordinates) {
               if(coord[0].equals(longitude) && coord[1].equals(latitude)) {
                   return roadId;
               }
           }
        }
        return null;
    }
}
