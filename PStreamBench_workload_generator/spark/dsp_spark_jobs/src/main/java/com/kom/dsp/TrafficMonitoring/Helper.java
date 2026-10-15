package com.kom.dsp.TrafficMonitoring;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Ported verbatim from flink/dsp_flink_jobs/.../TrafficMonitoring/Helper.java
 * -- plain Java + Gson, no Flink dependency, so it carries over unchanged.
 * Each Spark task that calls loadShapeFile() re-fetches/re-parses the
 * GeoJSON, same as each parallel Flink RoadMatcher subtask does in open().
 */
public class Helper {

    public static Map<Long, Double[][]> roadMap = new HashMap<>();

    public static void loadShapeFile() throws IOException {
        DecimalFormat df = new DecimalFormat("#.##");
        String geoJsonUrl = "https://gitlab.com/sumalya/cloudlab_profile2/-/raw/master/roads.geojson?ref_type=heads&inline=false";
        URL url = new URL(geoJsonUrl);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
            JsonArray featureArray = jsonObject.getAsJsonArray("features");
            for (int i = 0; i < featureArray.size(); i++) {
                JsonObject featureObject = featureArray.get(i).getAsJsonObject();
                JsonArray coordinatesArray = featureObject.getAsJsonObject("geometry").getAsJsonArray("coordinates");
                Long osmId = featureObject.getAsJsonObject("properties").get("osm_id").getAsLong();
                Double[][] coordinates = new Double[coordinatesArray.size()][2];
                for (int j = 0; j < coordinatesArray.size(); j++) {
                    JsonArray coordinate = coordinatesArray.get(j).getAsJsonArray();
                    coordinates[j][0] = Double.parseDouble(df.format(coordinate.get(0).getAsDouble()));
                    coordinates[j][1] = Double.parseDouble(df.format(coordinate.get(1).getAsDouble()));
                }
                roadMap.put(osmId, coordinates);
            }
        } catch (Exception e) {
            // matches the Flink source's swallow-and-continue behavior
        }
    }
}
