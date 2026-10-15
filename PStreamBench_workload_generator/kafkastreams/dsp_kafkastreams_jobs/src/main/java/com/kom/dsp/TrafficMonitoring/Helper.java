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
 * Ported from spark/dsp_spark_jobs/.../TrafficMonitoring/Helper.java (itself
 * from flink/dsp_flink_jobs/.../TrafficMonitoring/Helper.java) -- plain Java
 * + Gson, no engine-specific dependency. Flink/Spark parallel tasks run in
 * separate JVM processes, so a bare "load once per JVM" lazy-init is safe
 * there. Kafka Streams' NUM_STREAM_THREADS_CONFIG instead runs multiple
 * StreamThreads inside ONE JVM, all sharing this static roadMap -- an
 * unsynchronized check-then-load-then-put let multiple threads race on the
 * same non-thread-safe HashMap at high parallelism, corrupting/losing
 * entries and causing roadmatcher to intermittently emit nothing (confirmed
 * live: exact-zero avgspeed/sink throughput at pd16+ that reproduced
 * deterministically on re-run, only at higher stream-thread counts).
 * ensureLoaded() below is a standard double-checked-locking singleton load:
 * only one thread ever executes loadShapeFile(), every other thread blocks
 * until it's done, and the volatile "loaded" flag gives all threads a
 * correct (memory-model-safe) view of the fully-populated map afterward,
 * with no lock contention on the hot path once loaded.
 */
public class Helper {

    public static final Map<Long, Double[][]> roadMap = new HashMap<>();
    private static volatile boolean loaded = false;

    public static void ensureLoaded() {
        if (!loaded) {
            synchronized (Helper.class) {
                if (!loaded) {
                    try {
                        loadShapeFile();
                    } catch (Exception e) {
                        // matches the Flink/Spark sources' swallow-and-continue behavior
                    }
                    loaded = true;
                }
            }
        }
    }

    private static void loadShapeFile() throws IOException {
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
            // matches the Flink/Spark sources' swallow-and-continue behavior
        }
    }
}
