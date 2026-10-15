package com.kom.dsp.synthetic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 * Per-component counters for local runs. Bolts and spouts record tuples in/out
 * and time spent executing; write() turns them into the same metaInfo and
 * evaluationMetrics JSON layout the CloudLab path produces (getprom_storm.py),
 * except hardwareMetrics, which can't be measured on a single machine.
 */
public final class LocalMetrics {
    private static final Map<String, Stage> STAGES = new ConcurrentHashMap<>();
    // Active window: first tuple emitted by any spout -> last tuple received by the sink.
    private static final AtomicLong FIRST_EMIT = new AtomicLong(0);
    private static final AtomicLong LAST_RECEIVED = new AtomicLong(0);

    /** Called by a spout before a batch of emits. */
    public static void markEmit(long nanos) {
        FIRST_EMIT.compareAndSet(0, nanos);
    }

    /** Called by the sink for every tuple it receives. */
    public static void markReceived(long nanos) {
        LAST_RECEIVED.set(nanos);
    }

    /** Seconds from first emit to last receive; 0 if nothing flowed end to end. */
    public static double activeSeconds() {
        long first = FIRST_EMIT.get();
        long last = LAST_RECEIVED.get();
        return (first == 0 || last <= first) ? 0.0 : (last - first) / 1e9;
    }

    public static final class Stage {
        final AtomicLong in = new AtomicLong();
        final AtomicLong out = new AtomicLong();
        final AtomicLong busyNanos = new AtomicLong();
        volatile int tasks = 1;
    }

    private LocalMetrics() {
    }

    public static Stage stage(String componentId, int tasks) {
        Stage s = STAGES.computeIfAbsent(componentId, k -> new Stage());
        s.tasks = tasks;
        return s;
    }

    private static Stage get(String componentId) {
        Stage s = STAGES.get(componentId);
        return s != null ? s : new Stage();
    }

    /**
     * Writes metaInfo-i0s0-<id>.json and evaluationMetrics-i0s0-<id>.json into
     * directory. Rates are divided by runSeconds, the configured run time.
     */
    public static void write(String jobName, String directory, long runSeconds,
                             List<String> interiorStages, List<String> spoutIds, String sinkId,
                             Map<String, String> meta) throws IOException {
        double active = activeSeconds();
        double run = active > 0 ? active : runSeconds;
        double runNanos = run * 1e9;

        JsonObject operatorMetrics = new JsonObject();
        for (String name : interiorStages) {
            Stage s = get(name);
            long in = s.in.get();
            long out = s.out.get();
            double busy = s.busyNanos.get();

            JsonObject selectivity = new JsonObject();
            selectivity.addProperty("Selectivity", in > 0 ? (double) out / in : 0.0);

            JsonObject op = new JsonObject();
            op.add("selectivity", selectivity);
            op.addProperty("throughput_per_sec", in / run);
            op.addProperty("busy_ratio", Math.min(1.0, busy / (runNanos * s.tasks)));
            op.addProperty("backpressure_ratio", 0.0);
            op.addProperty("execution_latency_avg_ms", in > 0 ? busy / in / 1e6 : 0.0);
            operatorMetrics.add(name, op);
        }

        JsonObject queryMetrics = new JsonObject();
        long sourced = 0;
        for (String spoutId : spoutIds) {
            sourced += get(spoutId).out.get();
        }
        queryMetrics.addProperty("recPerSecOutSource_spout_avg", sourced / run);
        queryMetrics.addProperty("throughput_avg", get(sinkId).in.get() / run);

        JsonObject root = new JsonObject();
        root.addProperty("job_name", jobName);
        root.addProperty("active_window_seconds", run);
        root.add("operatorMetrics", operatorMetrics);
        root.add("queryMetrics", queryMetrics);
        root.add("hardwareMetrics", new JsonObject());

        JsonObject metaInfo = new JsonObject();
        for (Map.Entry<String, String> e : meta.entrySet()) {
            metaInfo.addProperty(e.getKey(), e.getValue());
        }

        String id = jobName + "-local-" + (System.currentTimeMillis() / 1000);
        Path dir = Paths.get(directory);
        Files.createDirectories(dir);
        GsonBuilder builder = new GsonBuilder().setPrettyPrinting();
        Files.write(dir.resolve("metaInfo-i0s0-" + id + ".json"),
                builder.create().toJson(metaInfo).getBytes());
        Files.write(dir.resolve("evaluationMetrics-i0s0-" + id + ".json"),
                builder.create().toJson(root).getBytes());
    }
}
