package com.kom.dsp.LinearRoad;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Kafka Streams counterpart of the Spark/Flink flatMapGroupsWithState /
 * RichMapFunction daily-expenditure stage (query 3) -- see
 * spark/dsp_spark_jobs/.../LinearRoad/LinearRoad.java and DailyExpenditureCalculator.java.
 *
 * Only two event types produce any behavior: type==2 ("query my balance")
 * emits the vehicle's currently tracked totalExpenditure without touching
 * state; type==0 ("position report") updates totalExpenditure but never
 * emits anything itself. All other event types are no-ops. By the same
 * per-vehicleId state-siloing argument documented in
 * TollNotificationProcessor's javadoc, the accumulate-via-"+="-on-repeat-
 * visit branch in the reference source is unreachable: totalExpenditure is
 * always *overwritten* (not accumulated) with calculateToll(segmentId, 1)
 * the first time a *new* segment is seen, and left untouched on repeat
 * visits to an already-seen segment -- so this only implements that
 * reachable path. "First type==0 event ever for this vehicle" resets
 * totalExpenditure to 0 without emitting, matching the reference exactly.
 *
 * Registers its own "mapper" Tier 3 sensor, incrementing once per forwarded
 * (type==2) response -- type==0 events update state silently and are not
 * counted, matching the reference's flatMapGroupsWithState only ever
 * emitting from the type==2 branch.
 */
public class DailyExpenditureProcessor implements Processor<String, String, String, String> {

    private static final String STORE_NAME = "daily-expenditure-store";

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> store;
    private Sensor recordsSensor;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.store = context.getStateStore(STORE_NAME);
        StreamsMetrics metrics = context.metrics();
        this.recordsSensor = metrics.addRateTotalSensor(
                "operator", "mapper", "records", Sensor.RecordingLevel.INFO);
    }

    @Override
    public void process(Record<String, String> record) {
        String vehicleId = record.key();
        String[] f = record.value().split(",");
        int type = Integer.parseInt(f[0]);
        int segmentId = Integer.parseInt(f[7]);

        // Stored state: "hasSeenType0;totalExpenditure;seg1,seg2,..."
        String stored = store.get(vehicleId);
        boolean hasSeenType0 = false;
        long totalExpenditure = 0;
        Set<String> segments = new HashSet<>();
        if (stored != null) {
            String[] s = stored.split(";", 3);
            hasSeenType0 = Boolean.parseBoolean(s[0]);
            totalExpenditure = Long.parseLong(s[1]);
            if (s.length > 2 && !s[2].isEmpty()) {
                segments = new HashSet<>(Arrays.asList(s[2].split(",")));
            }
        }

        if (type == 2) {
            String out = "Vehicle:" + vehicleId + ",Total Expenditure: " + totalExpenditure;
            recordsSensor.record();
            context.forward(new Record<>(vehicleId, out, record.timestamp()));
        } else if (type == 0) {
            boolean firstEventEver = !hasSeenType0;
            hasSeenType0 = true;
            if (firstEventEver) {
                totalExpenditure = 0;
            } else if (!segments.contains(String.valueOf(segmentId))) {
                segments.add(String.valueOf(segmentId));
                totalExpenditure = LinearRoadTopology.calculateToll(segmentId, 1);
            }
            store.put(vehicleId, hasSeenType0 + ";" + totalExpenditure + ";" + String.join(",", segments));
        }
    }

    public static ProcessorSupplier<String, String, String, String> supplier() {
        return new ProcessorSupplier<String, String, String, String>() {
            @Override
            public Processor<String, String, String, String> get() {
                return new DailyExpenditureProcessor();
            }

            @Override
            public Set<StoreBuilder<?>> stores() {
                StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME), Serdes.String(), Serdes.String());
                return Collections.singleton(storeBuilder);
            }
        };
    }
}
