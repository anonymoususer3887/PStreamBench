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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Kafka Streams counterpart of the Spark/Flink flatMapGroupsWithState /
 * RichMapFunction toll-notification stage (query 1) -- see
 * spark/dsp_spark_jobs/.../LinearRoad/LinearRoad.java and VehicleState.java
 * for the full "why this shape" explanation.
 *
 * Per-vehicleId state there tracks a Map&lt;segmentId, List&lt;VehicleEvent&gt;&gt;
 * that -- because the state itself is siloed per vehicleId (only ever
 * populated by that one vehicle's own events) -- can only ever contain that
 * same vehicle's own entries. That makes the "is this vehicle already
 * present in the segment's list" check always true on every visit *after*
 * the segment was first inserted (the vehicle added itself on that very
 * insert), so the accumulate-with-event-speed branch documented in the
 * reference source is provably unreachable: a toll notification is emitted
 * exactly once per (vehicleId, newly-encountered segmentId) pair, always
 * using calculateToll(segmentId, 1) -- segmentId itself, not a speed, is
 * what the reference passes as "averageSpeed" here, which is what the
 * source actually does, not a translation slip. This only implements that
 * one reachable path; state is reduced to "has this vehicle sent any event
 * yet" plus the set of segments already touched, since that's all the
 * reachable logic ever reads.
 *
 * Registers its own "mapper" Tier 3 sensor (same addRateTotalSensor
 * mechanism as OperatorMetricsProcessor), incrementing once per forwarded
 * notification -- matching the Spark port's .observe("mapper", count) being
 * placed on the output of flatMapGroupsWithState (i.e. per emitted
 * notification, not per input event).
 */
public class TollNotificationProcessor implements Processor<String, String, String, String> {

    private static final String STORE_NAME = "toll-notification-store";

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
        int segmentId = Integer.parseInt(f[7]);

        String stored = store.get(vehicleId);
        boolean firstEventEver = (stored == null);
        Set<String> segments = (firstEventEver || stored.isEmpty())
                ? new HashSet<>()
                : new HashSet<>(java.util.Arrays.asList(stored.split(",")));

        if (!firstEventEver && !segments.contains(String.valueOf(segmentId))) {
            segments.add(String.valueOf(segmentId));
            int toll = LinearRoadTopology.calculateToll(segmentId, 1);
            String out = "VehicleId: " + vehicleId + " Segment: " + segmentId + " Toll: " + toll;
            recordsSensor.record();
            context.forward(new Record<>(vehicleId, out, record.timestamp()));
        }

        store.put(vehicleId, String.join(",", segments));
    }

    public static ProcessorSupplier<String, String, String, String> supplier() {
        return new ProcessorSupplier<String, String, String, String>() {
            @Override
            public Processor<String, String, String, String> get() {
                return new TollNotificationProcessor();
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
