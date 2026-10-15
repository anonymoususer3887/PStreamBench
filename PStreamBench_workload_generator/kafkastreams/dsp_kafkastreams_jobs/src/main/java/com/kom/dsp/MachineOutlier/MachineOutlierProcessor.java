package com.kom.dsp.MachineOutlier;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Kafka Streams counterpart of the Spark port's flatMapGroupsWithState
 * outlier-detector stage (see MachineOutlierTopology's javadoc for the full
 * "why this shape" explanation). Unlike the simple passthrough stages
 * handled by OperatorMetricsProcessor, this is a genuine stateful
 * 1-input/N-output transform (caps each machineId's recent readings at 4,
 * runs BFPRT median-of-medians selection, forwards readings whose
 * Euclidean distance from the pivot exceeds the hardcoded 0.5 threshold --
 * see MachineOutlierTopology's javadoc for why 0.5 and not the CLI
 * "popularityThreshold" argument), so it registers its own "outlier" Tier 3
 * sensor directly (same addRateTotalSensor mechanism as
 * OperatorMetricsProcessor, incrementing once per *forwarded* record, i.e.
 * once per detected outlier -- not once per input reading).
 *
 * Per-machineId state is a semicolon-separated list of
 * "machineId,timestamp,cpu,mem,memBW,mkpi,netIn,netOut,diskIO" strings
 * (each entry an encoded MachineUsage), matching this port's project-wide
 * convention of plain delimited strings over custom Serdes.
 */
public class MachineOutlierProcessor implements Processor<String, String, String, String> {

    private static final String STORE_NAME = "machine-outlier-store";

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> store;
    private Sensor recordsSensor;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.store = context.getStateStore(STORE_NAME);
        StreamsMetrics metrics = context.metrics();
        this.recordsSensor = metrics.addRateTotalSensor(
                "operator", "outlier", "records", Sensor.RecordingLevel.INFO);
    }

    @Override
    public void process(Record<String, String> record) {
        String machineId = record.key();
        String existing = store.get(machineId);
        List<String> readings = existing == null
                ? new ArrayList<>()
                : new ArrayList<>(Arrays.asList(existing.split(";")));
        readings.add(record.value());
        if (readings.size() > 4) {
            readings.remove(0);
        }
        store.put(machineId, String.join(";", readings));

        if (readings.size() < 2) {
            return;
        }

        MachineUsage[] arr = new MachineUsage[readings.size()];
        for (int i = 0; i < readings.size(); i++) {
            arr[i] = parseUsage(readings.get(i));
        }
        double threshold = 0.5;
        int k = (int) Math.ceil(arr.length * 0.75);
        MachineUsage kth = BFPRTAlgorithm.bfprt(arr.clone(), 0, arr.length - 1, k);
        for (MachineUsage m : arr) {
            if (m.getEuclideanDistance(kth) > threshold) {
                String out = m.getMachineId() + "," + m.getTimestamp() + "," + m.getCpuUtilPercentage()
                        + "," + m.getMemUtilPercentage() + "," + m.getMissPerThousandInstructions()
                        + "," + m.getNetIn() + "," + m.getNetOut() + "," + m.getDiskIO();
                recordsSensor.record();
                context.forward(new Record<>(machineId, out, record.timestamp()));
            }
        }
    }

    private static MachineUsage parseUsage(String csv) {
        String[] v = csv.split(",");
        return new MachineUsage(v[0], Double.parseDouble(v[1]), Double.parseDouble(v[2]),
                Double.parseDouble(v[3]), Double.parseDouble(v[4]), Double.parseDouble(v[5]),
                Double.parseDouble(v[6]), Double.parseDouble(v[7]), Double.parseDouble(v[8]));
    }

    public static ProcessorSupplier<String, String, String, String> supplier() {
        return new ProcessorSupplier<String, String, String, String>() {
            @Override
            public Processor<String, String, String, String> get() {
                return new MachineOutlierProcessor();
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
