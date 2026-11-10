package com.kom.dsp.synthetic.utils;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Copied/adapted from dsp_kafkastreams_jobs' OperatorMetricsProcessor (not
 * depended on -- independent project, same relationship
 * dsp_flink_synthetic_jobs/dsp_spark_synthetic_jobs have to their
 * real-workload counterparts). Identical mechanism: a passthrough
 * processor node per pipeline stage, registering a records-rate/-total
 * sensor under the standard kafka.streams:type=stream-operator-metrics
 * MBean -- rides the same JMX-exporter Prometheus scrape as every other
 * native Kafka Streams metric, no separate exporter process needed.
 */
public class OperatorMetricsProcessor<K, V> implements Processor<K, V, K, V> {

    private final String stageName;
    private ProcessorContext<K, V> context;
    private Sensor recordsSensor;

    public OperatorMetricsProcessor(String stageName) {
        this.stageName = stageName;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.context = context;
        StreamsMetrics metrics = context.metrics();
        this.recordsSensor = metrics.addRateTotalSensor(
                "operator", stageName, "records",
                Sensor.RecordingLevel.INFO);
    }

    @Override
    public void process(Record<K, V> record) {
        recordsSensor.record();
        context.forward(record);
    }

    public static <K, V> ProcessorSupplier<K, V, K, V> supplier(String stageName) {
        return () -> new OperatorMetricsProcessor<>(stageName);
    }
}
