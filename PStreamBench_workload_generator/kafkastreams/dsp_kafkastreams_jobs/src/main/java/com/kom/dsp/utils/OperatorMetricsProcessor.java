package com.kom.dsp.utils;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * The Kafka Streams counterpart of Spark's Dataset.observe() + MetricsUtils/
 * OperatorMetricsSource (see dsp_spark_jobs' utils package). Kafka Streams
 * has no native per-processor-node records-in/records-out breakdown --
 * "process-rate" at stream-processor-node-metrics is emitted for source
 * nodes only (see kafka.apache.org/41/operations/monitoring, "Processor
 * Node Metrics") -- so, same as the Spark port, every job marks its own
 * pipeline stage boundaries explicitly. Unlike Spark's bolt-on Dropwizard
 * Source, this needs no separate metrics registry: StreamsMetrics.
 * addRateTotalSensor() registers the sensor directly under the standard
 * kafka.streams:type=stream-operator-metrics MBean, so it rides the same
 * JMX-exporter Prometheus scrape already used for every native Kafka
 * Streams metric (see jmx_exporter.yml's "kafka.streams operator sensors"
 * rule) -- no extra exporter process, unlike Spark's separate
 * PrometheusServlet plumbing.
 *
 * One counter per stage ("records" rate/total), not separate in/out
 * counters -- matches Spark's single observe("stagename", count(lit(1))...)
 * per stage. getprom_kafkastreams.py's selectivity/throughput math is the
 * same chain (this stage's count / the previous stage's) as
 * getprom_spark.py's getOperatorMetrics().
 *
 * Usage: insert as a passthrough node between DSL stages,
 * e.g. `stream.process(OperatorMetricsProcessor.supplier("tokenizer"))`.
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
        // scopeName="operator", entityName=stageName -> metric group
        // kafka.streams:type=stream-operator-metrics,...,operator-id=<stageName>,
        // metric names "records-rate"/"records-total".
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
