package com.kom.dsp.synthetic.utils;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.source.Source;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copied/adapted from dsp_spark_jobs' OperatorMetricsSource (not depended on
 * -- this is an independent Maven project, same relationship
 * dsp_flink_synthetic_jobs has to dsp_flink_jobs). Identical mechanism: feed
 * per-stage observed values (captured via Dataset.observe() + a
 * StreamingQueryListener, see MetricsUtils) into Spark's own metrics system,
 * so they surface through the same PrometheusServlet endpoint already
 * scraped for the real-workload sweeps -- getprom_spark.py's
 * getOperatorMetrics()/_get_operator_rows() reads these same
 * "operatorMetrics_&lt;name&gt;_rows" gauges regardless of which project produced
 * them.
 */
public class OperatorMetricsSource implements Source {

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final Map<String, Double> values = new ConcurrentHashMap<>();

    @Override
    public String sourceName() {
        return "operatorMetrics";
    }

    @Override
    public MetricRegistry metricRegistry() {
        return metricRegistry;
    }

    /** Update (or lazily register, on first sight) the gauge for one named stage. */
    public void set(String operatorName, double value) {
        values.put(operatorName, value);
        String metricName = operatorName + "_rows";
        if (!metricRegistry.getGauges().containsKey(metricName)) {
            metricRegistry.register(metricName, (Gauge<Double>) () -> values.getOrDefault(operatorName, 0.0));
        }
    }
}
