package com.kom.dsp.utils;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.source.Source;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Feeds per-stage observed values (captured via Dataset.observe() + a
 * StreamingQueryListener, see MetricsUtils.registerOperatorMetrics) into
 * Spark's own metrics system, so they surface through the same
 * PrometheusServlet endpoint (spark.metrics.conf) already scraped for the
 * built-in spark_streaming_* query-level metrics -- no separate exporter
 * process needed.
 *
 * Values are doubles rather than row counts specifically, so the same
 * plumbing covers both per-stage row counts (e.g. "tokenizer") and
 * non-count observations like an average ingestion-to-processing lag in
 * seconds (e.g. "end_to_end_lag").
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
