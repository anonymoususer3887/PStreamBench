package com.kom.dsp.utils;

import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.Map;

/**
 * Shared per-job wiring for operator-level metrics (selectivity, i.e.
 * output-rows/input-rows per conceptual pipeline stage), the Spark
 * counterpart of what Flink/Storm get for free from their native
 * per-operator instrumentation. Structured Streaming has no equivalent, so
 * each job marks its own stage boundaries with Dataset.observe(name, ...)
 * calls; this class does the one-time plumbing (registering a metrics
 * Source + a listener that feeds observed per-batch counts into it) so each
 * job only needs a single setup call plus its own .observe() calls.
 */
public class MetricsUtils {

    /**
     * Call once per job, right after SparkSession creation and before
     * .start(). observationNames must match the name given to every
     * .observe(name, ....cast("double").as("rows")) call in the job's
     * pipeline -- the aggregate expression must be cast to double
     * (whether it's count(lit(1)) for a row count or an avg(...) lag in
     * seconds), since OperatorMetricsSource.set() takes a double and the
     * generic row.getAs() below throws a ClassCastException if the
     * underlying column turns out to be some other numeric type (e.g. the
     * bigint count() naturally produces).
     */
    public static void registerOperatorMetrics(SparkSession spark, String... observationNames) {
        OperatorMetricsSource source = new OperatorMetricsSource();
        SparkEnv.get().metricsSystem().registerSource(source);

        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent event) {
            }

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                Map<String, Row> observedMetrics = event.progress().observedMetrics();
                for (String name : observationNames) {
                    Row row = observedMetrics.get(name);
                    if (row == null) {
                        continue;
                    }
                    // avg(...) (used for the end_to_end_lag observations) is
                    // SQL NULL over a micro-batch with zero matching rows,
                    // e.g. right at query startup/shutdown before any data
                    // has arrived on that side yet. Unboxing that null into
                    // set()'s primitive double throws NPE -- skip the update
                    // instead, leaving the gauge at its last known value.
                    Double value = row.getAs("rows");
                    if (value != null) {
                        source.set(name, value);
                    }
                }
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {
            }

            @Override
            public void onQueryIdle(QueryIdleEvent event) {
            }
        });
    }
}
