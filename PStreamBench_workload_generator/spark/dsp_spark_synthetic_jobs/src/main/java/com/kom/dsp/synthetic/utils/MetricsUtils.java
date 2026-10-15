package com.kom.dsp.synthetic.utils;

import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.Map;

/**
 * Copied/adapted from dsp_spark_jobs' MetricsUtils (not depended on, same
 * reasoning as OperatorMetricsSource's javadoc). Each synthetic topology
 * marks its own stage boundaries with Dataset.observe() calls; this class
 * does the one-time plumbing so a job only needs one setup call plus its own
 * .observe() calls -- identical mechanism to the real-workload jobs, so
 * getprom_spark.py's existing getOperatorMetrics()/getCombo() machinery
 * applies unmodified to synthetic jobs too (see SparkSyntheticDispatch in
 * getprom_spark.py).
 */
public class MetricsUtils {

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
                    // avg(...)/count(...) can be SQL NULL for a micro-batch
                    // with zero matching rows (e.g. right at startup/shutdown)
                    // -- unboxing that into set()'s primitive double throws
                    // NPE, same guard as the real-workload MetricsUtils.
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
