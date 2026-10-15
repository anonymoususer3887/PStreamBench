package com.kom.dsp.synthetic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * source -> filter1 -> window-agg -> filter2 -> sink. Spark counterpart of
 * dsp_flink_synthetic_jobs' LinearQueryTopology -- identical shape, same
 * (key,val) tuple model, same fixed threshold (0.5, ~50% selectivity).
 * --parallelism: [source, filter1, window-agg, filter2, sink] (5 values,
 * same count/order as the Flink version -- this shape has only one shuffle
 * stage, so there is no shuffle-width collapsing to do, unlike JoinTopology).
 */
public class LinearQueryTopology extends SyntheticTopology {

    private static final double THRESHOLD = 0.5;
    private static final int DEFAULT_KEY_SPACE = 1000;

    public LinearQueryTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build() throws Exception {
        registerMetrics("source", "filter1", "window-agg", "filter2", "sink");

        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : DEFAULT_KEY_SPACE;

        int pSrc = cfg.parallelism.get(0);
        int pFilter1 = cfg.parallelism.get(1);
        int pAgg = cfg.parallelism.get(2);
        int pFilter2 = cfg.parallelism.get(3);
        int pSink = cfg.parallelism.get(4);

        Dataset<Row> src = source("source", keySpace, pSrc);
        Dataset<Row> f1 = filter(src, "filter1", THRESHOLD, pFilter1);
        Dataset<Row> agg = windowAvg(f1, "window-agg", cfg.windowSize, pAgg);
        Dataset<Row> f2 = filter(agg, "filter2", THRESHOLD, pFilter2);
        sink(f2, "sink", pSink);

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        new LinearQueryTopology(cfg).build();
    }
}
