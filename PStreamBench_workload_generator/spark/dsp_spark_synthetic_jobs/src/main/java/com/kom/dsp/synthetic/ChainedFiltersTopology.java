package com.kom.dsp.synthetic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * source -> window-agg -> filter1..filterN -> sink (N = query arg, 2..4:
 * TwoFilters/ThreeFilters/FourFilters). Spark counterpart of
 * dsp_flink_synthetic_jobs' ChainedFiltersTopology -- identical shape,
 * thresholds, and ordering (window-agg BEFORE the filter chain, matching
 * Flink/Storm's synthetic convention exactly, not a Spark-specific
 * reordering). --parallelism: [source, window-agg, filter1, ..., filterN,
 * sink] (N+3 values, same count/order as the Flink version -- window-agg is
 * this shape's only shuffle stage, so no collapsing needed, same reasoning
 * as LinearQueryTopology).
 */
public class ChainedFiltersTopology extends SyntheticTopology {

    private static final double[] THRESHOLDS = {0.52, 0.50, 0.48, 0.46};
    private static final int DEFAULT_KEY_SPACE = 1000;

    public ChainedFiltersTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build() throws Exception {
        int n = cfg.query; // 2..4
        String[] names = new String[n + 3];
        names[0] = "source";
        names[1] = "window-agg";
        for (int i = 1; i <= n; i++) {
            names[1 + i] = "filter" + i;
        }
        names[n + 2] = "sink";
        registerMetrics(names);

        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : DEFAULT_KEY_SPACE;

        int idx = 0;
        int pSrc = cfg.parallelism.get(idx++);
        int pAgg = cfg.parallelism.get(idx++);

        Dataset<Row> src = source("source", keySpace, pSrc);
        Dataset<Row> cur = windowAvg(src, "window-agg", cfg.windowSize, pAgg);
        for (int i = 1; i <= n; i++) {
            int pFilter = cfg.parallelism.get(idx++);
            cur = filter(cur, "filter" + i, THRESHOLDS[i - 1], pFilter);
        }

        int pSink = cfg.parallelism.get(idx);
        sink(cur, "sink", pSink);

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        new ChainedFiltersTopology(cfg).build();
    }
}
