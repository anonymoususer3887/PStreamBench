package com.kom.dsp.synthetic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Diagnostic tool (not production code): builds the 6-way JoinTopology
 * pipeline using the rate source (same as a real run) but calls
 * .explain("formatted") on the final pre-sink Dataset instead of starting
 * the streaming query -- captures Spark's actual physical plan (join
 * strategy, Exchange nodes, stateful operators) with zero execution/OOM
 * risk, since .explain() only triggers analysis/planning, never a
 * streaming run. Written 2026-10-16 for the 6-way OOM investigation (see
 * SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md).
 */
public class SixWayPlanInspector extends JoinTopology {

    public SixWayPlanInspector(ConfigArgs cfg) {
        super(cfg);
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(new String[]{
                "--name", "SixWayPlanInspector", "--parallelism", "2,2,2,2,2,2,4,1",
                "--query", "6", "--eventRate", "500", "--size", "1000", "--slide", "1000",
                "--waitTimeToCancel", "1", "--targetMatches", "1000"});
        SixWayPlanInspector t = new SixWayPlanInspector(cfg);
        Dataset<Row> agg = t.buildUpToAgg();
        System.out.println("=== LOGICAL PLAN (extended) ===");
        agg.explain("simple");
        System.out.println("=== PHYSICAL PLAN (formatted, full detail) ===");
        agg.explain("formatted");
        System.exit(0);
    }

    /** Duplicates JoinTopology.build()'s pipeline construction (sources ->
     * joins -> window-agg) but stops before sink()/runAndSelfCancel(), so
     * the resulting Dataset can be explain()'d without starting a query. */
    private Dataset<Row> buildUpToAgg() {
        int n = cfg.query;
        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : 51; // matches calibratedKeySpace(6,...) for eventRate=500
        registerMetrics("source-0", "source-1", "source-2", "source-3", "source-4", "source-5",
                "join-1", "join-2", "join-3", "join-4", "join-5", "window-agg", "sink");

        int idx = 0;
        @SuppressWarnings("unchecked")
        Dataset<Row>[] streams = new Dataset[n];
        for (int i = 0; i < n; i++) {
            int pSrc = cfg.parallelism.get(idx++);
            streams[i] = source("source-" + i, keySpace, pSrc);
        }
        int shuffleWidthValue = cfg.parallelism.get(idx++);
        Dataset<Row> joined = streams[0];
        for (int i = 1; i < n; i++) {
            joined = pairwiseJoin(joined, streams[i], "join-" + i, shuffleWidthValue);
        }
        return windowAvg(joined, "window-agg", cfg.windowSize, shuffleWidthValue);
    }
}
