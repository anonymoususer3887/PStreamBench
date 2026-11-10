package com.kom.dsp.synthetic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * N sources (N = query arg, 2..6). For N&lt;=3 each source is pre-filtered
 * (threshold 0.5) before joining, matching dsp_flink_synthetic_jobs'
 * JoinTopology; N&gt;=4 omits pre-filtering, relying on the calibrated key
 * space alone. N-1 pairwise joins chained left-to-right -> windowed-avg ->
 * sink.
 *
 * <b>--parallelism vector is SHORTER than Flink's for this shape -- a real
 * engine-capability difference, not a simplification.</b> Flink exposes an
 * independent parallelism degree per join operator (N-1 of them) plus one
 * for window-agg, because each Flink join() call creates its own
 * independently-parallelizable operator. Spark cannot: every shuffle stage
 * in one query (every join AND the final window-agg) shares exactly one
 * spark.sql.shuffle.partitions value, captured once at .start() time --
 * see SyntheticTopology's javadoc for the live spark-shell experiment that
 * confirmed this before this design was committed to. Passing N-1
 * seemingly-independent join values here (as Flink's vector shape does)
 * would misrepresent what the engine actually does, so the vector is
 * honest about the collapse instead: exactly ONE "shuffleWidth" value
 * covers every join plus window-agg combined.
 *
 * --parallelism: [source_0..source_{N-1}] + [prefilter_0..prefilter_{N-1}]
 * (only present when N&lt;=3) + [shuffleWidth] + [sink]. Prefilters stay
 * independently parallelizable (narrow repartition() stages, not shuffles),
 * same as Flink.
 */
public class JoinTopology extends SyntheticTopology {

    private static final double PREFILTER_THRESHOLD = 0.5;

    public JoinTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build() throws Exception {
        int n = cfg.query; // 2..6
        boolean preFilter = n <= 3;
        double preFilterSelectivity = preFilter ? 0.5 : 1.0;

        java.util.List<String> names = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) names.add("source-" + i);
        if (preFilter) {
            for (int i = 0; i < n; i++) names.add("filter-" + i);
        }
        for (int i = 1; i < n; i++) names.add("join-" + i);
        names.add("window-agg");
        names.add("sink");
        registerMetrics(names.toArray(new String[0]));

        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : calibratedKeySpace(n, preFilterSelectivity);

        int idx = 0;
        @SuppressWarnings("unchecked")
        Dataset<Row>[] streams = new Dataset[n];
        for (int i = 0; i < n; i++) {
            int pSrc = cfg.parallelism.get(idx++);
            streams[i] = source("source-" + i, keySpace, pSrc);
        }
        if (preFilter) {
            for (int i = 0; i < n; i++) {
                int pFilter = cfg.parallelism.get(idx++);
                streams[i] = filter(streams[i], "filter-" + i, PREFILTER_THRESHOLD, pFilter);
            }
        }

        // Exactly one shuffleWidth value for the whole rest of the query
        // (see class javadoc) -- still passed to every pairwiseJoin/
        // windowAvg call site so each call's intent reads naturally; only
        // the LAST one applied (window-agg's, since it's always last)
        // actually matters by the time runAndSelfCancel() calls .start().
        int shuffleWidthValue = cfg.parallelism.get(idx++);

        // No watermark bookkeeping needed at the call sites anymore (see
        // SyntheticTopology.source()'s javadoc): the watermark and
        // "bucket" are both declared once, in source(), and every
        // pairwiseJoin()/windowAvg() call below just joins/groups on the
        // already-propagated "bucket" column.
        Dataset<Row> joined = streams[0];
        for (int i = 1; i < n; i++) {
            joined = pairwiseJoin(joined, streams[i], "join-" + i, shuffleWidthValue);
        }

        Dataset<Row> agg = windowAvg(joined, "window-agg", cfg.windowSize, shuffleWidthValue);

        int pSink = cfg.parallelism.get(idx);
        sink(agg, "sink", pSink);

        runAndSelfCancel();
    }

    /**
     * K = (n_w^N / target)^(1/(N-1)), n_w = post-filter per-source tuples
     * per window. Same formula as dsp_flink_synthetic_jobs'
     * JoinTopology.calibratedKeySpace().
     */
    private int calibratedKeySpace(int n, double preFilterSelectivity) {
        double perSourceRate = (double) cfg.eventRate / n;
        double nw = perSourceRate * preFilterSelectivity * (cfg.windowSize / 1000.0);
        double k = Math.pow(Math.pow(nw, n) / cfg.targetMatches, 1.0 / (n - 1));
        return Math.max(2, (int) Math.round(k));
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        new JoinTopology(cfg).build();
    }
}
