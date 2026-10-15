package com.kom.dsp.synthetic;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * source -> windowed-avg -> filter1..N -> sink, N in {2,3,4} (query arg).
 * Same decreasing thresholds as ChainedFiltersTopology in
 * dsp_storm_synthetic_jobs (0.52, 0.50, 0.48, 0.46), corrected there from an
 * initial no-op range (0.9-0.6, which sat above the ~0.5 natural average of
 * window-aggregated uniform values and let everything through).
 * parallelism: [source, window-agg, filter1..N, sink].
 */
public class ChainedFiltersTopology extends SyntheticTopology {
    private static final double[] THRESHOLDS = {0.52, 0.50, 0.48, 0.46};

    public ChainedFiltersTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build() throws Exception {
        int n = cfg.query; // 2, 3, or 4
        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : 1000;
        int p0 = cfg.parallelism.get(0);
        int p1 = cfg.parallelism.get(1);

        DataStream<Tuple2<String, Double>> spout = source("spout", keySpace, p0);
        DataStream<Tuple2<String, Double>> stream = windowAvg(spout, "window-agg", cfg.windowSize, p1);
        for (int i = 1; i <= n; i++) {
            int p = cfg.parallelism.get(1 + i);
            stream = filter(stream, "filter" + i, THRESHOLDS[i - 1], p);
        }
        sink(stream, "sink", cfg.parallelism.get(cfg.parallelism.size() - 1));

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        new ChainedFiltersTopology(cfg).build();
    }
}
