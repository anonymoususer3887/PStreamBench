package com.kom.dsp.synthetic;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * N sources (N = query arg, 2..6). For N&lt;=3 each source is pre-filtered
 * (threshold 0.5) before joining, matching JoinTopology in
 * dsp_storm_synthetic_jobs; N&gt;=4 omits pre-filtering, relying solely on the
 * calibrated key space to keep output volume controlled. N-1 pairwise joins
 * chained left-to-right -> windowed-avg -> sink.
 *
 * Engine-specific structural note (for the contributions writeup): Storm's
 * native JoinBolt fuses all N-1 pairwise joins of an N-way join into a single
 * bolt/component regardless of N (chained .join() calls on one builder), so
 * the Storm topology exposes only one "join" parallelism knob per shape.
 * Flink's DataStream join() returns a new, independently parallelizable
 * operator per call -- there is no equivalent single-operator fusion -- so
 * here each of the N-1 joins is its own operator with its own parallelism
 * degree. Same query shape (sources, pre-filter, join count, key space,
 * post-join aggregation) across both engines; the join STAGE COUNT exposed
 * to the parallelism-strategy layer differs by one engine's native join
 * primitive, not by a modeling choice.
 */
public class JoinTopology extends SyntheticTopology {

    public JoinTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build() throws Exception {
        int n = cfg.query; // 2..6
        boolean preFilter = n <= 3;
        double preFilterThreshold = 0.5;
        double preFilterSelectivity = preFilter ? 0.5 : 1.0;

        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : calibratedKeySpace(n, preFilterSelectivity);

        int idx = 0;
        DataStream<Tuple2<String, Double>>[] streams = new DataStream[n];
        for (int i = 0; i < n; i++) {
            int pSrc = cfg.parallelism.get(idx++);
            DataStream<Tuple2<String, Double>> s = source("spout-" + i, keySpace, pSrc);
            streams[i] = s;
        }
        if (preFilter) {
            for (int i = 0; i < n; i++) {
                int pFilter = cfg.parallelism.get(idx++);
                streams[i] = filter(streams[i], "filter-" + i, preFilterThreshold, pFilter);
            }
        }

        DataStream<Tuple2<String, Double>> joined = streams[0];
        for (int i = 1; i < n; i++) {
            int pJoin = cfg.parallelism.get(idx++);
            joined = pairwiseJoin(joined, streams[i], "join-" + i, pJoin);
        }

        int pAgg = cfg.parallelism.get(idx++);
        DataStream<Tuple2<String, Double>> agg = windowAvg(joined, "window-agg", cfg.windowSize, pAgg);

        int pSink = cfg.parallelism.get(idx);
        sink(agg, "sink", pSink);

        runAndSelfCancel();
    }

    private DataStream<Tuple2<String, Double>> pairwiseJoin(
            DataStream<Tuple2<String, Double>> a,
            DataStream<Tuple2<String, Double>> b,
            String name,
            int parallelism) {
        JoinFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>> combine =
                (x, y) -> Tuple2.of(x.f0, (x.f1 + y.f1) / 2.0);
        // Lambdas erase their generic signature, so Flink's type extractor cannot
        // infer the join's output type automatically; pass it explicitly.
        org.apache.flink.api.common.typeinfo.TypeInformation<Tuple2<String, Double>> outType =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Double>>() {});
        return ((org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<Tuple2<String, Double>>)
                        a.join(b)
                                .where((org.apache.flink.api.java.functions.KeySelector<Tuple2<String, Double>, String>) t -> t.f0)
                                .equalTo((org.apache.flink.api.java.functions.KeySelector<Tuple2<String, Double>, String>) t -> t.f0)
                                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(cfg.windowSize)))
                                .apply(combine, outType))
                .name(name)
                .setParallelism(parallelism);
    }

    /**
     * K = (n_w^N / target)^(1/(N-1)), n_w = post-filter per-source tuples per
     * window. Same formula as dsp_storm_synthetic_jobs' JoinTopology.joinKeySpace(),
     * corrected there to use post- rather than pre-filter counts.
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
