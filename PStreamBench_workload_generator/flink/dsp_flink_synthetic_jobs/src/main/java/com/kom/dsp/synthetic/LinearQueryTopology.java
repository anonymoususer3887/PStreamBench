package com.kom.dsp.synthetic;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * source -> filter1(value&lt;0.5) -> windowed-avg -> filter2(avg&lt;0.25) -> sink.
 * Same shape and thresholds as LinearQueryTopology in dsp_storm_synthetic_jobs.
 * parallelism: [source, filter1, window-agg, filter2, sink].
 */
public class LinearQueryTopology extends SyntheticTopology {

    public LinearQueryTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build() throws Exception {
        int keySpace = cfg.keySpace > 0 ? cfg.keySpace : 1000;
        int p0 = cfg.parallelism.get(0);
        int p1 = cfg.parallelism.get(1);
        int p2 = cfg.parallelism.get(2);
        int p3 = cfg.parallelism.get(3);
        int p4 = cfg.parallelism.get(4);

        DataStream<Tuple2<String, Double>> spout = source("spout", keySpace, p0);
        DataStream<Tuple2<String, Double>> f1 = filter(spout, "filter1", 0.5, p1);
        DataStream<Tuple2<String, Double>> agg = windowAvg(f1, "window-agg", cfg.windowSize, p2);
        DataStream<Tuple2<String, Double>> f2 = filter(agg, "filter2", 0.25, p3);
        sink(f2, "sink", p4);

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        new LinearQueryTopology(cfg).build();
    }
}
