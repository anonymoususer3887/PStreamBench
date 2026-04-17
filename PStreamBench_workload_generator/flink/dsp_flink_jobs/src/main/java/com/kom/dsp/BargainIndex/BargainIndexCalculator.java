package com.kom.dsp.BargainIndex;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class BargainIndexCalculator implements FlatMapFunction<Tuple4<String, Double, Long, Double>, Tuple3<String, Double, Long>> {

    int threshold = 0;

    public BargainIndexCalculator(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void flatMap(Tuple4<String, Double, Long, Double> in, Collector<Tuple3<String, Double, Long>> collector) throws Exception {
        double bargainIndex = 0.0;
        if (in.f3 > in.f1) {  // quotePrice > vwap
            bargainIndex = in.f3 / in.f1;
        }

        if (bargainIndex > threshold) {
            collector.collect(new Tuple3<>(in.f0, bargainIndex, in.f2));
        }
    }
}
