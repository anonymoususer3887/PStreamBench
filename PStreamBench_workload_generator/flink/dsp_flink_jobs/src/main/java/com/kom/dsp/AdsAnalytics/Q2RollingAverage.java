package com.kom.dsp.AdsAnalytics;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Q2RollingAverage extends ProcessWindowFunction<RollingCTR, RollingCTR, Tuple2<Long, Long>, TimeWindow> {

    @Override
    public void process(Tuple2<Long, Long> id, Context ctx, Iterable<RollingCTR> values, Collector<RollingCTR> out) {
        List<Float> rollingCTRValues = new ArrayList<>();

        for (RollingCTR value : values) {
            Float ctrValue = value.getCtrValue();
            rollingCTRValues.add(ctrValue);
        }

        // Calculate the rolling CTR
        float rollingCTR = calculateRollingCTR(rollingCTRValues);

        out.collect(new RollingCTR(id.f0, id.f1, 0, 0, rollingCTR));
    }

    private float calculateRollingCTR(List<Float> ctrValues) {
        // Calculate the average of the CTR values in the list
        float sum = 0;
        for (float value : ctrValues) {
            sum += value;
        }
        return sum / ctrValues.size();
    }
}
