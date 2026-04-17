package com.kom.dsp.GoogleCloudMonitoring;


import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class CPUPerCategoryCalculator implements WindowFunction<TaskEvent, CPUPerCatgory, Integer, TimeWindow> {
    @Override
    public void apply(Integer key, TimeWindow timeWindow, Iterable<TaskEvent> values, Collector<CPUPerCatgory> out) {
        float sum = (float) 0;
        long minTimestamp = Long.MAX_VALUE;

        for (TaskEvent value : values) {
            if (key != value.getCategory()) {
                continue;
            }

            if (minTimestamp >= value.getTimestamp()) {
                minTimestamp = value.getTimestamp();
            }

            sum = sum + value.getCpu();
        }

        CPUPerCatgory result = new CPUPerCatgory(
                minTimestamp,
                key,
                sum);
        out.collect(result);
    }
}
