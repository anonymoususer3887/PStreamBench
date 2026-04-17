package com.kom.dsp.SpikeDetection;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageValueCalculator extends ProcessWindowFunction<SensorMeasurement, AverageValue, Integer, TimeWindow> {

    @Override
    public void process(Integer key, Context ctx, Iterable<SensorMeasurement> elements, Collector<AverageValue> out) throws Exception {
        float sum = 0f;
        int count = 0;

        for (SensorMeasurement element : elements) {
            if (key != element.getSensorId()) {
                continue;
            }

            sum = sum + element.getVoltage();
            count = count + 1;
        }

        for (SensorMeasurement element : elements) {
            if (key != element.getSensorId()) {
                continue;
            }

            float average = sum / count;

            AverageValue result = new AverageValue(key, element.getVoltage(), average);
            out.collect(result);
        }
    }
}
