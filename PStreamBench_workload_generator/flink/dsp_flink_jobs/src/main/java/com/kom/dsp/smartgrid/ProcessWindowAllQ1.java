package com.kom.dsp.smartgrid;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessWindowAllQ1 implements AggregateFunction<HouseEvent, List<HouseEvent>, ProcessOutputQ1> {

    private static final long serialVersionUID = 1L;

    @Override
    public List<HouseEvent> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<HouseEvent> add(HouseEvent value, List<HouseEvent> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public ProcessOutputQ1 getResult(List<HouseEvent> readings)  {

        //double avgLoad;
        //double totalLoad = 0;
        //int cnt = 0;
        List<Double> energyConsumptions = new ArrayList<>();
        for (HouseEvent r : readings) {
            energyConsumptions.add(r.getValue());
            //cnt++;
            //totalLoad += r.getValue();
        }
        //avgLoad = totalLoad/cnt;

        // get current watermark
        ProcessOutputQ1 po = new ProcessOutputQ1();
        po.setHouse(0L);
        po.setGal(this.calculateMedian(energyConsumptions));
        po.setTs(111111);
        // emit result
        return po;
    }

    @Override
    public List<HouseEvent> merge(List<HouseEvent> a, List<HouseEvent> b) {
        a.addAll(b);
        return a;
    }

    double calculateMedian(List<Double> energyConsumptions) {
        Collections.sort(energyConsumptions);

        double median;
        int size = energyConsumptions.size();
        if (size % 2 == 0) {
            median = (energyConsumptions.get(size / 2 - 1) + energyConsumptions.get(size / 2)) / 2.0;
        } else {
            median = energyConsumptions.get(size / 2);
        }

        return median;
    }
}
