package com.kom.dsp.smartgrid;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessWindowQ1
        extends ProcessWindowFunction<HouseEvent, ProcessOutputQ1, Long, TimeWindow> {

    private static final long serialVersionUID = 1L;
    @Override
    public void process(Long id,Context ctx,Iterable<HouseEvent> readings,Collector<ProcessOutputQ1> out)  {

        //double avgLoad;
        //double totalLoad = 0;
        //int cnt = 0;
        List<Double> energyConsumptions = new ArrayList<>();
        for (HouseEvent r : readings) {
            if(r.getHouse() == id){
                energyConsumptions.add(r.getValue());
                //cnt++;
                //totalLoad += r.getValue();
            }
        }
        //avgLoad = totalLoad/cnt;

        // get current watermark
        ProcessOutputQ1 po = new ProcessOutputQ1();
        po.setHouse(id);
        po.setGal(this.calculateMedian(energyConsumptions));
        po.setTs(111111);
        // emit result
        out.collect(po);
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
