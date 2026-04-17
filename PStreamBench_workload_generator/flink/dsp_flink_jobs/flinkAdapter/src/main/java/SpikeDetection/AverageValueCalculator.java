package SpikeDetection;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageValueCalculator extends ProcessWindowFunction<SensorMeasurement, AverageValue, String, TimeWindow> {
    @Override
    public void process(String str, ProcessWindowFunction<SensorMeasurement, AverageValue, String, TimeWindow>.Context context, Iterable<SensorMeasurement> elements, Collector<AverageValue> out) throws Exception {
        int sensorId = -1;
        float currentValue = 0;
        float sum = (float) 0;
        int count = 0;
        

        for (SensorMeasurement element : elements) {
            	
            sensorId = element.getSensorId();
            
            sum = sum + element.getTemperature();
            count = count + 1;
            currentValue = element.getTemperature();
        }
        
        AverageValue result = new AverageValue(sensorId, currentValue, sum / count);
      
        out.collect(result);
    }
}
