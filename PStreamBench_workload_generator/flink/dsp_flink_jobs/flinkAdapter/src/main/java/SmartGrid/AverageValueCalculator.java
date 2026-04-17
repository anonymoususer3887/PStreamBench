package SmartGrid;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageValueCalculator extends ProcessWindowFunction<GridEvent, AverageValue, Integer, TimeWindow> {
    @Override
    public void process(Integer integer, ProcessWindowFunction<GridEvent, AverageValue, Integer, TimeWindow>.Context context, Iterable<GridEvent> elements, Collector<AverageValue> out) throws Exception {
        long timestamp = 0;
		int house = -1;
		float globalAvgLoad = 0f;
		int count = 0;
        for (GridEvent element : elements) {
        	timestamp = element.getTimestamp();
        	globalAvgLoad = globalAvgLoad + element.getValue();
            count = count + 1;
            house = element.getHouse();
        }

        AverageValue result = new AverageValue(timestamp, house, globalAvgLoad / count);
        out.collect(result);
    }
}
