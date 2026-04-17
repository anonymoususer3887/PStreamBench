package SmartGrid;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageValueCalculator2 extends ProcessWindowFunction<GridEvent, AverageValue2, String, TimeWindow> {
    @Override
    public void process(String str, ProcessWindowFunction<GridEvent, AverageValue2, String, TimeWindow>.Context context, Iterable<GridEvent> elements, Collector<AverageValue2> out) throws Exception {
        long timestamp = 0;
		int house = -1;
		float localAvgLoad = 0f;
		int plug = -1;
		int household= -1;
		int count = 0;
        for (GridEvent element : elements) {
        	timestamp = element.getTimestamp();
        	localAvgLoad = localAvgLoad + element.getValue();
            count = count + 1;
            house = element.getHouse();
            plug = element.getPlug();
            household = element.getHousehold();
        }

        AverageValue2 result = new AverageValue2(timestamp,plug,household, house, localAvgLoad / count);
        out.collect(result);
    }
}
