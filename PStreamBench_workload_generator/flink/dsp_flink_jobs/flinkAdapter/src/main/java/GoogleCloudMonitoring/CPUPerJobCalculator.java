package GoogleCloudMonitoring;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class CPUPerJobCalculator implements WindowFunction<TaskEvent, CPUPerJob, Long, TimeWindow> {
    @Override
    public void apply(Long key, TimeWindow window, Iterable<TaskEvent> values, Collector<CPUPerJob> out) {
        long minTimestamp = Long.MAX_VALUE;
        long jobId = -1L;
        float sum = (float) 0;
        int count = 0;

        for (TaskEvent value : values) {
            if (minTimestamp >= value.getTimestamp()) {
                minTimestamp = value.getTimestamp();
            }
            jobId = value.getJobId();
            sum = sum + value.getCpu();
            count = count + 1;
        }

        CPUPerJob result = new CPUPerJob(
        minTimestamp,
        jobId,
        sum / count);
        out.collect(result);
    }
}
