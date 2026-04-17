package com.kom.dsp.googleCloudMonitoring;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class CPUPerJobMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        CPUPerJob o = (CPUPerJob) tuple.getValueByField("cpuPerJob");
        long timestamp = o.getTimestamp();
        long jobId = o.getJobId();
        float averageCpu = o.getAverageCpu();
        return "CPUPerJob{" +
                "timestamp=" + timestamp +
                ", jobId=" + jobId +
                ", averageCpu=" + averageCpu +
                '}';
    }
}
