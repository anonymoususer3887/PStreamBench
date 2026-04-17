package com.kom.dsp.smartgrid;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class ProcessOutputQ1SerializationSchema implements SerializationSchema<ProcessOutputQ1> {

    @Override
    public byte[] serialize(ProcessOutputQ1 element) {
        return element.toString().getBytes();
    }
}
