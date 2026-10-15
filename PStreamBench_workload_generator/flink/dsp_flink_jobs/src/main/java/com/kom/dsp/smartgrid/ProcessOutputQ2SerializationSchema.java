package com.kom.dsp.smartgrid;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class ProcessOutputQ2SerializationSchema implements SerializationSchema<ProcessOutputQ2> {

    @Override
    public byte[] serialize(ProcessOutputQ2 element) {
        return element.toString().getBytes();
    }
}
