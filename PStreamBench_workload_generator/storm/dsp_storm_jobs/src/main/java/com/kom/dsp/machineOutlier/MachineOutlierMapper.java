package com.kom.dsp.machineOutlier;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

public class MachineOutlierMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        String machineId = tuple.getStringByField("machineId");
        double timestamp = tuple.getDoubleByField("timestamp");
        double cpuUtilPercentage = tuple.getDoubleByField("cpuUtilPercentage");
        double memUtilPercentage = tuple.getDoubleByField("memUtilPercentage");
        double missPerThousandInstructions = tuple.getDoubleByField("missPerThousandInstructions");
        double netIn = tuple.getDoubleByField("netIn");
        double netOut = tuple.getDoubleByField("netOut");
        double diskIO = tuple.getDoubleByField("diskIO");

        return machineId + "," + timestamp + "," + cpuUtilPercentage +
            "," + memUtilPercentage + "," + missPerThousandInstructions +
            "," + netIn + "," + netOut + "," + diskIO;
    }
}
