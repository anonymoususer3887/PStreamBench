package com.kom.dsp.machineOutlier;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.kom.dsp.utils.KafkaUtils;

import java.util.Map;

public class MachineUsageParserBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();
        try {
            String[] values = tuple.getStringByField("value").split(",");
            long e2eTimestamp = tuple.getLongByField("e2eTimestamp");

            MachineUsage machineUsage;
            if (values[4].isEmpty() && values[5].isEmpty()) {
                machineUsage = new MachineUsage(
                        values[0],
                        Double.parseDouble(values[1]),
                        Double.parseDouble(values[2]),
                        Double.parseDouble(values[3]),
                        0,
                        0,
                        Double.parseDouble(values[6]),
                        Double.parseDouble(values[7]),
                        Double.parseDouble(values[8])
                        );
            } else {
                machineUsage = new MachineUsage(values[0],
                        Double.parseDouble(values[1]),
                        Double.parseDouble(values[2]),
                        Double.parseDouble(values[3]),
                        Double.parseDouble(values[4]),
                        Double.parseDouble(values[5]),
                        Double.parseDouble(values[6]),
                        Double.parseDouble(values[7]),
                        Double.parseDouble(values[8]));
            }
            outputCollector.emit(new Values(machineUsage.getMachineId(), machineUsage, e2eTimestamp, processingTimestamp));
            outputCollector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("machineId", "machineUsage", "e2eTimestamp", "processingTimestamp"));
    }
}
