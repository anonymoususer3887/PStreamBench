package com.kom.dsp.synthetic;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * File sink for local runs of synthetic queries. Writes "key avg" lines and
 * counts results, so local output can be checked without Kafka.
 */
public class SyntheticFileSinkBolt extends BaseRichBolt {
    private final String filePath;
    private transient OutputCollector collector;
    private transient BufferedWriter writer;
    private transient LocalMetrics.Stage stage;

    public SyntheticFileSinkBolt(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String id = context.getThisComponentId();
        this.stage = LocalMetrics.stage(id, context.getComponentTasks(id).size());
        try {
            this.writer = new BufferedWriter(new FileWriter(filePath, true));
        } catch (IOException e) {
            throw new RuntimeException("Error opening file " + filePath, e);
        }
    }

    @Override
    public void execute(Tuple input) {
        long start = System.nanoTime();
        LocalMetrics.markReceived(start);
        stage.in.incrementAndGet();
        try {
            writer.write(input.getStringByField("key") + " " + input.getValueByField("avg"));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file " + filePath, e);
        }
        collector.ack(input);
        stage.busyNanos.addAndGet(System.nanoTime() - start);
    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // terminal sink, emits nothing
    }
}
