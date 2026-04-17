package com.kom.dsp.common.bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class FileWriterBolt implements IRichBolt {
    private OutputCollector collector;
    private BufferedWriter writer;
    private String filePath;

    public FileWriterBolt(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.writer = new BufferedWriter(new FileWriter(filePath, true));
        } catch (IOException e) {
            throw new RuntimeException("Error opening file " + filePath, e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            String word = input.getStringByField("word");
            Integer count = input.getIntegerByField("count");
            writer.write(word + " " + count.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file " + filePath, e);
        }
        this.collector.ack(input);
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
        // bold does not emit tuples
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
