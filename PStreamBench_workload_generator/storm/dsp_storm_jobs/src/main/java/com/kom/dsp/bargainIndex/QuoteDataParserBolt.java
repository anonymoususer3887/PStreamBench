package com.kom.dsp.bargainIndex;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.kom.dsp.utils.KafkaUtils;

import java.util.Map;

public class QuoteDataParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("preparing parser");
    }

    @Override
    public void execute(Tuple tuple) {
        long processingTimestamp = System.currentTimeMillis();

        try {
            long e2eTimestamp = tuple.getLongByField("e2eTimestamp");
            String line = tuple.getStringByField("value");

            String[] fields = line.split(",");

            String symbol = fields[0];
            String date = fields[1];
            double open = Double.parseDouble(fields[2]);
            double high = Double.parseDouble(fields[2]);

            double low = Double.parseDouble(fields[4]);
            double close = Double.parseDouble(fields[5]);
            double adjClose = Double.parseDouble(fields[6]);
            long volume = Long.parseLong(fields[7]);

            QuoteDataModel quoteData = new QuoteDataModel(symbol, date, open, high, low, close, adjClose, volume);
            collector.emit(new Values(symbol, quoteData, e2eTimestamp, processingTimestamp));
            collector.ack(tuple);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            collector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("symbol","quoteData","e2eTimestamp","processingTimestamp"));
    }
}
