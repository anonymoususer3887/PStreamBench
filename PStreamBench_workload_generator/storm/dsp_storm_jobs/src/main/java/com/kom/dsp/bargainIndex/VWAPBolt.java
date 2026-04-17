package com.kom.dsp.bargainIndex;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.HashMap;

public class VWAPBolt extends BaseWindowedBolt {
    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        Map<String, Double> totalPrices = new HashMap<>();
        Map<String, Long> totalVolumes = new HashMap<>();
        Map<String, Double> quotePrices = new HashMap<>();

        // Variables to hold min and max timestamps
        long e2eTimestamp = Long.MAX_VALUE;
        long processingTimestamp = 0;

        // Iterate over tuples in the window
        for (Tuple tuple : inputWindow.get()) {
            e2eTimestamp = Math.min(e2eTimestamp, tuple.getLongByField("e2eTimestamp"));
            processingTimestamp = Math.max(processingTimestamp, tuple.getLongByField("processingTimestamp"));

            QuoteDataModel quoteData = (QuoteDataModel) tuple.getValueByField("quoteData");
            String symbol = quoteData.getSymbol();

            totalPrices.put(symbol, totalPrices.getOrDefault(symbol, 0.0) + quoteData.getClose() * quoteData.getVolume());
            totalVolumes.put(symbol, totalVolumes.getOrDefault(symbol, 0L) + quoteData.getVolume());
            quotePrices.put(symbol, quoteData.getClose());
        }

        for (String symbol : totalPrices.keySet()) {
            double totalPrice = totalPrices.get(symbol);
            long totalVolume = totalVolumes.get(symbol);
            double quotePrice = quotePrices.get(symbol);

            double vwap = totalPrice / totalVolume;

            collector.emit(new Values(symbol, vwap, totalVolume, quotePrice, e2eTimestamp, processingTimestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("symbol", "vwap", "totalVolume", "quotePrice", "e2eTimestamp", "processingTimestamp"));
    }
}
