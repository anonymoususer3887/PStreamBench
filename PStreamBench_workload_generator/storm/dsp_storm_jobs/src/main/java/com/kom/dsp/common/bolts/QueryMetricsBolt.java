package com.kom.dsp.common.bolts;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import io.prometheus.metrics.exporter.pushgateway.PushGateway;
import io.prometheus.metrics.model.snapshots.Unit;
import io.prometheus.metrics.core.metrics.Gauge;

public class QueryMetricsBolt extends BaseWindowedBolt {
    private Map<String, Object> stormConf;

    private String pushgatewayUrl;
    private PushGateway pushGateway;
    private Gauge e2eLatencyMetric;
    //private List<String> medianLatencies;

    public QueryMetricsBolt(String pushgatewayUrl) {
        this.pushgatewayUrl = pushgatewayUrl;
        //this.medianLatencies = new ArrayList<>();
    }

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        this.pushGateway = PushGateway.builder()
            .address(this.pushgatewayUrl)
            .job("topology-metrics")
            .build();

        this.e2eLatencyMetric = Gauge.builder()
            .name("e2e_latency")
            .help("End to end latency of tuples in query")
            .unit(Unit.SECONDS)
            .register();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void execute(TupleWindow input) {
        List<Long> latencies = new ArrayList<>();

        for (Tuple t : input.get()) {
            latencies.add(System.currentTimeMillis() - t.getLongByField("e2eTimestamp"));
        }

        Long latency = this.calculateMedian(latencies);
        //this.medianLatencies.add(System.currentTimeMillis() + "," + latency);
        this.pushLatency(latency);
    }

    @Override
    public void cleanup() {
        //String stormId = (String) this.stormConf.get(Config.STORM_ID);
        //try {
        //    new File("/home/playground/metrics").mkdirs();
        //    FileWriter writer = new FileWriter("/home/playground/metrics/" + stormId + ".latency");
        //    for (String latency : medianLatencies) {
        //        writer.write(latency + "\n");
        //    }
        //    writer.close();
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}
    }

    private Long calculateMedian(List<Long> list) {
        Collections.sort(list);

        Long median;
        int size = list.size();
        if (size % 2 == 0) {
            median = (list.get(size / 2 - 1) + list.get(size / 2)) / 2;
        } else {
            median = list.get(size / 2);
        }

        return median;
    }

    private void pushLatency(Long latency) {
        try {
            this.e2eLatencyMetric.set(latency);
            this.pushGateway.push();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
