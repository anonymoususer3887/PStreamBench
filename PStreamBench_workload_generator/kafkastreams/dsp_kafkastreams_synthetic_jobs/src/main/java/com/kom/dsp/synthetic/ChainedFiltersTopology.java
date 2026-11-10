package com.kom.dsp.synthetic;

import org.apache.kafka.streams.kstream.KStream;

/**
 * source -> window-agg -> filter1..filterN -> sink (N = query arg, 2..4:
 * TwoFilters/ThreeFilters/FourFilters). Kafka Streams counterpart of
 * dsp_flink_synthetic_jobs'/dsp_spark_synthetic_jobs' ChainedFiltersTopology
 * -- identical shape, thresholds, and ordering (window-agg BEFORE the
 * filter chain, matching every other engine's synthetic convention exactly,
 * not a Kafka-Streams-specific reordering).
 */
public class ChainedFiltersTopology extends SyntheticTopology {

    private static final double[] THRESHOLDS = {0.52, 0.50, 0.48, 0.46};

    public ChainedFiltersTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build(String inputTopic, String outputTopic) throws Exception {
        int n = cfg.query; // 2..4

        KStream<String, String> src = source(inputTopic, "source");
        KStream<String, String> cur = windowAvg(src, "window-agg", cfg.windowSize);
        for (int i = 1; i <= n; i++) {
            cur = filter(cur, "filter" + i, THRESHOLDS[i - 1]);
        }
        sink(cur, "sink", outputTopic);

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        String inputTopic = cfg.name + "In";
        String outputTopic = cfg.name + "Out";
        new ChainedFiltersTopology(cfg).build(inputTopic, outputTopic);
    }
}
