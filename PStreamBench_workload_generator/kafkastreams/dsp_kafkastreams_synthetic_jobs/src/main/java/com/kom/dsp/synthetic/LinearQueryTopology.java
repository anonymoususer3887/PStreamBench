package com.kom.dsp.synthetic;

import org.apache.kafka.streams.kstream.KStream;

/**
 * source -> filter1 -> window-agg -> filter2 -> sink. Kafka Streams
 * counterpart of dsp_flink_synthetic_jobs'/dsp_spark_synthetic_jobs'
 * LinearQueryTopology -- identical shape, same fixed threshold (0.5,
 * ~50% selectivity). Reads from --input, writes to --output (both real
 * Kafka topics -- see SyntheticProducer for how --input gets its traffic).
 */
public class LinearQueryTopology extends SyntheticTopology {

    private static final double THRESHOLD = 0.5;

    public LinearQueryTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build(String inputTopic, String outputTopic) throws Exception {
        KStream<String, String> src = source(inputTopic, "source");
        KStream<String, String> f1 = filter(src, "filter1", THRESHOLD);
        KStream<String, String> agg = windowAvg(f1, "window-agg", cfg.windowSize);
        KStream<String, String> f2 = filter(agg, "filter2", THRESHOLD);
        sink(f2, "sink", outputTopic);

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        String inputTopic = cfg.name + "In";
        String outputTopic = cfg.name + "Out";
        new LinearQueryTopology(cfg).build(inputTopic, outputTopic);
    }
}
