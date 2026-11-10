package com.kom.dsp.synthetic;

import org.apache.kafka.streams.kstream.KStream;

/**
 * N sources (N = query arg, 2..6). For N&lt;=3 each source is pre-filtered
 * (threshold 0.5) before joining, matching every other engine's synthetic
 * JoinTopology; N&gt;=4 omits pre-filtering, relying on the calibrated key
 * space alone (see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md section 11/12 for
 * the known calibratedKeySpace() cardinality-explosion issue at high N --
 * same CLI --targetMatches knob applies here too, not yet wired through
 * this port since calibratedKeySpace() itself isn't ported here either;
 * --keySpace override is the direct lever meanwhile). N-1 pairwise joins
 * chained left-to-right -&gt; windowed-avg -&gt; sink.
 *
 * <b>Co-partitioning requirement, Kafka-Streams-specific.</b> KStream.join()
 * requires both sides to be co-partitioned (same partition count, same key
 * -- enforced at topology-build time, not silently approximated). Every
 * source topic for one sweep run must therefore be created with the SAME
 * partition count; the join chain's output naturally stays co-partitioned
 * with each next source since join() preserves the shared record key
 * (does not rekey), so no explicit repartition step is needed between
 * hops.
 */
public class JoinTopology extends SyntheticTopology {

    private static final double PREFILTER_THRESHOLD = 0.5;

    public JoinTopology(ConfigArgs cfg) {
        super(cfg);
    }

    public void build(String[] inputTopics, String outputTopic) throws Exception {
        int n = cfg.query; // 2..6
        boolean preFilter = n <= 3;

        @SuppressWarnings("unchecked")
        KStream<String, String>[] streams = new KStream[n];
        for (int i = 0; i < n; i++) {
            streams[i] = source(inputTopics[i], "source-" + i);
        }
        if (preFilter) {
            for (int i = 0; i < n; i++) {
                streams[i] = filter(streams[i], "filter-" + i, PREFILTER_THRESHOLD);
            }
        }

        KStream<String, String> joined = streams[0];
        for (int i = 1; i < n; i++) {
            joined = pairwiseJoin(joined, streams[i], "join-" + i, cfg.windowSize);
        }

        KStream<String, String> agg = windowAvg(joined, "window-agg", cfg.windowSize);
        sink(agg, "sink", outputTopic);

        runAndSelfCancel();
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(args);
        int n = cfg.query;
        String[] inputTopics = new String[n];
        for (int i = 0; i < n; i++) {
            inputTopics[i] = cfg.name + "In" + i;
        }
        String outputTopic = cfg.name + "Out";
        new JoinTopology(cfg).build(inputTopics, outputTopic);
    }
}
