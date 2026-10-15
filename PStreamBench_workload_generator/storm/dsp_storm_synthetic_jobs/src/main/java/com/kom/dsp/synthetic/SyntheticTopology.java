package com.kom.dsp.synthetic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Tuple;

/**
 * Base class for synthetic benchmark topologies. Independent of the real
 * workloads in dsp_storm_jobs: sources are in-process SyntheticSpouts, and the
 * sink is Kafka (distributed) or a local file. Subclasses register their
 * application name and stage ids so local runs can write their metrics.
 */
public abstract class SyntheticTopology {
    protected final TopologyBuilder builder = new TopologyBuilder();
    protected final Config config;

    /** Application folder name under data/ (e.g. "LinearQuery"). */
    protected String application;
    /** Spout component ids, and interior (controllable) stage ids in pipeline order. */
    protected final List<String> spoutIds = new ArrayList<>();
    protected final List<String> interiorStages = new ArrayList<>();
    protected String sinkId = "sink";
    /** Extra per-run settings recorded in metaInfo (e.g. the join key space used). */
    protected final Map<String, String> extraMeta = new LinkedHashMap<>();

    protected SyntheticTopology(Config config) {
        this.config = config;
        config.setNumWorkers(Integer.parseInt((String) config.getOrDefault("topology.numWorkers", "10")));
    }

    public abstract void buildTopology();

    protected Integer getParallelismDegree(int index) {
        String parallelism = (String) config.getOrDefault("topology.parallelism", "1,1,1,1,1");
        return Integer.parseInt(parallelism.split(",")[index]);
    }

    /** Source at the configured total event rate (single-source shapes). */
    protected IRichSpout getSpout() {
        return getSpout("value", SyntheticSpout.DEFAULT_KEYS, configuredEventRate());
    }

    /** Source with its own value field, key space and rate (multi-source shapes). */
    protected IRichSpout getSpout(String valueField, int numKeys, int eventRate) {
        return new SyntheticSpout(eventRate, valueField, numKeys);
    }

    protected int configuredEventRate() {
        return Integer.parseInt((String) config.getOrDefault("topology.eventRate", "0"));
    }

    protected IRichBolt getSink(TupleToKafkaMapper<Object, Object> mapper) {
        String mode = (String) config.getOrDefault("topology.mode", "kafka");
        String output = (String) config.getOrDefault("topology.output", "TopicOut");

        if (mode.equalsIgnoreCase("file")) {
            return new SyntheticFileSinkBolt(output);
        }

        String kafkaServer = (String) config.getOrDefault("topology.kafkaServer", "127.0.0.1:9092");
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("batch.size", "200000");
        props.put("linger.ms", "10");
        props.put("compression.type", "lz4");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<Object, Object> kafkaBolt = new KafkaBolt<>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(output))
                .withTupleToKafkaMapper(mapper);
        kafkaBolt.setAsync(true);
        return kafkaBolt;
    }

    /** Sink for topologies whose last stage emits (key, avg). */
    protected IRichBolt getAvgSink() {
        return getSink(new TupleToKafkaMapper<Object, Object>() {
            @Override
            public Object getKeyFromTuple(Tuple tuple) {
                return tuple.getStringByField("key");
            }

            @Override
            public Object getMessageFromTuple(Tuple tuple) {
                return String.valueOf(tuple.getValueByField("avg"));
            }
        });
    }

    /** Windowed average of valueField, tumbling if slide is unset or equal to size. */
    protected BaseWindowedBolt windowAgg(String valueField) {
        return windowAgg("key", valueField);
    }

    /** As above, grouping by keyField (join outputs use qualified names). */
    protected BaseWindowedBolt windowAgg(String keyField, String valueField) {
        long size = Long.parseLong((String) config.getOrDefault("topology.windowSize", "0"));
        long slide = Long.parseLong((String) config.getOrDefault("topology.windowSlide", "0"));
        if (size <= 0) {
            throw new IllegalArgumentException("synthetic window stage needs -size (window length in ms) > 0");
        }
        WindowAvgBolt bolt = new WindowAvgBolt(keyField, valueField);
        if (slide > 0 && slide != size) {
            return bolt.withWindow(new Duration(Math.toIntExact(size), TimeUnit.MILLISECONDS),
                    new Duration(Math.toIntExact(slide), TimeUnit.MILLISECONDS));
        }
        return bolt.withTumblingWindow(new Duration(Math.toIntExact(size), TimeUnit.MILLISECONDS));
    }

    /** The same window as windowAgg, for stages that are not averaging (e.g. the join). */
    protected Duration windowSize() {
        return new Duration(Math.toIntExact(Long.parseLong((String) config.getOrDefault("topology.windowSize", "0"))),
                TimeUnit.MILLISECONDS);
    }

    protected Duration windowSlide() {
        long slide = Long.parseLong((String) config.getOrDefault("topology.windowSlide", "0"));
        return new Duration(Math.toIntExact(slide > 0 ? slide : Long.parseLong((String) config.getOrDefault("topology.windowSize", "0"))),
                TimeUnit.MILLISECONDS);
    }

    public void runTopology(Long durationSeconds) throws Exception {
        String name = (String) config.getOrDefault("topology.name", "Topology");
        if (isLocal()) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, builder.createTopology());
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSeconds));
            cluster.killTopology(name);
            cluster.close();
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(name, config, builder.createTopology());
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSeconds));
        }
    }

    protected boolean isLocal() {
        return ((String) config.getOrDefault("topology.cluster", "distributed")).equalsIgnoreCase("local");
    }

    /**
     * Writes metaInfo/evaluationMetrics JSON for this application. Default folder is
     * data/<application>/storm/local/<enumeration strategy>/ under the working directory
     * (override the whole path with -metricsDir).
     */
    public void writeLocalMetrics(long runSeconds) throws IOException {
        String name = (String) config.getOrDefault("topology.name", application);
        String strategy = (String) config.getOrDefault("topology.enumerationStrategy", "custom");
        String dir = (String) config.getOrDefault("topology.metricsDir", "");
        if (dir.isEmpty()) {
            dir = "data/" + application + "/storm/local/" + strategy;
        }
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("job_framework", "Apache Storm");
        meta.put("job_iteration", "0");
        meta.put("job_sample", "0");
        meta.put("job_run_time", String.valueOf(runSeconds));
        meta.put("job_parallelization", (String) config.getOrDefault("topology.parallelism", ""));
        meta.put("job_query_number", (String) config.getOrDefault("topology.query", "1"));
        meta.put("job_window_size", (String) config.getOrDefault("topology.windowSize", "0"));
        meta.put("job_window_slide_size", (String) config.getOrDefault("topology.windowSlide", "0"));
        meta.put("producer_event_per_second", (String) config.getOrDefault("topology.eventRate", "0"));
        meta.put("cluster_hardware_type", "local");
        meta.put("cluster_nodes", "1");
        meta.put("cluster_slots_per_node", (String) config.getOrDefault("topology.numWorkers", "10"));
        meta.put("enumeration_strategy", strategy);
        meta.putAll(extraMeta);

        LocalMetrics.write(name, dir, runSeconds, interiorStages, spoutIds, sinkId, meta);
    }
}
