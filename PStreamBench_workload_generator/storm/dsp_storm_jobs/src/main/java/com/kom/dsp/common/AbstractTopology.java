package com.kom.dsp.common;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;

import com.kom.dsp.common.bolts.FileWriterBolt;
import com.kom.dsp.common.spouts.CustomKafkaSpout;
import com.kom.dsp.common.spouts.FileSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopology.class);

    protected TopologyBuilder builder;
    protected Config config;
    protected int parallelism;

    public AbstractTopology() {
        this.builder = new TopologyBuilder();
        this.config = new Config();

        this.setDefaultConfig();
    }

    public AbstractTopology(Config config) {
        this.builder = new TopologyBuilder();
        this.config = config;

        this.setDefaultConfig();
    }

    public abstract void buildTopology();

    public void runTopology(Long durationSeconds) {
        String clusterMode = "distributed";

        if (clusterMode.equalsIgnoreCase("local")) {
            runTopologyLocal(durationSeconds);
        } else if (clusterMode.equalsIgnoreCase("distributed")) {
            try {
                runTopologyCluster(durationSeconds);
            } catch (Exception e) {
                LOG.error("Error while running the topology", e);
            }
        } else {
            LOG.error("Unsupported cluster mode: {}", clusterMode);
        }
    }

    private void runTopologyLocal(Long durationSeconds) {
        try {
            String name = (String) config.getOrDefault("topology.name", "Topology");
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(name, this.config, this.builder.createTopology());
            LOG.info("Topology {} started", name);

            Thread.sleep(durationSeconds * 1000);

            cluster.killTopology(name);
            cluster.close();
            cluster.shutdown();
            LOG.info("Topology {} stopped", name);
        } catch (Exception e) {
            LOG.error("Error while running the topology", e);
        }
    }

    private void runTopologyCluster(Long durationSeconds) throws Exception {
        try {
            String name = (String) config.getOrDefault("topology.name", "Topology");

            StormSubmitter.submitTopology(name, this.config, this.builder.createTopology());

            Thread.sleep(durationSeconds * 1000);

            LOG.info("Killing query: {}", name);
        } catch (Exception e) {
            LOG.error("Error while running the topology", e);
        }
    }

    protected BaseRichSpout getSpout() {
        String groupId = (String) config.getOrDefault("topology.name", "default-group");
        return this.getSpout(groupId);
    }

    protected BaseRichSpout getSpout(String groupId) {
        String mode = (String) config.getOrDefault("topology.mode", "kafka");
        String input = (String) config.getOrDefault("topology.input", "TopicIn");

        if (mode.equalsIgnoreCase("kafka")) {
            String kafkaServer = (String) config.getOrDefault("topology.kafkaServer", "127.0.0.1:9092");

            return new CustomKafkaSpout(kafkaServer, input, groupId).getKafkaSpout();
        } else if (mode.equalsIgnoreCase("file")) {
            return new FileSpout(input);
        }

        throw new IllegalArgumentException("Unsupported mode: " + mode);
    }

    protected IRichBolt getSink() {
        return this.getSink(new FieldNameBasedTupleToKafkaMapper<Object, Object>("key", "message"));
    }

    protected IRichBolt getSink(TupleToKafkaMapper<Object, Object> mapper) {
        String mode = (String) config.getOrDefault("topology.mode", "kafka");
        String output = (String) config.getOrDefault("topology.output", "TopicOut");

        if (mode.equalsIgnoreCase("kafka")) {
            String kafkaServer = (String) config.getOrDefault("topology.kafkaServer", "127.0.0.1:9092");

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("batch.size", "200000");
            props.put("batch.num.messages", "200000");
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
        } else if (mode.equalsIgnoreCase("file")) {
            return new FileWriterBolt(output);
        }

        throw new IllegalArgumentException("Unsupported mode: " + mode);
    }

    protected Integer getParallelismDegree(int index) {
        String parallelism = (String) config.getOrDefault("topology.parallelism", "1,1,1,1,1");
        int parallelismAtIndex = Integer.parseInt(parallelism.split(",")[index]);
        return parallelismAtIndex;
    }

    protected Integer getQuery() {
        String queryConfig = (String) config.getOrDefault("topology.query", Constants.QUERY_ONE);
        Integer query = Integer.parseInt(queryConfig);
        return query;
    }

    protected Integer getWindowSize() {
        String windowSizeConfig = (String) config.getOrDefault("topology.windowSize", "0");
        Integer windowSize = Integer.parseInt(windowSizeConfig);
        return windowSize;
    }

    protected Integer getWindowSlide() {
        String windowSlideConfig = (String) config.getOrDefault("topology.windowSlide", "0");
        Integer windowSlide = Integer.parseInt(windowSlideConfig);
        return windowSlide;
    }

    protected String getMainIp() {
        return (String) config.getOrDefault("topology.mainIp", "localhost");
    }

    protected Integer getNumWorkers() {
        String numWorkersConfig = (String) config.getOrDefault("topology.numWorkers", "10");
        Integer numWorkers = Integer.parseInt(numWorkersConfig);
        return numWorkers;
    }

    private void setDefaultConfig() {
        // observability
        //config.setDebug(true);
        config.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0);
        config.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.1);  // default 0.05

        config.setNumWorkers(this.getNumWorkers());

        // disable optimization for shuffleGrouping
        config.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, false);

        //config.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);

        config.registerSerialization(org.apache.storm.kafka.spout.KafkaSpoutMessageId.class);
        config.registerSerialization(org.apache.kafka.common.TopicPartition.class);

        // wait strategies to define wait time when queues are full
        ////config.put(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");  // default WaitStrategyProgressive
        ////config.put(Config.TOPOLOGY_BOLT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");  // default WaitStrategyProgressive
        ////config.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");  // default WaitStrategyProgressive
        ////config.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 0);  // default 100
        ////config.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PARK_MICROSEC, 0);  // default 100

        // memory
        ////config.put(Config.SUPERVISOR_QUEUE_SIZE, 256);  // default 128
        ////config.put(Config.DRPC_QUEUE_SIZE, 256);  // default 128

        // memory m510
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 2048);  // default 128
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 64000);

        // memory c6525-25g
        //config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 4096);  // default 128
        //config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128000);

        // memory c6525-25g
        //config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 8192);  // default 128
        //config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 256000);

        // acknowledgements and backpressure
        //config.setNumAckers(5);
        //config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);  // default #workers
        //config.setMaxSpoutPending(300);
        config.put(Config.TOPOLOGY_BACKPRESSURE_CHECK_MILLIS, 100);  //default 50

        // batching
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 32_768);  // intra-worker, default: 32768
        config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 2_048);  // inter-worker, default: 1_000
        config.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 10);  // intra-worker, default: 1
        config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 20);  // inter-worker, default 1
        config.put(Config.TOPOLOGY_BATCH_FLUSH_INTERVAL_MILLIS, 10);  // default 1
        //config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 65_536);  // intra-worker, default: 32768
        //config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 4_096);  // inter-worker, default: 1_000
        //config.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 20);  // intra-worker, default: 1
        //config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 40);  // inter-worker, default 1
        //config.put(Config.TOPOLOGY_BATCH_FLUSH_INTERVAL_MILLIS, 20);  // default 1

        // netty
        ////config.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 1000);  // default 100
        ////config.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 10000);  // default 1000
        ////config.put(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS, 2);  // default 1
        ////config.put(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS, 2);  // default 1
        ////config.put(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG, 5);  // default 500
        ////config.put(Config.STORM_MESSAGING_NETTY_TRANSFER_BATCH_SIZE, 262144);  // default 262144
        ////config.put(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE, 10485760);  // default 5242880

        //config.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
        //config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 5);
    }
}
