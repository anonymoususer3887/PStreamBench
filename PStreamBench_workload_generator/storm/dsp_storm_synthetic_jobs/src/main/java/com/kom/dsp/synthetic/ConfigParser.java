package com.kom.dsp.synthetic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;

/**
 * Arguments for synthetic topologies. Accepts every flag the controller sends
 * (see utils/playbook/tasks/storm/storm-provide-synthetic-job.yaml) so the
 * same command line works for both local and distributed runs.
 */
public class ConfigParser {

    public static Config fromArgs(String[] args) throws ParseException {
        Config config = new Config();

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("name", true, "Topology name");
        options.addOption("parallelism", true, "Comma list: spout, interior stages..., sink");
        options.addOption("query", true, "Query variant");
        options.addOption("mode", true, "Sink: kafka (default) or file");
        options.addOption("input", true, "Unused by synthetic sources (kept for controller compatibility)");
        options.addOption("output", true, "Kafka topic, or file path when mode=file");
        options.addOption("kafka-server", true, "Kafka bootstrap server");
        options.addOption("main-ip", true, "Hostname of main node");
        options.addOption("waitTimeToCancel", true, "Run time in seconds");
        options.addOption("size", true, "Window size in ms");
        options.addOption("slide", true, "Window slide in ms");
        options.addOption("lateness", true, "Unused by synthetic topologies");
        options.addOption("popularityThreshold", true, "Unused by synthetic topologies");
        options.addOption("numWorkers", true, "Number of workers");
        options.addOption("eventRate", true, "Total tuples per second from the synthetic source");
        options.addOption("cluster", true, "distributed (default) or local");
        options.addOption("metricsDir", true, "Local runs: where metaInfo/evaluationMetrics JSON go");
        options.addOption("enumerationStrategy", true, "Parallelism strategy label: uniform, random, ds2, dhalion, custom");
        options.addOption("keySpace", true, "Join key domain: 'calibrated' (default, sized for targetMatches) or a fixed integer");
        options.addOption("targetMatches", true, "Target join matches per window for calibrated key space (default 1000)");

        CommandLine cLine = parser.parse(options, args);

        config.put("topology.name", cLine.getOptionValue("name", "Topology"));
        config.put("topology.parallelism", cLine.getOptionValue("parallelism", "1,1,1,1,1")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", ""));
        config.put("topology.query", cLine.getOptionValue("query", "1"));
        config.put("topology.mode", cLine.getOptionValue("mode", "kafka").toLowerCase());
        config.put("topology.output", cLine.getOptionValue("output", "TopicOut"));
        config.put("topology.kafkaServer", cLine.getOptionValue("kafka-server", "127.0.0.1:9092"));
        config.put("topology.mainIp", cLine.getOptionValue("main-ip", "127.0.0.1"));
        config.put("topology.waitTimeToCancel", cLine.getOptionValue("waitTimeToCancel", "6"));
        config.put("topology.windowSize", cLine.getOptionValue("size", "0"));
        config.put("topology.windowSlide", cLine.getOptionValue("slide", "0"));
        config.put("topology.numWorkers", cLine.getOptionValue("numWorkers", "10"));
        config.put("topology.eventRate", cLine.getOptionValue("eventRate", "0"));
        config.put("topology.cluster", cLine.getOptionValue("cluster", "distributed").toLowerCase());
        config.put("topology.metricsDir", cLine.getOptionValue("metricsDir", ""));
        config.put("topology.enumerationStrategy", cLine.getOptionValue("enumerationStrategy", "custom"));
        config.put("topology.keySpace", cLine.getOptionValue("keySpace", "calibrated"));
        config.put("topology.targetMatches", cLine.getOptionValue("targetMatches", "1000"));

        return config;
    }
}
