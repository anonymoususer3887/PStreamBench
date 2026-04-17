package com.kom.dsp.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;

public class ConfigParser {

    public static Config fromArgs(String[] args) throws ParseException {
        Config config = new Config();

        // Accepted command line args
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("name", true, "Topology name");
        options.addOption("parallelism", true, "List of parallelism levels for each operator");
        options.addOption("query", true, "Number of topology query to execute");
        options.addOption("mode", true, "Input source");
        options.addOption("input", true, "Name of filepath of input");
        options.addOption("output", true, "Name of filepath of output");
        options.addOption("kafka-server", true, "URL and port of kafka bootstrap server");
        options.addOption("main-ip", true, "Hostname of main node");
        options.addOption("waitTimeToCancel", true, "Execution time of query");
        options.addOption("size", true, "Window size for window operators");
        options.addOption("slide", true, "Window slide size for window operators");
        options.addOption("lateness", true, "Watermark lateness");
        options.addOption("popularityThreshold", true, "Threshold for filtering queries");
        options.addOption("numWorkers", true, "Number of Workers");

        CommandLine cLine = parser.parse(options, args);

        config.put("topology.name", cLine.getOptionValue("name", "Topology"));
        String parallelism = cLine.getOptionValue("parallelism", "[1,1,1,1]")
            .replace(" ","").replace("[","").replace("]","").replace("'","");
        config.put("topology.parallelism", parallelism);
        config.put("topology.query", cLine.getOptionValue("query", "1"));
        config.put("topology.mode", cLine.getOptionValue("mode", "KAFKA").toLowerCase());
        config.put("topology.input", cLine.getOptionValue("input", "TopicIn"));
        config.put("topology.output", cLine.getOptionValue("output", "TopicOut"));
        config.put("topology.kafkaServer", cLine.getOptionValue("kafka-server", "127.0.0.1:9092"));
        config.put("topology.mainIp", cLine.getOptionValue("main-ip", "127.0.0.1"));
        config.put("topology.waitTimeToCancel", cLine.getOptionValue("waitTimeToCancel", "6"));
        config.put("topology.windowSize", cLine.getOptionValue("size", "0"));
        config.put("topology.windowSlide", cLine.getOptionValue("slide", "0"));
        config.put("topology.watermarkLateness", cLine.getOptionValue("lateness", "0"));
        config.put("topology.threshold", cLine.getOptionValue("popularityThreshold", "0"));
        config.put("topology.numWorkers", cLine.getOptionValue("numWorkers", "10"));

        return config;
    }

    public static Config fromFile(String filePath) {
        return new Config();
    }
}
