package com.kom.dsp.synthetic;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI argument parsing shared by all 9 synthetic topologies, mirroring
 * dsp_flink_synthetic_jobs/dsp_spark_synthetic_jobs' ConfigArgs' option
 * names/semantics so the same sweep-automation pattern drives every
 * engine. Kafka-specific additions (bootstrap-server, topic names,
 * application-id) mirror dsp_kafkastreams_jobs' WordCountTopology's own
 * CLI surface -- mode=KAFKA is this engine's ONLY mode (no in-process
 * synthetic source/discard sink exists for Kafka Streams at all, unlike
 * Flink's DataGeneratorSource+DiscardingSink or Spark's rate+noop --
 * Kafka Streams always reads from and writes to real Kafka topics), so
 * "mode"/"output" (dropped in the Flink/Spark versions, since those
 * engines have a real choice to make) are replaced here by the Kafka
 * connection parameters that are NOT optional for this engine.
 *
 * parallelism: kept as a list for CLI-shape consistency with every other
 * engine's sweep tooling, but Kafka Streams has no per-stage parallelism
 * control at all (see SyntheticTopology's javadoc) -- only
 * parallelism.get(1) (the first "swept" value, matching
 * dsp_kafkastreams_jobs' WordCountTopology's own documented precedent) is
 * actually used, as num.stream.threads.
 */
public class ConfigArgs {
    public final String name;
    public final List<Integer> parallelism;
    public final int query;
    public final long waitTimeToCancel;
    public final int windowSize; // ms
    public final int windowSlide; // ms
    public final int eventRate;
    public final int keySpace;
    public final int targetMatches;
    public final String bootstrapServer;
    public final String applicationId;
    public final String topics; // SyntheticProducer only -- comma-separated topic list

    private ConfigArgs(CommandLine cmd) {
        this.name = cmd.getOptionValue("name");
        this.parallelism =
                Arrays.stream(cmd.getOptionValue("parallelism").split(","))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
        this.query = Integer.parseInt(cmd.getOptionValue("query"));
        this.waitTimeToCancel = Long.parseLong(cmd.getOptionValue("waitTimeToCancel", "300"));
        this.windowSize = Integer.parseInt(cmd.getOptionValue("size", "1000"));
        this.windowSlide = Integer.parseInt(cmd.getOptionValue("slide", "1000"));
        this.eventRate = Integer.parseInt(cmd.getOptionValue("eventRate", "100000"));
        this.keySpace = Integer.parseInt(cmd.getOptionValue("keySpace", "-1"));
        this.targetMatches = Integer.parseInt(cmd.getOptionValue("targetMatches", "1000"));
        this.bootstrapServer = cmd.getOptionValue("bootstrap-server", "localhost:9092");
        this.applicationId = cmd.getOptionValue("application-id", this.name + "-" + System.currentTimeMillis());
        this.topics = cmd.getOptionValue("topics");
    }

    public static ConfigArgs fromArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("name").hasArg().required().build());
        options.addOption(Option.builder().longOpt("parallelism").hasArg().required().build());
        options.addOption(Option.builder().longOpt("query").hasArg().required().build());
        options.addOption(Option.builder().longOpt("waitTimeToCancel").hasArg().build());
        options.addOption(Option.builder().longOpt("size").hasArg().build());
        options.addOption(Option.builder().longOpt("slide").hasArg().build());
        options.addOption(Option.builder().longOpt("eventRate").hasArg().build());
        options.addOption(Option.builder().longOpt("keySpace").hasArg().build());
        options.addOption(Option.builder().longOpt("targetMatches").hasArg().build());
        options.addOption(Option.builder().longOpt("bootstrap-server").hasArg().build());
        options.addOption(Option.builder().longOpt("application-id").hasArg().build());
        options.addOption(Option.builder().longOpt("topics").hasArg().build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return new ConfigArgs(cmd);
    }
}
