package com.kom.dsp.synthetic;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CLI argument parsing shared by all 9 synthetic topologies. Mirrors the CLI
 * surface of dsp_storm_synthetic_jobs' ConfigParser (same option names) so the
 * same sweep-automation pattern can drive both engines, plus the handful of
 * options that only make sense for Flink (parallelism is per-operator, set
 * directly via setParallelism(); there is no "numWorkers"/"cluster" flag --
 * local vs. distributed is purely how the jar is invoked: `java -jar` for
 * local, `flink run` for distributed. Same convention as dsp_flink_jobs).
 */
public class ConfigArgs {
    public final String name;
    public final List<Integer> parallelism; // one degree per controllable stage, source(s) first
    public final int query; // shape-specific meaning: filter count / join width
    public final String mode; // "discard" (default, no disk writes) or "file"
    public final String output;
    public final long waitTimeToCancel; // seconds; local-mode self-cancel only
    public final int windowSize; // ms
    public final int windowSlide; // ms
    public final int eventRate; // total tuples/sec across all source subtasks
    public final int keySpace; // override; -1 = use shape default
    public final int targetMatches; // calibrated join output target per window

    private ConfigArgs(CommandLine cmd) {
        this.name = cmd.getOptionValue("name");
        this.parallelism =
                Arrays.stream(cmd.getOptionValue("parallelism").split(","))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
        this.query = Integer.parseInt(cmd.getOptionValue("query"));
        this.mode = cmd.getOptionValue("mode", "discard");
        this.output = cmd.getOptionValue("output", "");
        this.waitTimeToCancel = Long.parseLong(cmd.getOptionValue("waitTimeToCancel", "300"));
        this.windowSize = Integer.parseInt(cmd.getOptionValue("size", "1000"));
        this.windowSlide = Integer.parseInt(cmd.getOptionValue("slide", "1000"));
        this.eventRate = Integer.parseInt(cmd.getOptionValue("eventRate", "100000"));
        this.keySpace = Integer.parseInt(cmd.getOptionValue("keySpace", "-1"));
        this.targetMatches = Integer.parseInt(cmd.getOptionValue("targetMatches", "1000"));
    }

    public static ConfigArgs fromArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("name").hasArg().required().build());
        options.addOption(Option.builder().longOpt("parallelism").hasArg().required().build());
        options.addOption(Option.builder().longOpt("query").hasArg().required().build());
        options.addOption(Option.builder().longOpt("mode").hasArg().build());
        options.addOption(Option.builder().longOpt("output").hasArg().build());
        options.addOption(Option.builder().longOpt("waitTimeToCancel").hasArg().build());
        options.addOption(Option.builder().longOpt("size").hasArg().build());
        options.addOption(Option.builder().longOpt("slide").hasArg().build());
        options.addOption(Option.builder().longOpt("eventRate").hasArg().build());
        options.addOption(Option.builder().longOpt("keySpace").hasArg().build());
        options.addOption(Option.builder().longOpt("targetMatches").hasArg().build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return new ConfigArgs(cmd);
    }
}
