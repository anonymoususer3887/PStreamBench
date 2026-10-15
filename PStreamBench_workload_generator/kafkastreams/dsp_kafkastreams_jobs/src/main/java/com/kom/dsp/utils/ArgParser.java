package com.kom.dsp.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal "-key value" CLI parser, copied verbatim from dsp_spark_jobs so
 * every engine's job jars share the same CLI contract without a
 * cross-module dependency.
 */
public class ArgParser {

    private final Map<String, String> values = new HashMap<>();

    public static ArgParser fromArgs(String[] args) {
        ArgParser parser = new ArgParser();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                String key = arg.replaceFirst("^-+", "");
                String value = (i + 1 < args.length) ? args[++i] : "";
                parser.values.put(key, value);
            }
        }
        return parser;
    }

    public String get(String key) {
        return values.get(key);
    }

    public String get(String key, String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }
}
