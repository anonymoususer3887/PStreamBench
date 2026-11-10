package com.kom.dsp.synthetic.utils;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.JoinWindows;

import java.time.Duration;

/**
 * Copied/adapted from dsp_kafkastreams_jobs' WindowConfig (not depended on,
 * same reasoning as OperatorMetricsProcessor's javadoc). Grace period
 * defaults to max(windowSizeMs, DEFAULT_MIN_GRACE_MS), never zero -- the
 * real-workload WindowConfig's own javadoc documents a live-confirmed bug
 * class this avoids: a zero grace period silently drops any record that
 * arrives even slightly late (processing/consumer/rebalance lag) as
 * "already expired", which can discard the overwhelming majority of real
 * throughput while queryMetrics still reports near-zero, looking like "no
 * data flowing" rather than "every record being dropped at the window
 * boundary". Applying the same floor here before this project's own
 * synthetic jobs can hit the identical failure mode.
 */
public class WindowConfig {

    public static final long DEFAULT_MIN_GRACE_MS = 500;

    public static TimeWindows buildTumbling(long windowSizeMs, Long graceMsOverride) {
        long graceMs = (graceMsOverride == null || graceMsOverride < 0)
                ? Math.max(windowSizeMs, DEFAULT_MIN_GRACE_MS)
                : graceMsOverride;
        return TimeWindows
                .ofSizeAndGrace(Duration.ofMillis(windowSizeMs), Duration.ofMillis(graceMs))
                .advanceBy(Duration.ofMillis(windowSizeMs));
    }

    /** Join window for pairwiseJoin() -- symmetric +/-windowSizeMs, same
     * tolerance already used consistently by this project's other
     * engines' synthetic joins before the Spark bucket-equality redesign
     * (see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md section 11.1 for why a
     * sliding tolerance and Flink's discrete-window rule are NOT
     * equivalent -- Kafka Streams' own native stream-stream join has no
     * discrete-bucket primitive at all, only JoinWindows' symmetric
     * before/after tolerance, so this is the closest *native* fit for
     * this engine specifically, not a copy of the superseded Spark
     * design). Grace period floor applied the same way as the tumbling
     * case. */
    public static JoinWindows buildJoin(long windowSizeMs, Long graceMsOverride) {
        long graceMs = (graceMsOverride == null || graceMsOverride < 0)
                ? Math.max(windowSizeMs, DEFAULT_MIN_GRACE_MS)
                : graceMsOverride;
        return JoinWindows
                .ofTimeDifferenceAndGrace(Duration.ofMillis(windowSizeMs), Duration.ofMillis(graceMs));
    }
}
