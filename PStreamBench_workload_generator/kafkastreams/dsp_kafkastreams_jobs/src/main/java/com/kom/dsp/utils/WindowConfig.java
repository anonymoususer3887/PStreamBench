package com.kom.dsp.utils;

import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * Centralizes window+grace construction for every windowed Kafka Streams
 * topology in this project. Previously each of the 12 windowed
 * topologies (AdsAnalytics, ClickAnalytics, GoogleCloudMonitoring,
 * LinearRoad, LogsAnalyzer, SmartGrid, SpikeDetection, TrafficMonitoring,
 * BargainIndex, TrendingTopics) built its own TimeWindows.ofSizeAndGrace(...)
 * call independently, all hard-coding Duration.ZERO as the grace period --
 * confirmed live (2026-09-25) as a major correctness bug: combined with
 * randomly-drawn 10-1000ms window sizes, zero grace meant ANY
 * processing/consumer/rebalance lag caused Kafka Streams to silently
 * discard the record as "already expired" rather than aggregate it. One
 * job's log had 19.2 MILLION "Skipping record for expired window"
 * warnings while its real Kafka topic offsets showed tens of millions of
 * genuine messages actually flowing through -- the queryMetrics being
 * collected (kafka_throughput) was reporting null/zero despite massive
 * real throughput, because nearly every record was being dropped here,
 * not because the pipeline wasn't running.
 *
 * Grace period is independently configurable per job (see -graceMs on the
 * CLI via ArgParser) rather than hard-coded, defaulting to
 * max(windowSizeMs, DEFAULT_MIN_GRACE_MS) so very small windows (10-100ms,
 * used for stress-testing thread-count sensitivity) don't also get an
 * equally-tiny grace period that leaves no real margin for cross-thread
 * stream-time skew under core oversubscription -- confirmed live that
 * even a 1x-window-size grace (100ms window + 100ms grace) was
 * insufficient at num.stream.threads=40 on 16 real cores, where the
 * fastest StreamThread's max-seen-timestamp can race more than 200ms
 * ahead of the slowest.
 *
 * IMPORTANT -- units are NOT uniformly milliseconds. SmartGridTopology's
 * query 2 and LinearRoadTopology's query 2 interpret their window-size/
 * slide parameters in *seconds*, matching their Flink and Spark reference
 * implementations exactly (Flink: Time.seconds(...); Spark:
 * "... seconds"). This is a deliberate cross-engine-parity choice, not an
 * inconsistency to unify away -- silently converting it to milliseconds
 * would break comparability with those other engines' results for the
 * same query. Callers must pass the correct Unit explicitly; this class
 * logs the resolved millisecond values for every job so a unit mismatch
 * is visible in the job's own log rather than silently producing a
 * pathological (e.g. 1000x too small/large) window.
 */
public class WindowConfig {

    /** Floor applied to an unset/defaulted grace period, in milliseconds. */
    public static final long DEFAULT_MIN_GRACE_MS = 500;

    public enum Unit { MILLIS, SECONDS }

    /**
     * @param size            window size, in {@code unit}
     * @param slide           window slide/advance, in {@code unit}
     * @param unit            the unit {@code size}/{@code slide} are expressed in for
     *                        THIS query (see class javadoc -- not always MILLIS)
     * @param graceMsOverride explicit grace period in milliseconds, or {@code null}/
     *                        negative to fall back to max(windowSizeMs, DEFAULT_MIN_GRACE_MS)
     * @param callerLabel     short identifier (e.g. "AdsAnalytics/window") included in the
     *                        startup log line, so a misconfigured job is traceable to its source
     */
    public static TimeWindows build(long size, long slide, Unit unit, Long graceMsOverride, String callerLabel) {
        long sizeMs = unit == Unit.SECONDS ? size * 1000L : size;
        long slideMs = unit == Unit.SECONDS ? slide * 1000L : slide;
        long graceMs = (graceMsOverride == null || graceMsOverride < 0)
                ? Math.max(sizeMs, DEFAULT_MIN_GRACE_MS)
                : graceMsOverride;

        validate(sizeMs, slideMs, graceMs, unit, callerLabel);

        System.out.println(String.format(
                "[WindowConfig][%s] size=%d %s (=%dms) slide=%d %s (=%dms) grace=%dms",
                callerLabel, size, unit, sizeMs, slide, unit, slideMs, graceMs));

        return TimeWindows
                .ofSizeAndGrace(Duration.ofMillis(sizeMs), Duration.ofMillis(graceMs))
                .advanceBy(Duration.ofMillis(slideMs));
    }

    private static void validate(long sizeMs, long slideMs, long graceMs, Unit unit, String callerLabel) {
        if (graceMs == 0) {
            System.out.println("[WindowConfig][" + callerLabel + "] WARNING: grace period is 0ms -- "
                    + "any processing/consumer lag will silently drop records as expired. "
                    + "This is almost never what you want for a real (non-regression-test) run.");
        }
        if (sizeMs < 100) {
            System.out.println("[WindowConfig][" + callerLabel + "] WARNING: resolved window size " + sizeMs
                    + "ms is very small (<100ms) -- expect high sensitivity to processing lag, "
                    + "especially under thread oversubscription. Intended for stress-testing only.");
        }
        if (slideMs > sizeMs) {
            System.out.println("[WindowConfig][" + callerLabel + "] WARNING: slide (" + slideMs
                    + "ms) is larger than window size (" + sizeMs + "ms) -- windows will have gaps "
                    + "between them (some records permanently excluded from every window).");
        }
        if (unit == Unit.SECONDS) {
            System.out.println("[WindowConfig][" + callerLabel + "] NOTE: this query interprets its "
                    + "window-size/slide parameters in SECONDS, not milliseconds, to match the Flink/Spark "
                    + "reference implementations for this same query. Double-check any external sweep "
                    + "config assumed the right unit -- a value intended as milliseconds would silently "
                    + "become 1000x too large here.");
        }
    }
}
