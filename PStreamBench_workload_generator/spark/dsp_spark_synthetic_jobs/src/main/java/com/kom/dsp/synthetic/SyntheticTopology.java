package com.kom.dsp.synthetic;

import com.kom.dsp.synthetic.utils.MetricsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

/**
 * Base class for the 9 fixed synthetic query shapes, Spark Structured
 * Streaming counterpart of dsp_flink_synthetic_jobs' SyntheticTopology.
 * Independent project (no dependency on dsp_spark_jobs or
 * dsp_flink_synthetic_jobs), same (key: String, value: Double) tuple model,
 * represented here as a two-column Dataset&lt;Row&gt; ("key", "val") rather than
 * a typed Tuple2, since Structured Streaming's DataFrame API has no
 * equivalent to Flink's typed DataStream&lt;Tuple2&lt;...&gt;&gt;.
 *
 * <b>No Kafka, matching Flink/Storm's synthetic convention.</b> Every real
 * Spark job in dsp_spark_jobs requires Kafka (mode=KAFKA is the only
 * supported mode, see e.g. WordCountJob.java) -- there is no in-process
 * generator/discard-sink mode like Flink's DataGeneratorSource+DiscardingSink
 * or Storm's in-process spout+discard bolt. Spark Structured Streaming does
 * have a built-in equivalent pair, though, used here instead of adding a
 * Kafka dependency to the synthetic benchmark: the "rate" source format
 * (generates rows at a configured rowsPerSecond, in-process, no external
 * system) and the "noop" sink format (Spark 3.x's built-in
 * discard-everything sink, added specifically for Structured Streaming
 * micro-benchmarking). This is the standard, documented way to
 * micro-benchmark a Structured Streaming pipeline without Kafka.
 *
 * <b>Key/value derivation.</b> The rate source's own output columns
 * (timestamp, value: a strictly increasing Long sequence number) carry no
 * useful randomness, so key/value are derived via two independent
 * Spark SQL rand() columns -- each unseeded rand() call in one query gets
 * its own distinct internal seed from Spark's parser, so the two columns
 * are statistically independent, matching Flink/Storm's independently-drawn
 * uniform (key, value) pairs per row.
 *
 * <b>Parallelism model, a genuine engine capability difference from Flink
 * (confirmed live via spark-shell, 2026-10-15, before committing to this
 * design -- see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md for the full writeup).
 * </b> Spark has no per-operator setParallelism(): narrow stages (filter,
 * map) get an explicit task count via repartition(n), matching
 * dsp_spark_jobs' WordCountJob.java convention exactly. But every *shuffle*
 * stage (a groupBy aggregation or a stream-stream join) reads the session's
 * spark.sql.shuffle.partitions value, and Spark captures that value ONCE,
 * at physical-plan analysis time (triggered by .start()) -- NOT at the
 * point in code where each .groupBy()/.join() call is written. Confirmed
 * empirically: building two chained groupBys under two different
 * spark.conf().set() values each, then calling .explain() after a THIRD
 * .set() call, showed BOTH Exchange nodes resolved to the third
 * (last-set) value, including the first groupBy's own Exchange. Net effect:
 * every shuffle operator in one query -- every join AND the final
 * window-agg -- shares exactly one shuffle-width number, set exactly once,
 * right before runAndSelfCancel() calls .start(). JoinTopology collapses
 * what is N-1 independently-parallelizable join operators in Flink (see
 * that class's own javadoc on the Storm-vs-Flink join-fusion difference)
 * into a SINGLE shared knob here -- a third variant of the same
 * "the engine's native join primitive determines how many independent
 * parallelism knobs exist" finding, not a modeling shortcut.
 */
public abstract class SyntheticTopology {

    protected final ConfigArgs cfg;
    protected final SparkSession spark;

    /** Set by windowAvg()/pairwiseJoin() (whichever runs last while building
     * the query) and applied exactly once, in build(), right before
     * .start() -- see this class's javadoc on why only the last value
     * before analysis time actually matters. */
    protected Integer shuffleWidth = null;

    protected SyntheticTopology(ConfigArgs cfg) {
        this.cfg = cfg;
        this.spark = SparkSession.builder()
                .appName(cfg.name)
                .getOrCreate();
    }

    /** Call once per job, right after construction and before building the
     * pipeline -- registers the observationNames this shape will call
     * .observe() for (see MetricsUtils). Always appends "end_to_end_lag"
     * -- sink() (below) observes it unconditionally for every shape, so no
     * individual topology class's call site needs to list it explicitly
     * (added 2026-10-20, confirmed live that forgetting to list an
     * observation name here means MetricsUtils' listener never looks it
     * up at all, even though the .observe() call itself fires correctly
     * -- the gauge was simply never updated). */
    protected void registerMetrics(String... observationNames) {
        String[] withLag = new String[observationNames.length + 1];
        System.arraycopy(observationNames, 0, withLag, 0, observationNames.length);
        withLag[observationNames.length] = "end_to_end_lag";
        MetricsUtils.registerOperatorMetrics(spark, withLag);
    }

    /** Source: Spark's built-in "rate" format, independent rand()-derived
     * key/value columns (see class javadoc), repartition(parallelism) is
     * the narrow-stage task-count control (no shuffle involved -- a plain
     * repartition(n) always forces exactly n partitions for its own
     * Exchange, unaffected by spark.sql.shuffle.partitions, which only
     * governs shuffles Spark itself plans for groupBy/join).
     *
     * <b>Watermark and "bucket" are both declared here, once, for every
     * shape -- redesigned 2026-10-15 after the Flink-vs-Spark join-semantics
     * review (see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md).</b> Flink's
     * windowed join assigns every stream to the SAME discrete, globally
     * epoch-aligned tumbling window and matches same-key tuples that landed
     * in the same window instance -- a fundamentally different rule from
     * this class's earlier design (a continuous +/-windowSize tolerance
     * centered on each row's own timestamp, with no shared bucket at all;
     * confirmed non-equivalent by hand-calculated cardinality mismatches).
     * "bucket" (Spark's window(ts, windowSize) struct, the same bucketing
     * primitive windowAvg's groupBy already used) is the closest Spark
     * equivalent of "the same window instance" and is computed ONCE here,
     * immediately after the watermark, then propagated unchanged through
     * every filter/join/windowAvg call -- never recomputed downstream. This
     * also eliminates the "Redefining watermark is disallowed" bug class
     * entirely: the watermark is declared exactly once, at the earliest
     * possible point, and nothing downstream calls .withWatermark() again. */
    protected Dataset<Row> source(String name, int keySpace, int parallelism) {
        Dataset<Row> raw = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", cfg.eventRate)
                .load();
        Dataset<Row> withKV = raw
                .withColumn("key", rand().multiply(lit((double) keySpace)).cast("long").cast("string"))
                .withColumn("val", rand())
                .select(col("timestamp").as("ts"), col("key"), col("val"))
                .withWatermark("ts", (cfg.windowSize * 2) + " milliseconds")
                .withColumn("bucket", window(col("ts"), cfg.windowSize + " milliseconds"))
                .repartition(parallelism);
        return withKV.observe(name, count(lit(1)).cast("double").as("rows"));
    }

    /** Keeps rows with val &lt; threshold -- matching Flink's
     * FilterFunction exactly (dsp_flink_synthetic_jobs' SyntheticTopology:
     * "t.f1 &lt; threshold"). Found live, 2026-10-15 during the join-semantics
     * review: an earlier version of this method kept val &gt;= threshold, the
     * OPPOSITE direction. For a 0.5 threshold against a uniform [0,1) value
     * this happens to average out to the same ~50% selectivity, but
     * ChainedFiltersTopology's THRESHOLDS {0.52,0.50,0.48,0.46} are chosen
     * to make successive filters progressively more/less selective in a
     * specific direction -- the wrong comparison direction silently
     * inverted that selectivity trend, which would have produced
     * misleading cross-engine comparisons without ever throwing an error. */
    protected Dataset<Row> filter(Dataset<Row> in, String name, double threshold, int parallelism) {
        Dataset<Row> filtered = in.filter(col("val").lt(lit(threshold))).repartition(parallelism);
        return filtered.observe(name, count(lit(1)).cast("double").as("rows"));
    }

    /** Windowed average of "val" grouped by "key" -- the one shuffle stage
     * every shape except the joins has. Sets spark.sql.shuffle.partitions
     * directly (see class javadoc: for shapes with no join, this IS the
     * query's only shuffle, so setting it here, immediately before
     * runAndSelfCancel()'s .start(), is correct without any extra
     * bookkeeping -- JoinTopology overrides this by recording the value via
     * shuffleWidth instead of setting it immediately, since joins come
     * first in that shape and only the LAST .set() before .start() matters
     * anyway).
     *
     * <b>Groups by the already-computed "bucket" column, not a fresh
     * window(ts,...) call -- simplified 2026-10-15 (see source()'s
     * javadoc).</b> "bucket" was computed once in source() and propagated
     * unchanged through every filter/join call, so this is already exactly
     * Flink's per-window aggregation grouping key; recomputing window(ts,
     * windowSizeMs) here would be redundant (and the parameter is kept only
     * so callers' intent still reads naturally at each call site -- the
     * actual bucketing always traces back to source()'s single windowSize). */
    protected Dataset<Row> windowAvg(Dataset<Row> in, String name, int windowSizeMs, int parallelism) {
        applyShuffleWidth(parallelism);
        // "ts" is derived from bucket.end (the window's own end boundary),
        // NOT by aggregating the raw watermarked "ts" column directly
        // (e.g. max("ts")) -- confirmed live, 2026-10-20: doing the latter
        // silently stops this stage (and everything downstream) from ever
        // emitting a single row in append mode, no error logged, even
        // though the underlying streaming query keeps running and its
        // state store keeps evicting rows normally. Root cause not
        // confirmed beyond "aggregating the column Spark tracks as the
        // watermark source inside the SAME groupBy that performs the
        // window-eviction check confuses append-mode's emission-
        // eligibility logic" -- but "bucket" (a plain struct derived from
        // ts, not itself watermark-tracked) has safely survived every
        // existing aggregation/join in this file already, so deriving "ts"
        // from it sidesteps the issue entirely rather than explaining it
        // further.
        Dataset<Row> windowed = in
                .groupBy(col("bucket"), col("key"))
                .agg(avg("val").as("val"))
                .select(col("key"), col("val"), col("bucket").getField("end").as("ts"))
                .repartition(parallelism);
        return windowed.observe(name, count(lit(1)).cast("double").as("rows"));
    }

    /** Stream-stream join on (key, bucket) EQUALITY -- same combine
     * semantics as Flink's JoinFunction (x,y) -> (x.key, (x.val+y.val)/2).
     * Both sides carry same-named "key"/"val"/"bucket" columns, so each
     * side is aliased before the join to avoid an ambiguous-column
     * resolution error.
     *
     * <b>Redesigned 2026-10-15, replacing an earlier +/-windowSize sliding
     * time-range condition -- see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md for
     * the full Flink-vs-Spark semantics review that motivated this.</b> A
     * sliding tolerance centered on each row's own timestamp is NOT the
     * same rule as Flink's windowed join (which matches same-key tuples
     * that landed in the SAME discrete, globally epoch-aligned tumbling
     * window instance) -- confirmed non-equivalent by hand-calculated
     * cardinality mismatches (a sliding join can match pairs Flink's rule
     * would reject across a window boundary, and miss pairs Flink's rule
     * would accept within the same window but >windowSize apart in raw
     * time). Joining on "bucket" equality (the same window(ts,windowSize)
     * struct windowAvg groups by) reproduces Flink's same-key,
     * same-window-instance CARDINALITY RULE, instead of approximating it
     * with a tolerance -- not Flink's windowing mechanism itself: this is
     * still event-time/watermark-based bucketing, where Flink's is
     * processing-time tumbling windows. See
     * SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md §11 for that remaining
     * difference. Verified against a hand-crafted deterministic dataset covering all
     * four boundary cases (same-bucket match, cross-boundary non-match,
     * multi-row cross-product, different-key non-match) via spark-shell
     * before being ported here -- see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md
     * for the exact inputs/expected/actual cardinalities.
     *
     * No watermark bookkeeping needed here at all (contrast with the
     * earlier version's leftAlreadyWatermarked flag): the watermark is
     * declared exactly once, in source(), and this method never calls
     * .withWatermark() -- "bucket" and the propagated watermark both just
     * flow through unchanged. */
    protected Dataset<Row> pairwiseJoin(Dataset<Row> left, Dataset<Row> right, String name, int parallelism) {
        applyShuffleWidth(parallelism);
        Dataset<Row> l = left.select(col("key").as("lkey"), col("val").as("lval"), col("bucket").as("lbucket"), col("ts").as("lts"));
        Dataset<Row> r = right.select(col("key").as("rkey"), col("val").as("rval"), col("bucket").as("rbucket"), col("ts").as("rts"));
        org.apache.spark.sql.Column joinCondition = l.col("lkey").equalTo(r.col("rkey"))
                .and(l.col("lbucket").equalTo(r.col("rbucket")));
        // "ts" is the max of both sides -- the joined row couldn't have been
        // emitted until BOTH its contributing rows had arrived (same
        // "latest contributor" reasoning as windowAvg()'s own comment).
        Dataset<Row> joined = l.join(r, joinCondition, "inner")
                .select(col("lkey").as("key"), ((col("lval").plus(col("rval"))).divide(2.0)).as("val"),
                        col("lbucket").as("bucket"), greatest(col("lts"), col("rts")).as("ts"))
                .repartition(parallelism);
        return joined.observe(name, count(lit(1)).cast("double").as("rows"));
    }

    /** Records the shuffle-width value without applying it yet. Only the
     * LAST call's value before .start() actually takes effect (see class
     * javadoc), so repeatedly overwriting shuffleWidth as the pipeline is
     * built, then applying it exactly once right before .start(), gives
     * the same end result as applying it at every call site -- without
     * misleadingly suggesting each call site gets its own independent
     * value. */
    private void applyShuffleWidth(int parallelism) {
        this.shuffleWidth = parallelism;
        spark.conf().set("spark.sql.shuffle.partitions", parallelism);
    }

    /** True end-to-end latency (source row generation -> sink), unlike
     * WordCountJob.java's real-workload convention (ingestion-to-processing
     * lag measured immediately at the source, before any pipeline
     * processing happens -- see that file's own comment on why it stops
     * there). "ts" has been propagated through every filter/windowAvg/join
     * call all the way to here (see each method's own comment), so this
     * measures the SAME thing Flink's latency markers and Kafka Streams'
     * KIP-613 metric measure -- added specifically so Spark's synthetic
     * shapes are comparable to those two engines on this metric, not to
     * match Spark's own real-workload approximation (see
     * docs/14_issue_log.md SPARK-005's sibling finding: this metric was
     * simply never collected for synthetic jobs before this).
     *
     * current_timestamp() must be materialized into its own column before
     * observe() for the same non-deterministic-expression reason
     * WordCountJob.java's own comment documents. Metric name/column alias
     * ("end_to_end_lag"/"rows") matches that file's convention exactly, so
     * getprom_spark.py's existing getEndToEndLatency() needed no query
     * changes -- only wiring it into the synthetic dispatch path. */
    protected void sink(Dataset<Row> in, String name, int parallelism) {
        Dataset<Row> withLag = in.withColumn("lag_seconds",
                current_timestamp().cast("double").minus(col("ts").cast("double")));
        // Both observe() calls are now on the repartitioned Dataset (same
        // post-shuffle stage), not split across the repartition boundary.
        // Found live, 2026-10-31: for join shapes (which already have a
        // join/window-agg shuffle upstream), an observe() placed BEFORE
        // this repartition's shuffle was silently dropped -- confirmed via
        // direct Prometheus query: the job's other operatorMetrics (filter/
        // join/sink row counts, including this same repartition's own
        // "name" observe placed after it) all reported correctly for the
        // same run, but "end_to_end_lag" never appeared as a metric series
        // at all, not just delayed. Single-input shapes never hit this
        // because build() verified empirically that a Spark Catalyst
        // optimization can report observed metrics from only the LAST
        // distinct shuffle stage in a job that has more than one; the
        // repartition() here is that last stage for a join shape, but was
        // the ONLY stage (so the only reported one) for single-input
        // shapes. Averaging/counting after a repartition is mathematically
        // identical to before it -- repartition doesn't add, drop, or
        // duplicate rows -- so this changes nothing about what either
        // metric measures.
        Dataset<Row> repartitioned = withLag.repartition(parallelism);
        Dataset<Row> withLagObserved = repartitioned.observe("end_to_end_lag",
                avg(col("lag_seconds")).cast("double").as("rows"));
        Dataset<Row> observed = withLagObserved
                .observe(name, count(lit(1)).cast("double").as("rows"));
        this.finalSink = observed;
    }

    private Dataset<Row> finalSink;

    /** Re-applies shuffleWidth (if a join/window-agg stage set one) right
     * before .start(), since that is the only moment that actually matters
     * -- then runs the self-cancel lifecycle matching dsp_spark_jobs'
     * WordCountJob.java (query.awaitTermination(seconds*1000);
     * query.stop();), and the real-workload runner's expectations
     * (infra/runner.py's _run_spark_job() matches by --name on the Spark
     * master's JSON status API, not a job id). */
    protected void runAndSelfCancel() throws Exception {
        if (shuffleWidth != null) {
            spark.conf().set("spark.sql.shuffle.partitions", shuffleWidth);
        }
        String checkpointLocation = cfg.checkpointRoot + "/" + cfg.name + "-" + System.currentTimeMillis();
        // Pre-create the checkpoint subdirectories the driver would
        // otherwise create lazily on first write, and pause briefly before
        // .start() -- found live, 2026-10-16, first real distributed sweep
        // run (not just a single manual test): on a shared NFS mount, a
        // task running on a DIFFERENT physical node than the driver can
        // still hit FileNotFoundException on "commits"/"offsets" even
        // though the driver already created the parent checkpoint dir --
        // an NFS attribute-cache visibility race (the directory's
        // existence takes a moment to become visible cluster-wide), not
        // fixed by moving off a node-local path alone. Creating the
        // subdirectories here (from the driver, before any executor could
        // possibly need them) and waiting lets that propagation happen
        // ahead of time instead of racing the first micro-batch.
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(checkpointLocation, "offsets"));
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(checkpointLocation, "commits"));
            Thread.sleep(3000);
        } catch (Exception e) {
            System.out.println("[main] WARNING: checkpoint pre-create failed (continuing anyway): " + e);
        }
        // "append" (not "update") -- required by Spark for any query
        // containing a stream-stream join (confirmed live, 2026-10-15:
        // "update" throws AnalysisException "Join between two streaming
        // DataFrames/Datasets is not supported in Update output mode, only
        // in Append output mode"). Applied universally (not just for
        // JoinTopology) for one lifecycle code path across all 9 shapes --
        // append mode is also valid for the plain windowed aggregations in
        // LinearQueryTopology/ChainedFiltersTopology (re-validated locally
        // under this mode after the switch), just with the standard
        // append-mode semantics of only emitting a window once the
        // watermark has passed it, instead of emitting partial/updating
        // results as update mode would.
        StreamingQuery query = finalSink.writeStream()
                .format("noop")
                .option("checkpointLocation", checkpointLocation)
                .outputMode("append")
                .start();

        System.out.println("[main] Query started, will run for " + cfg.waitTimeToCancel + " seconds.");
        query.awaitTermination(cfg.waitTimeToCancel * 1000);
        query.stop();
        System.out.println("[main] Query stopped.");
    }
}
