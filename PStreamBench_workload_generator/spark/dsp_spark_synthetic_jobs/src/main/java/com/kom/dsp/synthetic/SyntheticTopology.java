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
     * .observe() for (see MetricsUtils). */
    protected void registerMetrics(String... observationNames) {
        MetricsUtils.registerOperatorMetrics(spark, observationNames);
    }

    /** Source: Spark's built-in "rate" format, independent rand()-derived
     * key/value columns (see class javadoc), repartition(parallelism) is
     * the narrow-stage task-count control (no shuffle involved -- a plain
     * repartition(n) always forces exactly n partitions for its own
     * Exchange, unaffected by spark.sql.shuffle.partitions, which only
     * governs shuffles Spark itself plans for groupBy/join). */
    protected Dataset<Row> source(String name, int keySpace, int parallelism) {
        Dataset<Row> raw = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", cfg.eventRate)
                .load();
        Dataset<Row> withKV = raw
                .withColumn("key", rand().multiply(lit((double) keySpace)).cast("long").cast("string"))
                .withColumn("val", rand())
                .select(col("timestamp").as("ts"), col("key"), col("val"))
                .repartition(parallelism);
        return withKV.observe(name, count(lit(1)).cast("double").as("rows"));
    }

    protected Dataset<Row> filter(Dataset<Row> in, String name, double threshold, int parallelism) {
        Dataset<Row> filtered = in.filter(col("val").geq(lit(threshold))).repartition(parallelism);
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
     * anyway). windowSize is also the slide (tumbling, not sliding),
     * matching Flink/Storm's synthetic convention. */
    protected Dataset<Row> windowAvg(Dataset<Row> in, String name, int windowSizeMs, int parallelism) {
        applyShuffleWidth(parallelism);
        Dataset<Row> windowed = in
                .withWatermark("ts", (windowSizeMs * 2) + " milliseconds")
                .groupBy(window(col("ts"), windowSizeMs + " milliseconds"), col("key"))
                .agg(avg("val").as("val"))
                .select(col("key"), col("val"))
                .repartition(parallelism);
        return windowed.observe(name, count(lit(1)).cast("double").as("rows"));
    }

    /** Stream-stream equi-join on "key", averaging the two "val" columns --
     * same combine semantics as Flink's JoinFunction (x,y) -> (x.key,
     * (x.val+y.val)/2). Both sides carry same-named "key"/"val"/"ts"
     * columns, so each side is aliased before the join to avoid an
     * ambiguous-column resolution error.
     *
     * <b>Watermarked time-range condition is required, not optional --
     * found live, 2026-10-15.</b> An earlier version had the equi-join
     * condition alone, reasoning that a time-range predicate is "only
     * required to let Spark evict state, acceptable for short runs". That
     * was wrong: a 2-way join survived a 25s local test, but 4-way/6-way
     * joins (3-5 chained joins, each accumulating unbounded state with no
     * eviction) OOM'd the local driver within ~20-25s -- i.e. well inside
     * a single real sweep run's 5-minute window, not just a local-testing
     * artifact. Added a watermark on each side plus a symmetric time-range
     * join condition (matching rows must have timestamps within one
     * windowSize of each other), which lets Spark's
     * StreamingSymmetricHashJoinExec evict state outside that range --
     * confirmed live afterwards: 6-way join ran its full local test
     * without OOM (see SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md). */
    protected Dataset<Row> pairwiseJoin(Dataset<Row> left, Dataset<Row> right, String name, int parallelism) {
        applyShuffleWidth(parallelism);
        String watermarkDelay = (cfg.windowSize * 2) + " milliseconds";
        Dataset<Row> l = left.select(col("key").as("lkey"), col("val").as("lval"), col("ts").as("lts"))
                .withWatermark("lts", watermarkDelay);
        Dataset<Row> r = right.select(col("key").as("rkey"), col("val").as("rval"), col("ts").as("rts"))
                .withWatermark("rts", watermarkDelay);
        org.apache.spark.sql.Column timeRange = r.col("rts").between(
                l.col("lts").minus(expr("INTERVAL " + cfg.windowSize + " MILLISECONDS")),
                l.col("lts").plus(expr("INTERVAL " + cfg.windowSize + " MILLISECONDS")));
        org.apache.spark.sql.Column joinCondition = l.col("lkey").equalTo(r.col("rkey")).and(timeRange);
        Dataset<Row> joined = l.join(r, joinCondition, "inner")
                .select(col("lkey").as("key"), ((col("lval").plus(col("rval"))).divide(2.0)).as("val"), col("lts").as("ts"))
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

    protected void sink(Dataset<Row> in, String name, int parallelism) {
        Dataset<Row> observed = in.repartition(parallelism)
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
        String checkpointLocation = "/tmp/spark-checkpoints/" + cfg.name + "-" + System.currentTimeMillis();
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
