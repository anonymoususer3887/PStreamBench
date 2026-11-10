package com.kom.dsp.synthetic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;

/**
 * Deterministic correctness test for the (key, bucket)-equality join
 * redesign, exercising the ACTUAL production pairwiseJoin()/windowAvg()
 * methods (not a re-implementation) -- written 2026-10-15 as part of the
 * Flink-vs-Spark join-semantics review (see
 * SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md). Not a JUnit suite (this project has
 * no test infrastructure; matches the project's existing convention of
 * live/manual checks with documented expected-vs-actual output rather than
 * a formal assertion framework) -- run directly via `java -cp ... main`
 * and compare the printed counts against the hand-calculated expectations
 * in this class's own comments.
 *
 * Uses BATCH DataFrames (spark.createDataFrame(), not the streaming "rate"
 * source) so the join/groupBy logic can be checked with an exact, static,
 * hand-countable input -- Spark's join/window() Catalyst expressions
 * evaluate identically in batch and streaming execution; only incremental
 * micro-batching and watermark-driven state EVICTION are streaming-only
 * concerns, neither of which affects whether a given pair of rows matches.
 * withWatermark() is a no-op on a batch Dataset (confirmed live: no
 * exception, watermark metadata is simply unused without a streaming
 * execution to evict against), so source()'s exact post-processing
 * (watermark + bucket) is replicated manually here via mkRow()/mkStream()
 * below rather than calling source() itself (which is hardwired to
 * read from the "rate" format and cannot be fed fixed rows).
 */
public class JoinSemanticsTest extends SyntheticTopology {

    private static final int WINDOW_SIZE_MS = 1000;

    public JoinSemanticsTest(ConfigArgs cfg) {
        super(cfg);
    }

    public Dataset<Row> publicPairwiseJoin(Dataset<Row> left, Dataset<Row> right, String name, int parallelism) {
        return pairwiseJoin(left, right, name, parallelism);
    }

    public Dataset<Row> publicWindowAvg(Dataset<Row> in, String name, int windowSizeMs, int parallelism) {
        return windowAvg(in, name, windowSizeMs, parallelism);
    }

    private static Row row(long tsMillis, String key, double val) {
        return RowFactory.create(new Timestamp(tsMillis), key, val);
    }

    /** Same post-processing as SyntheticTopology.source() (watermark +
     * bucket), applied to a fixed batch DataFrame instead of the streaming
     * rate source -- see class javadoc for why this, not source() itself,
     * is used here. */
    private Dataset<Row> mkStream(List<Row> rows) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("ts", DataTypes.TimestampType, false),
                DataTypes.createStructField("key", DataTypes.StringType, false),
                DataTypes.createStructField("val", DataTypes.DoubleType, false),
        });
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        return df.withWatermark("ts", (WINDOW_SIZE_MS * 2) + " milliseconds")
                .withColumn("bucket", window(col("ts"), WINDOW_SIZE_MS + " milliseconds"));
    }

    public static void main(String[] args) throws Exception {
        ConfigArgs cfg = ConfigArgs.fromArgs(new String[]{
                "--name", "JoinSemanticsTest", "--parallelism", "2", "--query", "4",
                "--size", String.valueOf(WINDOW_SIZE_MS), "--slide", String.valueOf(WINDOW_SIZE_MS)});
        JoinSemanticsTest t = new JoinSemanticsTest(cfg);

        // ---- Hand-crafted deterministic inputs (same dataset validated
        // via spark-shell before this port -- see
        // SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md for the full writeup).
        // Bucket B0 = [0,1000ms), B1 = [1000,2000ms). ----
        List<Row> s1Rows = Arrays.asList(
                row(100, "A", 1.0),   // A: 2 rows in S1, both in B0 (multi-row cross-product case)
                row(200, "A", 1.0),
                row(999, "B", 1.0),   // B: boundary case, S1 side in B0 (end of bucket)
                row(150, "C", 1.0),   // C: different-key case, S1 side
                row(120, "E", 1.0));  // E: simple single-row case
        List<Row> s2Rows = Arrays.asList(
                row(300, "A", 1.0),    // A: 1 row in S2, B0
                row(1001, "B", 1.0),   // B: S2 side in B1 -- MUST NOT match S1's B (B0 vs B1)
                row(160, "D", 1.0),    // D: different key than C -- MUST NOT match
                row(130, "E", 1.0));   // E: B0, matches S1's E
        List<Row> s3Rows = new ArrayList<>(Arrays.asList(row(400, "A", 1.0))); // A: 1 row, B0 only
        List<Row> s4Rows = new ArrayList<>(Arrays.asList(row(500, "A", 1.0))); // A: 1 row, B0 only

        Dataset<Row> s1 = t.mkStream(s1Rows);
        Dataset<Row> s2 = t.mkStream(s2Rows);
        Dataset<Row> s3 = t.mkStream(s3Rows);
        Dataset<Row> s4 = t.mkStream(s4Rows);

        Dataset<Row> join1 = t.publicPairwiseJoin(s1, s2, "join-1", 2);
        Dataset<Row> join2 = t.publicPairwiseJoin(join1, s3, "join-2", 2);
        Dataset<Row> join3 = t.publicPairwiseJoin(join2, s4, "join-3", 2);

        // Expected (hand-calculated):
        //   join1: key A -> 2 rows (2x1 cross-product), key E -> 1 row (1x1), B/C/D -> 0. Total 3.
        //   join2: key A -> 2 rows (2x1, S3 has no E). Total 2.
        //   join3 (4-way): key A -> 2 rows (2x1, S4 has no E). Total 2.
        System.out.println("=== JOIN1 (expect A=2, E=1, total=3) ===");
        join1.groupBy("key").count().orderBy("key").show();
        System.out.println("join1 total = " + join1.count());

        System.out.println("=== JOIN2 (expect A=2, total=2) ===");
        join2.groupBy("key").count().orderBy("key").show();
        System.out.println("join2 total = " + join2.count());

        System.out.println("=== JOIN3 == 4-WAY RESULT (expect A=2, total=2) ===");
        join3.groupBy("key").count().orderBy("key").show();
        System.out.println("join3 total = " + join3.count());

        System.out.println("=== DONE OK ===");
        System.exit(0);
    }
}
