package com.kom.dsp.synthetic;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.util.Random;

/**
 * Generates (key, value) tuples: key uniform over [0, keySpace), value
 * uniform in [0,1). Same data model as dsp_storm_synthetic_jobs'
 * SyntheticSpout -- deliberately simple (not pdspbench-plan-generation's
 * multi-type DataTuple), since the fixed 9 shapes only need a single numeric
 * value (for filter/aggregate) and a single join key.
 *
 * Rate limiting is handled entirely by Flink's DataGeneratorSource, which
 * wraps this DataGenerator in a Guava RateLimiter sized at
 * rowsPerSecond/parallelism per subtask -- no custom throttling needed here,
 * unlike Storm's hand-rolled batched emission.
 */
public class SyntheticGenerator implements DataGenerator<Tuple2<String, Double>> {
    private final int keySpace;
    private transient Random random;

    public SyntheticGenerator(int keySpace) {
        this.keySpace = keySpace;
    }

    @Override
    public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) {
        // Seed per-subtask so parallel source instances don't emit identical sequences.
        this.random = new Random(System.nanoTime() ^ runtimeContext.getIndexOfThisSubtask());
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Tuple2<String, Double> next() {
        String key = String.valueOf(random.nextInt(keySpace));
        double value = random.nextDouble();
        return Tuple2.of(key, value);
    }
}
