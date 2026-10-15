package com.kom.dsp.synthetic;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

/**
 * Storm's windowed JoinBolt with local-run counters: tuples entering each window
 * (in), tuples emitted by the join (out), and time spent joining (busy).
 */
public class MeteredJoinBolt extends JoinBolt {
    static final String STAGE = "join";
    private transient LocalMetrics.Stage stage;

    public MeteredJoinBolt(String stream, String field) {
        super(stream, field);
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.stage = LocalMetrics.stage(STAGE, context.getComponentTasks(context.getThisComponentId()).size());
        super.prepare(conf, context, new OutputCollector(new CountingCollector(collector, stage)));
    }

    @Override
    public void execute(TupleWindow window) {
        long start = System.nanoTime();
        stage.in.addAndGet(window.get().size());
        super.execute(window);
        stage.busyNanos.addAndGet(System.nanoTime() - start);
    }

    /** Forwards every call to the real collector and counts each emitted tuple. */
    private static final class CountingCollector implements IOutputCollector {
        private final IOutputCollector delegate;
        private final LocalMetrics.Stage stage;

        CountingCollector(IOutputCollector delegate, LocalMetrics.Stage stage) {
            this.delegate = delegate;
            this.stage = stage;
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            stage.out.incrementAndGet();
            return delegate.emit(streamId, anchors, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            stage.out.incrementAndGet();
            delegate.emitDirect(taskId, streamId, anchors, tuple);
        }

        @Override
        public void ack(Tuple input) {
            delegate.ack(input);
        }

        @Override
        public void fail(Tuple input) {
            delegate.fail(input);
        }

        @Override
        public void resetTimeout(Tuple input) {
            delegate.resetTimeout(input);
        }

        @Override
        public void reportError(Throwable error) {
            delegate.reportError(error);
        }

        @Override
        public void flush() {
            delegate.flush();
        }
    }
}
