package org.example.flink.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.example.flink.model.Message;
import org.example.flink.util.StreamResources;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PeriodicEmitSourceFunction implements ParallelSourceFunction<Message>, CheckpointedFunction {

    private boolean cancel = false;

    private final long interval;


    public PeriodicEmitSourceFunction(long interval) {
        this.interval = interval;
    }

    private transient ListState<Tuple2<String, Long>> offsets;

    private Tuple2<String, Long> buffer;

    private String name;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsets.clear();
        offsets.add(buffer);
    }


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        name = String.valueOf(StreamResources.COUNTER.incrementAndGet());
        ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>("counter",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
        offsets = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            buffer = offsets.get().iterator().next();
        }
    }



    @Override
    public void run(SourceContext<Message> ctx) throws Exception {
        AtomicLong count = buffer == null ? new AtomicLong(0) : new AtomicLong(buffer.f1);

        while (!cancel) {
            count.incrementAndGet();
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Message(UUID.randomUUID().toString(), System.currentTimeMillis(), count.toString()));
                buffer = new Tuple2<>(name, count.get());
            }
            TimeUnit.MILLISECONDS.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
