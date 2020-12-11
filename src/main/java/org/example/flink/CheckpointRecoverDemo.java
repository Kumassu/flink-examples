package org.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.example.flink.model.Message;
import org.example.flink.util.StreamResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CheckpointRecoverDemo {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointRecoverDemo.class);


    public static class PeriodicCrushSourceFunction implements ParallelSourceFunction<Message>, CheckpointedFunction {

        private boolean cancel = false;


        private final long interval;


        public PeriodicCrushSourceFunction(long interval) {
            this.interval = interval;
        }


        private transient ListState<Tuple2<String, Long>> offsets;

        private Tuple2<String, Long> buffer;

        private String name;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsets.clear();
            offsets.add(buffer);
            logger.info("[Source] checkpoint: " + buffer);
        }


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            name = String.valueOf(StreamResources.COUNTER.incrementAndGet());
            ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>("counter",
                    TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
            offsets = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                logger.info("[Restore] restore from: " + offsets.get());
                buffer = offsets.get().iterator().next();
            }
        }



        @Override
        public void run(SourceContext<Message> ctx) throws Exception {
            AtomicLong count = buffer == null ? new AtomicLong(0) : new AtomicLong(buffer.f1);

            int loop = 0;
            while (!cancel) {
                loop++;
                count.incrementAndGet();
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new Message(UUID.randomUUID().toString(), System.currentTimeMillis(), count.toString()));
                    buffer = new Tuple2<>(name, count.get());
                }
                logger.info("[Source] name: {}, count: {}", name, count);

                // crush every time when emit 10 elements
                if (loop == 10) {
                    logger.info("------------- crash ---------------");
                    throw new RuntimeException("interrupt");
                }
                TimeUnit.MILLISECONDS.sleep(interval);
            }
        }

        @Override
        public void cancel() {
            cancel = true;
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        StateBackend stateBackend = new MemoryStateBackend(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE, true);
//        stateBackend = new FsStateBackend("file:///D:/tmp/checkpoints");
//        stateBackend = new RocksDBStateBackend("file:///D:/tmp/checkpoints", true);

        env.setStateBackend(stateBackend);

        DataStreamSource<Message> sourceStream = env.addSource(new PeriodicCrushSourceFunction(1000)).setParallelism(2);

        sourceStream.print();
        env.execute();
    }
}
