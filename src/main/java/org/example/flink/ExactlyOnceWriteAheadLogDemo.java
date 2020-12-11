package org.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.example.flink.util.ConcurrentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * Steps of {@link GenericWriteAheadSink} to handle checkpointing and failover/recovery:
 * {@link GenericWriteAheadSink#open()} clean the committed PendingCheckpoint from {@link CheckpointCommitter}
 * {@link GenericWriteAheadSink#initializeState(StateInitializationContext)} initialize state and restore PendingCheckpoint from checkpointed state
 * {@link GenericWriteAheadSink#snapshotState(StateSnapshotContext)} create PendingCheckpoints and store in state
 * {@link GenericWriteAheadSink#notifyCheckpointComplete(long)} store the current committed checkpoint in {@link CheckpointCommitter},
 * and remove the previous committed PendingCheckpoints in state
 *
 *
 * Notes:
 * 1) write ahead log不能实现真正的exactly once 传输，下列两种情况会导致数据重复
 * - sendValues()在向外部commit数据时crash，若外部系统不支持原子写，则恢复后replay会造成数据重复。
 * - sendValues()方法成功完成，但程序在CheckpointCommitter在被调用前或者CheckpointCommitter commit checkpointid失败，Sink在recovery时会重复提交。
 * 2) 要避免重复写，需要外部系统支持upsert(更新插入)操作的key-value存储或关系型数据库，后续的commit会覆盖之前commit失败的数据，达到最终一致性。
 *    与幂等性Sink不同的是，WAL sink向外部系统提交的是处于global commit的数据，消除了不确定性带来的影响。
 * 3) 影响性能，buffer会占用内存数据，sendValues()时阻塞Sink，降低影响吞吐。
 *
 */
public class ExactlyOnceWriteAheadLogDemo {


    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceTwoPhaseCommitDemo.class);


    /**
     * emit number per seconds, and will crash per 10 numbers
     */
    public static class IntEmiter implements SourceFunction<Integer>, CheckpointedFunction {

        private boolean cancel = false;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            AtomicInteger count = buffer == 0 ? new AtomicInteger(0) : new AtomicInteger(buffer);
            int loop = 0;
            while (!cancel) {
                count.incrementAndGet();
                synchronized (ctx.getCheckpointLock()) {
                    logger.info("[SRC##] emit: {}", count.get());
                    ctx.collect(count.get());
                    buffer = count.get();
                }
                loop++;
                if (loop == 10) {
                    logger.info("------------- crash ---------------");
                    throw new RuntimeException("crash");
                }
                ConcurrentUtils.sleep(TimeUnit.SECONDS, 1);
            }
        }

        @Override
        public void cancel() {
            cancel = true;
        }

        private transient ListState<Integer> offsets;

        private int buffer;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsets.clear();
            offsets.add(buffer);
            logger.info("[SRC##] checkpoint: {}", buffer);
        }


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("counter",
                    TypeInformation.of(new TypeHint<Integer>() {}));
            offsets = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                logger.info("[SRC##] restore from: " + offsets.get());
                buffer = offsets.get().iterator().next();
            }
        }
    }


    /**
     * This class is used to save information about which sink operator instance has committed checkpoints to a backend.
     * <p/>
     * The current checkpointing mechanism is ill-suited for sinks relying on backends that do not support roll-backs.
     * When dealing with such a system, while trying to get exactly-once semantics,
     * one may neither commit data while creating the snapshot (since another sink instance may fail, leading to a replay on the same data)
     * nor when receiving a checkpoint-complete notification (since a subsequent failure would leave us with no knowledge as to whether data
     * was committed or not).
     * <p/>
     * A CheckpointCommitter can be used to solve the second problem by saving whether an instance committed all data
     * belonging to a checkpoint. This data must be stored in a backend that is persistent across retries,
     * which rules out(against) Flink's state mechanism, and accessible from all machines, like a database or distributed file.
     *
     * Note: the logic of storing and checking the checkpoint commitment should be implemented by users in
     * {@link CheckpointCommitter#commitCheckpoint(int, long)} to persist the commitment, and
     * {@link CheckpointCommitter#isCheckpointCommitted(int, long)} to retrieve from the persisted data.
     *
     * <p/>
     * There is no mandate as to how the resource is shared; there may be one resource for all Flink jobs, or one for
     * each job/operator/-instance separately. This implies that the resource must not be cleaned up by the system itself,
     * and as such should kept as small as possible.
     */
    public static class MyCheckpointCommitter extends CheckpointCommitter {


        private static final List<Tuple2<Integer, Long>> ids = new LinkedList<>();


        @Override
        public void open() throws Exception {

        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public void createResource() throws Exception {

        }

        @Override
        public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
            ids.add(new Tuple2<>(subtaskIdx, checkpointID));
            logger.info("[CMT--] commitCheckpoint, subtaskIdx: {}, checkpointID: {}, ids: {}", subtaskIdx, checkpointID, ids);
        }

        @Override
        public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
            logger.info("[CMT--] isCheckpointCommitted, subtaskIdx: {}, checkpointID: {}, ids: {}", subtaskIdx, checkpointID, ids);
            for (Tuple2<Integer, Long> id : ids) {
                if (id.f0 == subtaskIdx && id.f1 == checkpointID) {
                    return true;
                }
            }
            return false;
        }
    }


    public static final List<Integer> datum = new CopyOnWriteArrayList<>();


    /**
     * Generic Sink that emits its input elements into an arbitrary backend. This sink is integrated with Flink's checkpointing
     * mechanism and can provide exactly-once guarantees; depending on the storage backend and sink/committer implementation.
     *
     * <p>Incoming records are stored within a {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only committed if a
     * checkpoint is completed.
     *
     */
    public static class MyGenericWriteAheadSink extends GenericWriteAheadSink<Integer> {

        public MyGenericWriteAheadSink(CheckpointCommitter committer, TypeSerializer<Integer> serializer, String jobID) throws Exception {
            super(committer, serializer, jobID);
        }

        @Override
        protected boolean sendValues(Iterable<Integer> values, long checkpointId, long timestamp) throws Exception {
            datum.addAll(StreamSupport.stream(values.spliterator(), false).collect(Collectors.toList()));
            logger.info("[SNK@@] send: {}", datum);
            return true;
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        StateBackend stateBackend = new FsStateBackend("file:///D:/tmp/checkpoints");
        env.setStateBackend(stateBackend);


        String jobId = "job-1";

        DataStreamSource<Integer> source = env.addSource(new IntEmiter());

        TypeInformation<Integer> integerTypeInformation = TypeInformation.of(new TypeHint<Integer>() {});
        TypeSerializer<Integer> integerTypeSerializer = integerTypeInformation.createSerializer(env.getConfig());
        MyGenericWriteAheadSink myGenericWriteAheadSink = new MyGenericWriteAheadSink(new MyCheckpointCommitter(), integerTypeSerializer, jobId);

        SingleOutputStreamOperator<Integer> transform = source.transform("wal", integerTypeInformation, myGenericWriteAheadSink).setParallelism(1);

        transform.print();

        env.execute();
    }

}


//    @Override
//    public void initializeState(StateInitializationContext context) throws Exception {
//        super.initializeState(context);
//
//        Preconditions.checkState(this.checkpointedState == null,
//                "The reader state has already been initialized.");
//
//        // We are using JavaSerializer from the flink-runtime module here. This is very naughty and
//        // we shouldn't be doing it because ideally nothing in the API modules/connector depends
//        // directly on flink-runtime. We are doing it here because we need to maintain backwards
//        // compatibility with old state and because we will have to rework/remove this code soon.
//        checkpointedState = context
//                .getOperatorStateStore()
//                .getListState(new ListStateDescriptor<>("pending-checkpoints", new JavaSerializer<>()));
//
//        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
//        if (context.isRestored()) {
//            LOG.info("Restoring state for the GenericWriteAheadSink (taskIdx={}).", subtaskIdx);
//
//            for (GenericWriteAheadSink.PendingCheckpoint pendingCheckpoint : checkpointedState.get()) {
//                this.pendingCheckpoints.add(pendingCheckpoint);
//            }
//
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("GenericWriteAheadSink idx {} restored {}.", subtaskIdx, this.pendingCheckpoints);
//            }
//        } else {
//            LOG.info("No state to restore for the GenericWriteAheadSink (taskIdx={}).", subtaskIdx);
//        }
//    }
//
//
//    /**
//     * Called when a checkpoint barrier arrives. It closes any open streams to the backend
//     * and marks them as pending for committing to the external, third-party storage system.
//     *
//     * @param checkpointId the id of the latest received checkpoint.
//     * @throws IOException in case something went wrong when handling the stream to the backend.
//     */
//    private void saveHandleInState(final long checkpointId, final long timestamp) throws Exception {
//
//        //only add handle if a new OperatorState was created since the last snapshot
//        if (out != null) {
//            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
//            StreamStateHandle handle = out.closeAndGetHandle();
//
//            GenericWriteAheadSink.PendingCheckpoint pendingCheckpoint = new GenericWriteAheadSink.PendingCheckpoint(
//                    checkpointId, subtaskIdx, timestamp, handle);
//
//            if (pendingCheckpoints.contains(pendingCheckpoint)) {
//                //we already have a checkpoint stored for that ID that may have been partially written,
//                //so we discard this "alternate version" and use the stored checkpoint
//                handle.discardState();
//            } else {
//                pendingCheckpoints.add(pendingCheckpoint);
//            }
//            out = null;
//        }
//    }
//
//    @Override
//    public void snapshotState(StateSnapshotContext context) throws Exception {
//        super.snapshotState(context);
//
//        Preconditions.checkState(this.checkpointedState != null,
//                "The operator state has not been properly initialized.");
//
//        saveHandleInState(context.getCheckpointId(), context.getCheckpointTimestamp());
//
//        this.checkpointedState.clear();
//
//        try {
//            for (GenericWriteAheadSink.PendingCheckpoint pendingCheckpoint : pendingCheckpoints) {
//                // create a new partition for each entry.
//                this.checkpointedState.add(pendingCheckpoint);
//            }
//        } catch (Exception e) {
//            checkpointedState.clear();
//
//            throw new Exception("Could not add panding checkpoints to operator state " +
//                    "backend of operator " + getOperatorName() + '.', e);
//        }
//
//        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("{} (taskIdx= {}) checkpointed {}.", getClass().getSimpleName(), subtaskIdx, this.pendingCheckpoints);
//        }
//    }
//
//    @Override
//    public void open() throws Exception {
//        super.open();
//        committer.setOperatorId(id);
//        committer.open();
//
//        checkpointStorage = getContainingTask().getCheckpointStorage();
//
//        cleanRestoredHandles();
//    }
//
//    /**
//     * Called at {@link #open()} to clean-up the pending handle list.
//     * It iterates over all restored pending handles, checks which ones are already
//     * committed to the outside storage system and removes them from the list.
//     */
//    private void cleanRestoredHandles() throws Exception {
//        synchronized (pendingCheckpoints) {
//
//            Iterator<GenericWriteAheadSink.PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
//            while (pendingCheckpointIt.hasNext()) {
//                GenericWriteAheadSink.PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();
//
//                if (committer.isCheckpointCommitted(pendingCheckpoint.subtaskId, pendingCheckpoint.checkpointId)) {
//                    pendingCheckpoint.stateHandle.discardState();
//                    pendingCheckpointIt.remove();
//                }
//            }
//        }
//    }
//
//    @Override
//    public void notifyCheckpointComplete(long checkpointId) throws Exception {
//        super.notifyCheckpointComplete(checkpointId);
//
//        synchronized (pendingCheckpoints) {
//
//            Iterator<GenericWriteAheadSink.PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
//            while (pendingCheckpointIt.hasNext()) {
//
//                GenericWriteAheadSink.PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();
//
//                long pastCheckpointId = pendingCheckpoint.checkpointId;
//                int subtaskId = pendingCheckpoint.subtaskId;
//                long timestamp = pendingCheckpoint.timestamp;
//                StreamStateHandle streamHandle = pendingCheckpoint.stateHandle;
//
//                if (pastCheckpointId <= checkpointId) {
//                    try {
//                        if (!committer.isCheckpointCommitted(subtaskId, pastCheckpointId)) {
//                            try (FSDataInputStream in = streamHandle.openInputStream()) {
//                                boolean success = sendValues(
//                                        new ReusingMutableToRegularIteratorWrapper<>(
//                                                new InputViewIterator<>(
//                                                        new DataInputViewStreamWrapper(
//                                                                in),
//                                                        serializer),
//                                                serializer),
//                                        pastCheckpointId,
//                                        timestamp);
//                                if (success) {
//                                    // in case the checkpoint was successfully committed,
//                                    // discard its state from the backend and mark it for removal
//                                    // in case it failed, we retry on the next checkpoint
//                                    committer.commitCheckpoint(subtaskId, pastCheckpointId);
//                                    streamHandle.discardState();
//                                    pendingCheckpointIt.remove();
//                                }
//                            }
//                        } else {
//                            streamHandle.discardState();
//                            pendingCheckpointIt.remove();
//                        }
//                    } catch (Exception e) {
//                        // we have to break here to prevent a new (later) checkpoint
//                        // from being committed before this one
//                        LOG.error("Could not commit checkpoint.", e);
//                        break;
//                    }
//                }
//            }
//        }
//    }
