package org.example.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.flink.util.ConcurrentUtils;
import org.example.flink.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * end-to-end exactly once
 *
 * Exactly Once Guarantees
 * When things go wrong in a stream processing application, it is possible to have either lost,
 * or duplicated results. With Flink, depending on the choices you make for your application and
 * the cluster you run it on, any of these outcomes is possible:
 *
 * Flink makes no effort to recover from failures (at most once)
 * Nothing is lost, but you may experience duplicated results (at least once)
 * Nothing is lost or duplicated (exactly once)
 * Given that Flink recovers from faults by rewinding and replaying the source data streams,
 * when the ideal situation is described as exactly once this does not mean that every event
 * will be processed exactly once. Instead, it means that every event will affect the state
 * being managed by Flink exactly once.
 *
 * Barrier alignment is only needed for providing exactly once guarantees. If you don’t need
 * this, you can gain some performance by configuring Flink to use CheckpointingMode.AT_LEAST_ONCE,
 * which has the effect of disabling barrier alignment.
 *
 * Exactly Once End-to-end
 * To achieve exactly once end-to-end, so that every event from the sources affects the sinks
 * exactly once, the following must be true:
 * - your sources must be replayable, and
 * - your sinks must be transactional (or idempotent)
 *
 *
 * Method : transaction is satiated to checkpointing, only checkpointing done, if will submit
 * the transaction of sink operation.
 *
 *
 * @see TwoPhaseCommitSinkFunction
 *
 */
public class ExactlyOnceTwoPhaseCommitDemo {

    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceTwoPhaseCommitDemo.class);


    public static class IntEmiter implements SourceFunction<Integer>, CheckpointedFunction {

        private boolean cancel = false;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            AtomicInteger count = buffer == 0 ? new AtomicInteger(0) : new AtomicInteger(buffer);
            while (!cancel) {
                count.incrementAndGet();
                synchronized (ctx.getCheckpointLock()) {
                    logger.info("[SRC##] emit: {}", count.get());
                    ctx.collect(count.get());
                    buffer = count.get();
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


    public static class MessageProducer {
        public static AtomicInteger unCommitOffset = new AtomicInteger(0);
        public static AtomicInteger commitOffset = new AtomicInteger(0);
        public static List<Tuple2<Integer, Integer>> datum = new CopyOnWriteArrayList<>();
        public List<Tuple2<Integer, Integer>> dataInTxn = new CopyOnWriteArrayList<>();


        public void produce(int data) {
            unCommitOffset.incrementAndGet();
            dataInTxn.add(new Tuple2<>(unCommitOffset.get(), data));
        }

        public void commitTransaction() {
            logger.info("[PDC@@] commitTxn : commitOffset: {}, unCommitOffset: {}, dataInTxn: {}, datum: {}, pid: {}", commitOffset, unCommitOffset, dataInTxn, datum, this);
            commitOffset.set(unCommitOffset.get());
        }

        public void abortTransaction() {
            logger.info("[PDC@@] abortTxn : commitOffset : {}, unCommitOffset: {}, dataInTxn: {}, datum: {}, pid: {}", commitOffset, unCommitOffset, dataInTxn, datum, this);
            unCommitOffset.set(commitOffset.get());
        }

        public void flush() {
            datum.addAll(dataInTxn);
            dataInTxn.clear();
        }
    }


    public static class MyTransactionState {

        public MessageProducer messageProducer;

        public String transactionalId;

        public MyTransactionState(MessageProducer messageProducer, String transactionalId) {
            this.messageProducer = messageProducer;
            this.transactionalId = transactionalId;
        }
    }


    /**
     * Context could be null:
     * @see TwoPhaseCommitSinkFunction#initializeUserContext
     * @see TwoPhaseCommitSinkFunction#userContext
     */
    public static class MyTransactionContext {
        public Set<String> transactionalIds = new HashSet<>();
    }


    public static class MyTwoPhaseCommitSinkFunction extends TwoPhaseCommitSinkFunction<Integer, MyTransactionState, MyTransactionContext> {


        public MyTwoPhaseCommitSinkFunction(TypeSerializer<MyTransactionState> transactionSerializer, TypeSerializer<MyTransactionContext> contextSerializer) {
            super(transactionSerializer, contextSerializer);
        }


        @Override
        protected void invoke(MyTransactionState transaction, Integer value, Context context) throws Exception {
            logger.info("[SNK--] invoke: {}, tid : {}", value, transaction.transactionalId);
            transaction.messageProducer.produce(value);

            produceCount.incrementAndGet();
            if (produceCount.get() == 10) {
                logger.info("------------- crush ---------------");
                throw new RuntimeException();
            }
        }

        @Override
        protected MyTransactionState beginTransaction() throws Exception {
            MyTransactionState transaction = new MyTransactionState(new MessageProducer(), IdGenerator.next());
            logger.info("[SNK--] beginTransaction, tid : {}", transaction.transactionalId);
            return transaction;
        }


        private final AtomicInteger produceCount = new AtomicInteger();


        /**
         * Flush the data before checkpointing, to avoid persisting in state, otherwise,
         * the data will be restored in transaction and commit again.
         */
        @Override
        protected void preCommit(MyTransactionState transaction) throws Exception {
            logger.info("[SNK--] preCommit, tid : {}", transaction.transactionalId);
            transaction.messageProducer.flush();
        }

        @Override
        protected void commit(MyTransactionState transaction) {
            logger.info("[SNK--] commit, tid : {}", transaction.transactionalId);
            transaction.messageProducer.commitTransaction();
        }

        /**
         * Note that this method may cause twice transaction commit and lead to duplicate data.
         *
         * {@link TwoPhaseCommitSinkFunction.State} is built in {@link super#snapshotState(FunctionSnapshotContext)} when 
         * performing checkpointing with a field {@code pendingCommitTransactions}, if it stores the transaction data, then
         * on restoring, the transaction data will also be restored and committed again in  {@link super#initializeState(FunctionInitializationContext)},
         * this will result in duplicate data for committing twice transaction.
         *
         * Consider an example here:
         * flink completed one checkpoint, let's say chk-1, and committed the bounded transaction txn-1, then it failed
         * before next checkpoint chk-2, it will restore from the chk-1 and will also bring back the transaction txn-1,
         * then commit again in {@link super#recoverAndCommit(Object)}
         *
         * So usually, we should flush data at {@link this#preCommit}, we do not store actual data in state when checkpointing
         * but an offset instead, which could be committed twice and cause no trouble. 
         * And, if program crushed between {@link this#preCommit}(or checkpointing, or snapshot, all at the same time) and {@link this#commit},
         * the pendingCommitTransaction will be reconstructed and commit again.
         */
        @Override
        protected void recoverAndCommit(MyTransactionState transaction) {
            super.recoverAndCommit(transaction);
        }

        @Override
        protected void abort(MyTransactionState transaction) {
            logger.info("[SNK--] abort, tid : {}", transaction.transactionalId);
            transaction.messageProducer.abortTransaction();
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        StateBackend stateBackend = new FsStateBackend("file:///D:/tmp/checkpoints");
        env.setStateBackend(stateBackend);

        DataStreamSource<Integer> personStream = env.addSource(new IntEmiter());

        TypeSerializer<MyTransactionState> stateTypeSerializer = TypeInformation.of(new TypeHint<MyTransactionState>() {}).createSerializer(env.getConfig());
        TypeSerializer<MyTransactionContext> contextTypeSerializer = TypeInformation.of(new TypeHint<MyTransactionContext>() {}).createSerializer(env.getConfig());


        personStream.addSink(new MyTwoPhaseCommitSinkFunction(stateTypeSerializer, contextTypeSerializer)).setParallelism(1);

        env.execute();
    }



}
