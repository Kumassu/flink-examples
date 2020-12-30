package org.example.flink;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.flink.function.AutoKeyFunction;
import org.example.flink.model.KeyedMessage;
import org.example.flink.model.Message;
import org.example.flink.util.ConcurrentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WatermarkDemo {

    private static final Logger logger = LoggerFactory.getLogger(WatermarkDemo.class);


    public static String f(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }


    /**
     * watermark strategy is like: {@link BoundedOutOfOrdernessWatermarks}
     *
     * A WatermarkGenerator for situations where records are out of order, but you can place an upper
     * bound on how far the events are out of order. An out-of-order bound B means that once an
     * event with timestamp T was encountered, no events older than {@code T - B} will follow any more.
     *
     */
    public static class EventTimeEmiter implements SourceFunction<Message> {

        private boolean cancel = false;

        private long[] timestamps = {
                1607236021571L, // 2020-12-06 14:27:01 CST
                1607236022571L, // 2020-12-06 14:27:02 CST
                1607236031571L, // 2020-12-06 14:27:11 CST
                1607236031671L, // 2020-12-06 14:27:12 CST
                1607236031871L, // 2020-12-06 14:27:14 CST
                1607236028571L, // 2020-12-06 14:27:08 CST
                1607236041571L, // 2020-12-06 14:27:21 CST
                1607236038571L, // 2020-12-06 14:27:18 CST
                1607236048571L, // 2020-12-06 14:27:28 CST
                1607236058571L, // 2020-12-06 14:27:38 CST
                1607236068571L, // 2020-12-06 14:27:48 CST
                1607236078571L, // 2020-12-06 14:27:58 CST
        };


        @Override
        public void run(SourceContext<Message> ctx) throws Exception {
            AtomicInteger count = new AtomicInteger(0);
            while (!cancel) {
                count.incrementAndGet();
                synchronized (ctx.getCheckpointLock()) {
                    for (long timestamp : timestamps) {
                        ctx.collectWithTimestamp(new Message(UUID.randomUUID().toString(), timestamp, count.toString()), timestamp);
                        // bounded out-of-order is 2s
                        Watermark watermark = new Watermark(timestamp - 2000);
                        ctx.emitWatermark(watermark);
                        logger.info("[Source] timestamp: {}, watermark: {}", f(timestamp), f(watermark.getTimestamp()));
                        ConcurrentUtils.sleep(TimeUnit.SECONDS, 2);
                    }
                }
                ConcurrentUtils.sleep(TimeUnit.SECONDS, Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            cancel = true;
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Message> sourceStream = env.addSource(new EventTimeEmiter());

        sourceStream.map(new AutoKeyFunction(3))
                .keyBy(KeyedMessage::getPartitionKey)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<KeyedMessage, KeyedMessage, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<KeyedMessage> elements, Collector<KeyedMessage> out) throws Exception {
                        logger.info("[Window] key, {}, timeWindow: [{}, {}], watermark: {}, elements: {}",
                                s, f(context.window().getStart()), f(context.window().getEnd()), f(context.currentWatermark()), elements);
                        elements.forEach(out::collect);
                    }
                });

        env.execute();
    }
/**
 * the results are like:
 *
 * 2020-12-06 15:12:28.992 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:01, watermark: 2020-12-06 14:26:59
 * 2020-12-06 15:12:30.994 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:02, watermark: 2020-12-06 14:27:00
 * 2020-12-06 15:12:32.995 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:11, watermark: 2020-12-06 14:27:09
 * 2020-12-06 15:12:34.995 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:11, watermark: 2020-12-06 14:27:09
 * 2020-12-06 15:12:36.996 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:11, watermark: 2020-12-06 14:27:09
 * 2020-12-06 15:12:38.996 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:08, watermark: 2020-12-06 14:27:06
 * 2020-12-06 15:12:40.997 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:21, watermark: 2020-12-06 14:27:19
 * 2020-12-06 15:12:41.191 - INFO PID_IS_UNDEFINED [nction$1) (3/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_1, timeWindow: [2020-12-06 14:27:00, 2020-12-06 14:27:10], watermark: 2020-12-06 14:27:19, elements: [Message{timestamp=2020-12-06 14:27:02, partitionKey=_partition_1}, Message{timestamp=2020-12-06 14:27:08, partitionKey=_partition_1}]
 * 2020-12-06 15:12:41.191 - INFO PID_IS_UNDEFINED [nction$1) (6/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_2, timeWindow: [2020-12-06 14:27:00, 2020-12-06 14:27:10], watermark: 2020-12-06 14:27:19, elements: [Message{timestamp=2020-12-06 14:27:01, partitionKey=_partition_2}]
 * 2020-12-06 15:12:42.998 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:18, watermark: 2020-12-06 14:27:16
 * 2020-12-06 15:12:44.998 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:28, watermark: 2020-12-06 14:27:26
 * 2020-12-06 15:12:45.117 - INFO PID_IS_UNDEFINED [nction$1) (6/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_2, timeWindow: [2020-12-06 14:27:10, 2020-12-06 14:27:20], watermark: 2020-12-06 14:27:26, elements: [Message{timestamp=2020-12-06 14:27:11, partitionKey=_partition_2}, Message{timestamp=2020-12-06 14:27:18, partitionKey=_partition_2}]
 * 2020-12-06 15:12:45.117 - INFO PID_IS_UNDEFINED [nction$1) (5/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_3, timeWindow: [2020-12-06 14:27:10, 2020-12-06 14:27:20], watermark: 2020-12-06 14:27:26, elements: [Message{timestamp=2020-12-06 14:27:11, partitionKey=_partition_3}]
 * 2020-12-06 15:12:45.117 - INFO PID_IS_UNDEFINED [nction$1) (3/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_1, timeWindow: [2020-12-06 14:27:10, 2020-12-06 14:27:20], watermark: 2020-12-06 14:27:26, elements: [Message{timestamp=2020-12-06 14:27:11, partitionKey=_partition_1}]
 * 2020-12-06 15:12:46.998 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:38, watermark: 2020-12-06 14:27:36
 * 2020-12-06 15:12:47.130 - INFO PID_IS_UNDEFINED [nction$1) (6/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_2, timeWindow: [2020-12-06 14:27:20, 2020-12-06 14:27:30], watermark: 2020-12-06 14:27:36, elements: [Message{timestamp=2020-12-06 14:27:21, partitionKey=_partition_2}, Message{timestamp=2020-12-06 14:27:28, partitionKey=_partition_2}]
 * 2020-12-06 15:12:48.998 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:48, watermark: 2020-12-06 14:27:46
 * 2020-12-06 15:12:49.143 - INFO PID_IS_UNDEFINED [nction$1) (3/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_1, timeWindow: [2020-12-06 14:27:30, 2020-12-06 14:27:40], watermark: 2020-12-06 14:27:46, elements: [Message{timestamp=2020-12-06 14:27:38, partitionKey=_partition_1}]
 * 2020-12-06 15:12:50.998 - INFO PID_IS_UNDEFINED [om Source (1/1)] org.example.flink.WatermarkDemo          : [Source] timestamp: 2020-12-06 14:27:58, watermark: 2020-12-06 14:27:56
 * 2020-12-06 15:12:51.159 - INFO PID_IS_UNDEFINED [nction$1) (6/8)] org.example.flink.WatermarkDemo          : [Window] key, _partition_2, timeWindow: [2020-12-06 14:27:40, 2020-12-06 14:27:50], watermark: 2020-12-06 14:27:56, elements: [Message{timestamp=2020-12-06 14:27:48, partitionKey=_partition_2}]
 *
 * NOTE: only when watermark exceeds the end time of the window, the window will be triggered
 *
 *
 */



}
