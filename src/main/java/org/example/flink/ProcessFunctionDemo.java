package org.example.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.flink.function.PersonEmiter;
import org.example.flink.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessFunctionDemo {

    private static Logger logger = LoggerFactory.getLogger(ProcessFunctionDemo.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> personStream = env.addSource(new PersonEmiter());

        SingleOutputStreamOperator<String> processedStream = personStream
                .keyBy(person -> person.gender)
                .process(new KeyedProcessFunction<String, Person, String>() {

                    private ValueState<Integer> minAge;

                    private ValueState<Integer> maxAge;

                    // a field preserve timer's timestamp
                    private ValueState<Long> currentTimer;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        minAge = getRuntimeContext().getState(new ValueStateDescriptor<>("minAge", Integer.class));
                        maxAge = getRuntimeContext().getState(new ValueStateDescriptor<>("maxAge", Integer.class));
                        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
                    }

                    @Override
                    public void processElement(Person curPerson, Context context, Collector<String> collector) throws Exception {
                        // set initial value for state
                        if (minAge.value() == null) {
                            minAge.update(curPerson.age);
                        }
                        if (maxAge.value() == null) {
                            maxAge.update(curPerson.age);
                        }

                        if (currentTimer.value() == null) {
                            currentTimer.update(0L);
                        }


                        minAge.update(Math.min(minAge.value(), curPerson.age));
                        maxAge.update(Math.max(maxAge.value(), curPerson.age));


                        long curTimerMs = currentTimer.value();
                        // if age increase and timer not found, register one
                        if (curTimerMs == 0) {
                            long timerTs = context.timerService().currentProcessingTime() + 10000L;
                            logger.info("register timer: {}", timerTs);
                            // the timer is an absolute time millis, a certain moment
                            // only one timer is available, if register new timer, older one will be replaced
                            context.timerService().registerProcessingTimeTimer(timerTs);
                            // context.timerService().registerEventTimeTimer(timerTs);
                            currentTimer.update(timerTs);
                        }
                        // delete timer
                        // context.timerService().deleteProcessingTimeTimer(curTimerMs);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("registeredTimer: "+currentTimer.value() + ", onTimer: " + timestamp +
                                " Current key: " + ctx.getCurrentKey() + ", minAge: " + minAge.value() + ", maxAge: " + maxAge.value());
                        ctx.timerService().registerProcessingTimeTimer(timestamp + 10000L);
                    }
                });


        processedStream.print();

        env.execute();
    }
}
