package org.example.flink.function;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.flink.model.Gender;
import org.example.flink.model.Person;
import org.example.flink.util.ConcurrentUtils;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PersonEmiter implements SourceFunction<Person> {

    private boolean cancel = false;


    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Person(UUID.randomUUID().toString(),
                        new Random().nextInt(100) + 1,
                        Gender.values()[new Random().nextInt(Gender.values().length)]));
            }
            ConcurrentUtils.sleep(TimeUnit.SECONDS, 1);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}