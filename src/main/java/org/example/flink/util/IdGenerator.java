package org.example.flink.util;

import java.util.concurrent.atomic.AtomicInteger;

public class IdGenerator {


    public static final AtomicInteger COUNT = new AtomicInteger(0);

    public static String next() {
        return String.valueOf(COUNT.incrementAndGet());
    }


}
