package org.example.flink.util;

import java.util.concurrent.TimeUnit;

public class ConcurrentUtils {

    public static void sleep(TimeUnit timeUnit, long time) {
        try {
            timeUnit.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
