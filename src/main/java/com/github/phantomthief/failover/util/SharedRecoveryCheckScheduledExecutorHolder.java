/**
 * 
 */
package com.github.phantomthief.failover.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class SharedRecoveryCheckScheduledExecutorHolder {

    private static final int THREAD_COUNT = 10;

    private static class LazyHolder {

        private static final ScheduledExecutorService INSTANCE = Executors.newScheduledThreadPool(
                THREAD_COUNT,
                new ThreadFactoryBuilder() //
                        .setNameFormat("scheduled-failover-recovery-check-%d") //
                        .setPriority(Thread.MIN_PRIORITY) //
                        .setDaemon(true) //
                        .build());
    }

    private SharedRecoveryCheckScheduledExecutorHolder() {
    }

    public static ScheduledExecutorService getInstance() {
        return LazyHolder.INSTANCE;
    }

}
