package com.github.phantomthief.failover.util;

import static java.lang.String.format;
import static java.lang.Thread.MIN_PRIORITY;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author w.vela
 */
public class SharedCheckExecutorHolder {

    private static final int THREAD_COUNT = 10;

    public static ScheduledExecutorService getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {

        private static final ScheduledExecutorService INSTANCE = new ScheduledThreadPoolExecutor(
                THREAD_COUNT,
                new ThreadFactory() {
                    private AtomicLong count = new AtomicLong();
                    private static final String NAME_PATTERN = "scheduled-failover-recovery-check-%d";
                    @Override
                    public Thread newThread(Runnable r) {
                        String name = format(NAME_PATTERN, count.getAndIncrement());
                        Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        thread.setPriority(MIN_PRIORITY);
                        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
                            thread.setUncaughtExceptionHandler((t, e) -> {
                                e.printStackTrace();
                            });
                        }
                        return thread;
                    }
                }) {

            public void shutdown() {
                throw new UnsupportedOperationException();
            }

            public List<Runnable> shutdownNow() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
