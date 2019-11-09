package com.github.phantomthief.failover.util;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.Thread.MIN_PRIORITY;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class SharedCheckExecutorHolder {

    private static final int THREAD_COUNT = 10;

    public static ListeningScheduledExecutorService getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {

        private static final ListeningScheduledExecutorService INSTANCE =
                listeningDecorator(new ScheduledThreadPoolExecutor(THREAD_COUNT,
                        new ThreadFactoryBuilder()
                                .setNameFormat("scheduled-failover-recovery-check-%d")
                                .setPriority(MIN_PRIORITY)
                                .setDaemon(true)
                                .build()) {

                    @Override
                    public void shutdown() {
                        throw new UnsupportedOperationException();
                    }

                    @Nonnull
                    @Override
                    public List<Runnable> shutdownNow() {
                        throw new UnsupportedOperationException();
                    }
                });

    }

}