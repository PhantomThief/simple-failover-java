package com.github.phantomthief.failover.util;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 */
class LatencyAwareTest {

    private ConcurrentMap<Integer, AtomicInteger> callCount = new ConcurrentHashMap<>();

    @Test
    void test() {
        List<Integer> candidates = Lists.newArrayList(1, 2, 3, 10);
        LatencyAware<Integer> latencyAware = LatencyAware.create();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10000; i++) {
            executorService.execute(() -> latencyAware.run(candidates, j -> {
                sleepUninterruptibly(j, MILLISECONDS);
                callCount.computeIfAbsent(j, o -> new AtomicInteger()).incrementAndGet();
            }));
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, DAYS);
        System.out.println(callCount);
    }

}
