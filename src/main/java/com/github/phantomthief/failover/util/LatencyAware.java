/**
 * 
 */
package com.github.phantomthief.failover.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.EvictingQueue;

/**
 * @author w.vela
 */
public class LatencyAware<T> {

    private static final long DEFAULT_INIT_LATENCY = 1;
    private static final int DEFAULT_EVALUTION_COUNT = 10;

    private final long initLatency;
    private final int evaluationCount;
    private final LoadingCache<T, EvictingQueue<Long>> latencies = CacheBuilder.newBuilder() //
            .weakKeys() //
            .build(new CacheLoader<T, EvictingQueue<Long>>() {

                @Override
                public EvictingQueue<Long> load(T key) throws Exception {
                    return EvictingQueue.create(evaluationCount);
                }
            });

    private LatencyAware(long initLatency, int evaluationCount) {
        this.initLatency = initLatency;
        this.evaluationCount = evaluationCount;
    }

    public T get(Collection<T> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        AtomicLong sum = new AtomicLong();
        Map<T, Long> weightMap = candidates.stream().collect(toMap(identity(), t -> {
            EvictingQueue<Long> queue = latencies.getUnchecked(t);
            long result;
            if (queue == null) {
                result = initLatency;
            } else {
                synchronized (queue) {
                    result = queue.stream().mapToLong(Long::longValue).sum();
                    if (result == 0) {
                        result = initLatency;
                    }
                }
            }
            sum.addAndGet(result);
            return result;
        }));
        Weight<T> weight = new Weight<>();
        weightMap.forEach((k, w) -> weight.add(k, sum.get() - w));
        return weight.get();
    }

    public void cost(T obj, long cost) {
        EvictingQueue<Long> queue = latencies.getUnchecked(obj);
        synchronized (queue) {
            queue.add(cost);
        }
    }

    public void run(Collection<T> candidates, Consumer<T> function) {
        run(candidates, t -> {
            function.accept(t);
            return null;
        });
    }

    public <V> V run(Collection<T> candidates, Function<T, V> function) {
        T obj = get(candidates);
        if (obj == null) {
            throw new RuntimeException("no available resources.");
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            return function.apply(obj);
        } finally {
            stopwatch.stop();
            cost(obj, stopwatch.elapsed(MILLISECONDS));
        }
    }

    public static final <T> LatencyAware<T> create() {
        return new LatencyAware<>(DEFAULT_INIT_LATENCY, DEFAULT_EVALUTION_COUNT);
    }
}
