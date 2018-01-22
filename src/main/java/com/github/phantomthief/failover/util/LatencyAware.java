package com.github.phantomthief.failover.util;

import static com.github.phantomthief.stats.n.counter.SimpleCounter.stats;
import static com.google.common.base.Stopwatch.createStarted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.github.phantomthief.stats.n.DurationStats;
import com.github.phantomthief.stats.n.counter.SimpleCounter;
import com.github.phantomthief.stats.n.impl.SimpleDurationStats;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * use {@link ConcurrencyAware} instead
 *
 * @author w.vela
 */
@Deprecated
public class LatencyAware<T> {

    private static final long DEFAULT_INIT_LATENCY = 1;
    private static final long DEFAULT_EVALUATION_DURATION = SECONDS.toMillis(10);
    private static Logger logger = getLogger(LatencyAware.class);
    private final long initLatency;
    private final long evaluationDuration;
    private final boolean aggressiveMode;
    private final LoadingCache<T, DurationStats<SimpleCounter>> costMap = CacheBuilder.newBuilder() //
            .weakKeys() //
            .<T, DurationStats<SimpleCounter>> removalListener(notify -> {
                try {
                    notify.getValue().close();
                } catch (Exception e) {
                    logger.error("Ops.", e);
                }
            }) //
            .build(new CacheLoader<T, DurationStats<SimpleCounter>>() {

                @Override
                public DurationStats<SimpleCounter> load(T key) throws Exception {
                    return SimpleDurationStats.newBuilder() //
                            .addDuration(evaluationDuration, MILLISECONDS) //
                            .build();
                }
            });

    private LatencyAware(long initLatency, long evaluationDuration, boolean aggressiveMode) {
        this.initLatency = initLatency;
        this.evaluationDuration = evaluationDuration;
        this.aggressiveMode = aggressiveMode;
    }

    @SuppressWarnings("unchecked")
    public static <T> LatencyAware<T> shared() {
        return (LatencyAware<T>) LazyHolder.INSTANCE;
    }

    public static <T> LatencyAware<T> create() {
        return new LatencyAware<>(DEFAULT_INIT_LATENCY, DEFAULT_EVALUATION_DURATION, false);
    }

    public static <T> LatencyAware<T> withEvaluationDuration(long durationInMs) {
        return new LatencyAware<>(DEFAULT_INIT_LATENCY, durationInMs, false);
    }

    public static <T> LatencyAware<T> withAggressiveMode(long durationInMs) {
        return new LatencyAware<>(DEFAULT_INIT_LATENCY, durationInMs, true);
    }

    @Nullable
    public T get(Collection<T> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            return candidates.iterator().next();
        }
        if (aggressiveMode) {
            return getAggressiveMode(candidates);
        } else {
            return getNormalMode(candidates);
        }
    }

    private T getAggressiveMode(Collection<T> candidates) {
        AtomicLong sum = new AtomicLong();
        Map<T, Long> weightMap = candidates.stream().collect(toMap(identity(), t -> {
            DurationStats<SimpleCounter> stats = costMap.getUnchecked(t);
            SimpleCounter counter = stats.getStats().get(evaluationDuration);
            long result = counter == null ? initLatency : (long) ((double) counter.getCost()
                    / counter.getCount());
            if (result == 0) {
                result = initLatency;
            }
            sum.addAndGet(result);
            return result;
        }));
        Weight<T> weight = new Weight<>();
        weightMap.forEach((k, w) -> weight.add(k, sum.get() / w));
        return weight.get();
    }

    private T getNormalMode(Collection<T> candidates) {
        AtomicLong sum = new AtomicLong();
        Map<T, Long> weightMap = candidates.stream().collect(toMap(identity(), t -> {
            DurationStats<SimpleCounter> stats = costMap.getUnchecked(t);
            SimpleCounter counter = stats.getStats().get(evaluationDuration);
            long result = counter == null ? initLatency : (long) ((double) counter.getCost()
                    / counter.getCount());
            if (result == 0) {
                result = initLatency;
            }
            sum.addAndGet(result);
            return result;
        }));
        Weight<T> weight = new Weight<>();
        weightMap.forEach((k, w) -> weight.add(k, sum.get() - w));
        return weight.get();
    }

    public void cost(T obj, long cost) {
        if (cost < 0) {
            return;
        }
        costMap.getUnchecked(obj).stat(stats(cost));
    }

    public <X extends Throwable> void run(Collection<T> candidates,
            ThrowableConsumer<T, X> function) throws X {
        supply(candidates, t -> {
            function.accept(t);
            return null;
        });
    }

    public <V, X extends Throwable> V supply(Collection<T> candidates,
            ThrowableFunction<T, V, X> function) throws X {
        T obj = get(candidates);
        if (obj == null) {
            throw new RuntimeException("no available resources.");
        }
        Stopwatch stopwatch = createStarted();
        try {
            return function.apply(obj);
        } finally {
            stopwatch.stop();
            cost(obj, stopwatch.elapsed(MICROSECONDS));
        }
    }

    private static class LazyHolder {

        private static final LatencyAware<Object> INSTANCE = new LatencyAware<>(
                DEFAULT_INIT_LATENCY, DEFAULT_EVALUATION_DURATION, false);
    }
}
