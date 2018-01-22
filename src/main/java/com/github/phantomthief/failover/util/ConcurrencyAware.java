package com.github.phantomthief.failover.util;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Multimaps.asMap;
import static com.google.common.collect.Multimaps.newListMultimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.failover.exception.NoAvailableResourceException;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;
import com.google.common.collect.ListMultimap;

/**
 * @author w.vela
 * Created on 2018-01-22.
 */
public class ConcurrencyAware<T> {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrencyAware.class);
    private final Map<T, Integer> concurrency = new ConcurrentHashMap<>();

    private final BiFunction<T, Integer, Integer> concurrencyEvaluator;

    private ConcurrencyAware(@Nonnull BiFunction<T, Integer, Integer> concurrencyEvaluator) {
        this.concurrencyEvaluator = checkNotNull(concurrencyEvaluator);
    }

    public static <T> ConcurrencyAware<T>
            create(@Nonnull BiFunction<T, Integer, Integer> concurrencyEvaluator) {
        return new ConcurrencyAware<>(concurrencyEvaluator);
    }

    public static <T> ConcurrencyAware<T> create() {
        return create((b, i) -> i);
    }

    @Nonnull
    private T selectIdlest(Collection<T> candidates) {
        ListMultimap<Integer, T> map = newListMultimap(new TreeMap<>(), ArrayList::new);
        candidates.forEach(obj -> {
            int c = concurrency.getOrDefault(obj, 0);
            map.put(concurrencyEvaluator.apply(obj, c), obj);
        });
        NavigableMap<Integer, List<T>> asMap = (NavigableMap<Integer, List<T>>) asMap(map);
        T result = getRandom(asMap.firstEntry().getValue());
        assert result != null;
        return result;
    }

    /**
     * @throws X, or {@link NoAvailableResourceException} if candidates is empty
     */
    public <X extends Throwable> void run(Collection<T> candidates, ThrowableConsumer<T, X> func)
            throws X {
        supply(candidates, it -> {
            func.accept(it);
            return null;
        });
    }

    /**
     * @throws X, or {@link NoAvailableResourceException} if candidates is empty
     */
    public <E, X extends Throwable> E supply(Collection<T> candidates,
            ThrowableFunction<T, E, X> func) throws X {
        T obj = begin(candidates);
        try {
            return func.apply(obj);
        } finally {
            end(obj);
        }
    }

    /**
     * better use {@link #supply} or {@link #run} unless need to control begin and end in special situations.
     */
    public T begin(Collection<T> candidates) {
        if (candidates.isEmpty()) {
            throw new NoAvailableResourceException();
        }
        T obj = selectIdlest(candidates);
        concurrency.merge(obj, 1, Integer::sum);
        return obj;
    }

    /**
     * see {@link #begin}
     */
    public void end(@Nonnull T obj) {
        concurrency.compute(obj, (oldKey, oldValue) -> {
            if (oldValue == null) {
                logger.warn("illegal state found for concurrency aware:{}, old value is null.",
                        this);
                return null;
            }
            int result = oldValue - 1;
            if (result == 0) {
                return null;
            } else {
                return result;
            }
        });
    }
}
