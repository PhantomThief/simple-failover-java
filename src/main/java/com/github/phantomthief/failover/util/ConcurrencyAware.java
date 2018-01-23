package com.github.phantomthief.failover.util;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Multimaps.asMap;
import static com.google.common.collect.Multimaps.newListMultimap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Nullable
    private T selectIdlest(@Nonnull Iterable<T> candidates) {
        checkNotNull(candidates);
        ListMultimap<Integer, T> map = null;
        for (T obj : candidates) {
            int c = concurrency.getOrDefault(obj, 0);
            if (map == null) {
                map = newListMultimap(new TreeMap<>(), ArrayList::new);
            }
            map.put(concurrencyEvaluator.apply(obj, c), obj);
        }
        if (map == null) {
            return null;
        }
        NavigableMap<Integer, List<T>> asMap = (NavigableMap<Integer, List<T>>) asMap(map);
        T result = getRandom(asMap.firstEntry().getValue());
        assert result != null;
        return result;
    }

    /**
     * @throws X, or {@link NoSuchElementException} if candidates is empty
     */
    public <X extends Throwable> void run(@Nonnull Iterable<T> candidates,
            @Nonnull ThrowableConsumer<T, X> func) throws X {
        checkNotNull(func);
        supply(candidates, it -> {
            func.accept(it);
            return null;
        });
    }

    /**
     * @throws X, or {@link NoSuchElementException} if candidates is empty
     */
    public <E, X extends Throwable> E supply(@Nonnull Iterable<T> candidates,
            @Nonnull ThrowableFunction<T, E, X> func) throws X {
        checkNotNull(func);
        T obj = begin(candidates);
        try {
            return func.apply(obj);
        } finally {
            end(obj);
        }
    }

    /**
     * better use {@link #supply} or {@link #run} unless need to control begin and end in special situations.
     * @throws NoSuchElementException if candidates is empty
     */
    public T begin(@Nonnull Iterable<T> candidates) {
        T obj = selectIdlest(candidates);
        if (obj == null) {
            throw new NoSuchElementException();
        }
        concurrency.merge(obj, 1, Integer::sum);
        return obj;
    }

    /**
     * @param obj from {@link #begin}'s return
     * @see #begin
     */
    public void end(@Nonnull T obj) {
        concurrency.compute(obj, (thisKey, oldValue) -> {
            if (oldValue == null) {
                logger.warn("illegal state found, obj:{}", thisKey);
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
