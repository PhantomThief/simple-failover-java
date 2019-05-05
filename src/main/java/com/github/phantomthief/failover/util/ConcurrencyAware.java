package com.github.phantomthief.failover.util;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 * Created on 2018-01-22.
 */
public class ConcurrencyAware<T> {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrencyAware.class);

    private static final int OPTIMIZE_RANDOM_TRIES = 2;

    private final Map<T, Integer> concurrency = new ConcurrentHashMap<>();
    private final List<ThrowableConsumer<T, Throwable>> illegalStateHandlers = new ArrayList<>();

    private ConcurrencyAware() {
    }

    public static <T> ConcurrencyAware<T> create() {
        return new ConcurrencyAware<>();
    }

    @Nullable
    private T selectIdlest(@Nonnull Iterable<T> candidates) {
        checkNotNull(candidates);
        if (candidates instanceof List) {
            List<T> candidatesCol = (List<T>) candidates;
            T t = selectIdlestFast(candidatesCol);
            if (t != null) {
                return t;
            }
        }

        // find objects with minimum concurrency
        List<T> idlest = new ArrayList<>();
        int minValue = Integer.MAX_VALUE;
        for (T obj : candidates) {
            int c = concurrency.getOrDefault(obj, 0);
            if (c < minValue) {
                minValue = c;
                idlest.clear();
                idlest.add(obj);
            } else if (c == minValue) {
                idlest.add(obj);
            }
        }
        T result = getRandom(idlest);
        assert result != null;
        return result;
    }

    /**
     * Try to find a node with 0 concurrency by pure random.
     * It is assuming that concurrency is very low for most cases.
     * This method will optimize performance significantly on large candidates collection,
     * because it is no need to build a large ListMultimap of TreeMap.
     */
    @Nullable
    private T selectIdlestFast(List<T> candidates) {
        if (candidates.isEmpty()) {
            throw new NoSuchElementException("candidates list is empty");
        } else if (candidates.size() == 1) {
            return candidates.get(0);
        }
        for (int i = 0; i < OPTIMIZE_RANDOM_TRIES; ++i) {
            T result = getRandom(candidates);
            int objConcurrency = concurrency.getOrDefault(result, 0);
            if (objConcurrency <= 0) {
                return result;
            }
        }
        return null;
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
    @Nonnull
    public T begin(@Nonnull Iterable<T> candidates) {
        T obj = beginWithoutRecordConcurrency(candidates);
        recordBeginConcurrency(obj);
        return obj;
    }

    /**
     * this is a low level api, for special purpose or mock.
     */
    @Nonnull
    public T beginWithoutRecordConcurrency(@Nonnull Iterable<T> candidates) {
        T obj = selectIdlest(candidates);
        if (obj == null) {
            throw new NoSuchElementException();
        }
        return obj;
    }

    /**
     * this is a low level api, for special purpose or mock.
     */
    public void recordBeginConcurrency(@Nonnull T obj) {
        concurrency.merge(obj, 1, Integer::sum);
    }

    /**
     * @param obj from {@link #begin}'s return
     * @see #begin
     */
    public void end(@Nonnull T obj) {
        concurrency.compute(obj, (thisKey, oldValue) -> {
            if (oldValue == null) {
                logger.warn("illegal state found, obj:{}", thisKey);
                for (ThrowableConsumer<T, Throwable> handler : illegalStateHandlers) {
                    try {
                        handler.accept(thisKey);
                    } catch (Throwable e) {
                        logger.error("", e);
                    }
                }
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

    public ConcurrencyAware<T>
            addIllegalStateHandler(@Nonnull ThrowableConsumer<T, Throwable> handler) {
        illegalStateHandlers.add(checkNotNull(handler));
        return this;
    }
}
