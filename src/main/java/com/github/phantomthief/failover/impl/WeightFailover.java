/**
 * 
 */
package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.lang3.RandomUtils;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;

/**
 * 默认权重记录
 * fail时权重下降
 * success时权重恢复
 * 
 * @author w.vela
 */
public class WeightFailover<T> implements Failover<T>, Closeable {

    private static org.slf4j.Logger logger = getLogger(WeightFailover.class);

    private final int failReduceWeight;
    private final int successIncreaceWeight;

    private final ConcurrentMap<T, Integer> initWeightMap;
    private final ConcurrentMap<T, Integer> currentWeightMap;
    private final ScheduledFuture<?> recoveryFuture;

    private WeightFailover(int failReduceWeight, int successIncreaceWeight,
            int recoveriedInitWeight, Map<T, Integer> initWeightMap, long failCheckDuration,
            Predicate<T> checker) {
        this.failReduceWeight = failReduceWeight;
        this.successIncreaceWeight = successIncreaceWeight;
        this.initWeightMap = new ConcurrentHashMap<>(initWeightMap);
        this.currentWeightMap = new ConcurrentHashMap<>(initWeightMap);
        this.recoveryFuture = SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(() -> {
            Set<T> recoveriedObjects = this.currentWeightMap.entrySet().stream() //
                    .filter(entry -> entry.getValue() == 0) //
                    .map(Entry::getKey) //
                    .filter(checker::test) //
                    .collect(toSet());
            recoveriedObjects
                    .forEach(recoveried -> currentWeightMap.put(recoveried, recoveriedInitWeight));
        }, failCheckDuration, failCheckDuration, TimeUnit.MILLISECONDS);
    }

    /* (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    @Override
    public synchronized void close() {
        if (!recoveryFuture.isCancelled()) {
            if (!recoveryFuture.cancel(true)) {
                logger.warn("fail to close failover:{}", this);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.failover.Failover#getAll()
     */
    @Override
    public List<T> getAll() {
        return initWeightMap.keySet().stream().collect(toList());
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.failover.Failover#fail(java.lang.Object)
     */
    @Override
    public void fail(T object) {
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                logger.warn("invalid fail obj:{}, it's not in original list.", object);
                return null;
            }
            return Math.max(0, oldValue - failReduceWeight);
        });
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.failover.Failover#getAvailable()
     */
    @Override
    public List<T> getAvailable() {
        return getAvailable(Integer.MAX_VALUE);
    }

    @Override
    public T getOneAvailable() {
        List<T> available = getAvailable(1);
        return available.isEmpty() ? null : available.get(0);
    }

    @Override
    public List<T> getAvailable(int n) {
        Map<T, Integer> snapshot = new HashMap<>(currentWeightMap);
        List<T> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            if (snapshot.isEmpty()) {
                break;
            }
            int sum = snapshot.values().stream().mapToInt(Integer::intValue).sum();
            int left = RandomUtils.nextInt(0, sum);
            Iterator<Entry<T, Integer>> iterator = snapshot.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<T, Integer> candidate = iterator.next();
                if (left < candidate.getValue()) {
                    result.add(candidate.getKey());
                    iterator.remove();
                    break;
                }
                left -= candidate.getValue();
            }
        }
        return result;
    }

    @Override
    public void success(T object) {
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                logger.warn("invalid fail obj:{}, it's not in original list.", object);
                return null;
            }
            return Math.min(initWeightMap.get(k), oldValue + successIncreaceWeight);
        });
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.failover.Failover#getFailed()
     */
    @Override
    public Set<T> getFailed() {
        return currentWeightMap.entrySet().stream() //
                .filter(entry -> entry.getValue() == 0) //
                .collect(mapping(Entry::getKey, toSet()));
    }

    public static Builder<Object> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {

        private static final int DEFAULT_INIT_WEIGHT = 100;
        private static final int DEFAULT_FAIL_REDUCE_WEIGHT = 5;
        private static final int DEFAULT_SUCCESS_INCREASE_WEIGHT = 1;
        private static final int DEFAULT_RECOVERIED_INIT_WEIGHT = 1;
        private static final long DEFAULT_CHECK_DURATION = SECONDS.toMillis(5);

        private int failReduceWeight;
        private int successIncreaceWeight;
        private int recoveriedInitWeight;
        private Map<T, Integer> initWeightMap;
        private Predicate<T> checker;
        private long checkDuration;

        public Builder<T> failReduce(int weight) {
            checkArgument(weight > 0);
            failReduceWeight = weight;
            return this;
        }

        public Builder<T> successIncrease(int weight) {
            checkArgument(weight > 0);
            successIncreaceWeight = weight;
            return this;
        }

        public Builder<T> recoveiedInit(int weight) {
            checkArgument(weight > 0);
            recoveriedInitWeight = weight;
            return this;
        }

        public Builder<T> checkDuration(long time, TimeUnit unit) {
            checkNotNull(unit);
            checkArgument(time > 0);
            checkDuration = unit.toMillis(time);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <E> Builder<E> checker(Predicate<? super E> failChecker) {
            checkNotNull(failChecker);
            Builder<E> thisBuilder = (Builder<E>) this;
            thisBuilder.checker = t -> {
                try {
                    return failChecker.test(t);
                } catch (Throwable e) {
                    logger.error("Ops.", e);
                    return false;
                }
            };
            return thisBuilder;
        }

        public <E> WeightFailover<E> build(Collection<? extends E> original) {
            return build(original, DEFAULT_INIT_WEIGHT);
        }

        public <E> WeightFailover<E> build(Collection<? extends E> original, int initWeight) {
            checkNotNull(original);
            checkArgument(initWeight > 0);
            return build(original.stream().collect(toMap(identity(), i -> initWeight)));
        }

        @SuppressWarnings("unchecked")
        public <E> WeightFailover<E> build(Map<? extends E, Integer> original) {
            checkNotNull(original);
            Builder<E> thisBuilder = (Builder<E>) this;
            thisBuilder.initWeightMap = (Map<E, Integer>) original;
            return thisBuilder.build();
        }

        private WeightFailover<T> build() {
            ensure();
            return new WeightFailover<>(failReduceWeight, successIncreaceWeight,
                    recoveriedInitWeight, initWeightMap, checkDuration, checker);
        }

        private void ensure() {
            checkNotNull(checker);
            if (failReduceWeight == 0) {
                failReduceWeight = DEFAULT_FAIL_REDUCE_WEIGHT;
            }
            if (successIncreaceWeight == 0) {
                successIncreaceWeight = DEFAULT_SUCCESS_INCREASE_WEIGHT;
            }
            if (recoveriedInitWeight == 0) {
                recoveriedInitWeight = DEFAULT_RECOVERIED_INIT_WEIGHT;
            }
            if (checkDuration == 0) {
                checkDuration = DEFAULT_CHECK_DURATION;
            }
        }
    }

    @Override
    public String toString() {
        return "WeightFailover [" + initWeightMap + "]";
    }
}
