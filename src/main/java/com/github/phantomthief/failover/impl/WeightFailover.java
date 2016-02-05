/**
 * 
 */
package com.github.phantomthief.failover.impl;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;

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

    static org.slf4j.Logger logger = getLogger(WeightFailover.class);

    private final int failReduceWeight;
    private final int successIncreaceWeight;

    private final ConcurrentMap<T, Integer> initWeightMap;
    private final ConcurrentMap<T, Integer> currentWeightMap;
    private final ScheduledFuture<?> recoveryFuture;

    WeightFailover(int failReduceWeight, int successIncreaceWeight, int recoveriedInitWeight,
            Map<T, Integer> initWeightMap, long failCheckDuration, Predicate<T> checker) {
        this.failReduceWeight = failReduceWeight;
        this.successIncreaceWeight = successIncreaceWeight;
        this.initWeightMap = new ConcurrentHashMap<>(initWeightMap);
        this.currentWeightMap = new ConcurrentHashMap<>(initWeightMap);
        this.recoveryFuture = SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(
                () -> {
                    Set<T> recoveriedObjects = this.currentWeightMap.entrySet().stream() //
                            .filter(entry -> entry.getValue() == 0) //
                            .map(Entry::getKey) //
                            .filter(checker) //
                            .collect(toSet());
                    recoveriedObjects.forEach(recoveried -> currentWeightMap.put(recoveried,
                            recoveriedInitWeight));
                }, failCheckDuration, failCheckDuration, MILLISECONDS);
    }

    public static WeightFailoverBuilder<Object> newBuilder() {
        return new WeightFailoverBuilder<>();
    }

    public static <E> GenericWeightFailoverBuilder<E> newGenericBuilder() {
        return new GenericWeightFailoverBuilder<>(newBuilder());
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

    @Override
    public List<T> getAll() {
        return initWeightMap.keySet().stream().collect(toList());
    }

    @Override
    public void fail(T object) {
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                logger.warn("invalid fail obj:{}, it's not in original list.", object);
                return null;
            }
            return max(0, oldValue - failReduceWeight);
        });
    }

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
            int left = nextInt(0, sum);
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
            return min(initWeightMap.get(k), oldValue + successIncreaceWeight);
        });
    }

    @Override
    public Set<T> getFailed() {
        return currentWeightMap.entrySet().stream() //
                .filter(entry -> entry.getValue() == 0) //
                .collect(mapping(Entry::getKey, toSet()));
    }

    @Override
    public String toString() {
        return "WeightFailover [" + initWeightMap + "]";
    }
}
