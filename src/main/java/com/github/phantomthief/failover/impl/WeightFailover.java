/**
 *
 */
package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

import org.slf4j.Logger;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;

/**
 * 默认权重记录
 * fail时权重下降
 * success时权重恢复
 *
 * @author w.vela
 */
public class WeightFailover<T> implements Failover<T>, Closeable {

    private static final Logger logger = getLogger(WeightFailover.class);

    private final IntUnaryOperator failReduceWeight;
    private final IntUnaryOperator successIncreaseWeight;

    private final ConcurrentMap<T, Integer> initWeightMap;
    private final ConcurrentMap<T, Integer> currentWeightMap;
    private final CloseableSupplier<ScheduledFuture<?>> recoveryFuture;
    private final Consumer<T> onMinWeight;
    private final int minWeight;

    WeightFailover(IntUnaryOperator failReduceWeight, IntUnaryOperator successIncreaseWeight,
            IntUnaryOperator recoveredInitWeight, Map<T, Integer> initWeightMap, int minWeight,
            long failCheckDuration, Predicate<T> checker, Consumer<T> onMinWeight,
            Consumer<T> onRecovered) {
        this.minWeight = minWeight;
        this.failReduceWeight = failReduceWeight;
        this.successIncreaseWeight = successIncreaseWeight;
        this.initWeightMap = new ConcurrentHashMap<>(initWeightMap);
        this.currentWeightMap = new ConcurrentHashMap<>(initWeightMap);
        this.onMinWeight = onMinWeight;
        this.recoveryFuture = lazy(
                () -> SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(() -> {
                    try {
                        Set<T> recoveredObjects = this.currentWeightMap.entrySet().stream() //
                                .filter(entry -> entry.getValue() == 0) //
                                .map(Entry::getKey) //
                                .filter(checker) //
                                .collect(toSet());
                        if (!recoveredObjects.isEmpty()) {
                            logger.info("found recovered objects:{}", recoveredObjects);
                        }
                        recoveredObjects.forEach(recovered -> {
                            currentWeightMap.put(recovered,
                                    recoveredInitWeight.applyAsInt(initWeightMap.get(recovered)));
                            if (onRecovered != null) {
                                onRecovered.accept(recovered);
                            }
                        });
                    } catch (Throwable e) {
                        logger.error("", e);
                    }
                }, failCheckDuration, failCheckDuration, MILLISECONDS));
    }

    /**
     * better use {@link #newGenericBuilder()} for type safe
     */
    @Deprecated
    public static WeightFailoverBuilder<Object> newBuilder() {
        return new WeightFailoverBuilder<>();
    }

    public static <E> GenericWeightFailoverBuilder<E> newGenericBuilder() {
        return new GenericWeightFailoverBuilder<>(newBuilder());
    }

    @Override
    public synchronized void close() {
        recoveryFuture.ifPresent(future -> {
            if (!future.isCancelled()) {
                if (!future.cancel(true)) {
                    logger.warn("fail to close failover:{}", this);
                }
            }
        });
    }

    @Override
    public List<T> getAll() {
        return new ArrayList<>(initWeightMap.keySet());
    }

    @Override
    public void fail(T object) {
        if (object == null) {
            logger.warn("invalid fail call, null object found.");
            return;
        }
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                logger.warn("invalid fail obj:{}, it's not in original list.", object);
                return null;
            }
            int initWeight = initWeightMap.get(k);
            int result = max(minWeight, oldValue - failReduceWeight.applyAsInt(initWeight));
            if (onMinWeight != null) {
                if (result == minWeight && result != oldValue) {
                    onMinWeight.accept(object);
                }
            }
            return result;
        });
        recoveryFuture.get();
    }

    @Override
    public void down(T object) {
        if (object == null) {
            logger.warn("invalid fail call, null object found.");
            return;
        }
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                logger.warn("invalid fail obj:{}, it's not in original list.", object);
                return null;
            }
            int result = minWeight;
            if (onMinWeight != null) {
                if (result != oldValue) {
                    onMinWeight.accept(object);
                }
            }
            return result;
        });
        recoveryFuture.get();
    }

    @Override
    public List<T> getAvailable() {
        return getAvailable(MAX_VALUE);
    }

    @Override
    public T getOneAvailable() {
        List<T> available = getAvailable(1);
        return available.isEmpty() ? null : available.get(0);
    }

    @Override
    public List<T> getAvailableExclude(Collection<T> exclusions) {
        return getAvailable(MAX_VALUE, exclusions);
    }

    @Override
    public List<T> getAvailable(int n) {
        return getAvailable(n, emptySet());
    }

    private List<T> getAvailable(int n, Collection<T> exclusions) {
        Map<T, Integer> snapshot = new HashMap<>(currentWeightMap);
        List<T> result = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (snapshot.isEmpty()) {
                break;
            }
            int sum = snapshot.values().stream().mapToInt(Integer::intValue).sum();
            if (sum == 0) {
                break;
            }
            int left = ThreadLocalRandom.current().nextInt(sum);
            Iterator<Entry<T, Integer>> iterator = snapshot.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<T, Integer> candidate = iterator.next();
                if (left < candidate.getValue()) {
                    T obj = candidate.getKey();
                    if (!exclusions.contains(obj)) {
                        result.add(obj);
                    }
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
            int initWeight = initWeightMap.get(k);
            return min(initWeight, oldValue + successIncreaseWeight.applyAsInt(initWeight));
        });
    }

    @Override
    public Set<T> getFailed() {
        return currentWeightMap.entrySet().stream() //
                .filter(entry -> entry.getValue() == 0) //
                .map(Entry::getKey) //
                .collect(toSet());
    }

    @Override
    public String toString() {
        return "WeightFailover [" + initWeightMap + "]" + "@" + Integer.toHexString(hashCode());
    }
}
