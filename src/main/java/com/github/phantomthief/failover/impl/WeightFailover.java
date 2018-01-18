package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.primitives.Ints.constrainToRange;
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
import java.util.LinkedList;
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

import org.slf4j.Logger;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;
import com.github.phantomthief.tuple.TwoTuple;
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

    /**
     * {@code null} if this feature is off.
     */
    private final Integer weightOnMissingNode;

    private volatile boolean closed;

    WeightFailover(WeightFailoverBuilder<T> builder) {
        this.minWeight = builder.minWeight;
        this.failReduceWeight = builder.failReduceWeight;
        this.successIncreaseWeight = builder.successIncreaseWeight;
        this.initWeightMap = new ConcurrentHashMap<>(builder.initWeightMap);
        this.currentWeightMap = new ConcurrentHashMap<>(builder.initWeightMap);
        this.onMinWeight = builder.onMinWeight;
        this.weightOnMissingNode = builder.weightOnMissingNode;
        this.recoveryFuture = lazy(
                () -> SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(() -> {
                    if (closed) {
                        tryCloseRecoveryScheduler();
                        return;
                    }
                    Thread currentThread = Thread.currentThread();
                    String origName = currentThread.getName();
                    if (builder.name != null) {
                        currentThread.setName(origName + "-[" + builder.name + "]");
                    }
                    try {
                        Map<T, Double> recoveredObjects = new HashMap<>();
                        this.currentWeightMap.forEach((obj, weight) -> {
                            if (weight == 0) {
                                double recoverRate = builder.checker.applyAsDouble(obj);
                                if (recoverRate > 0) {
                                    recoveredObjects.put(obj, recoverRate);
                                }
                            }
                        });
                        if (!recoveredObjects.isEmpty()) {
                            logger.info("found recovered objects:{}", recoveredObjects);
                        }
                        recoveredObjects.forEach((recovered, rate) -> {
                            Integer initWeight = initWeightMap.get(recovered);
                            if (initWeight == null) {
                                throw new IllegalStateException("obj:" + recovered);
                            }
                            int recoveredWeight = constrainToRange((int) (initWeight * rate), 1,
                                    initWeight);
                            currentWeightMap.put(recovered, recoveredWeight);
                            if (builder.onRecovered != null) {
                                builder.onRecovered.accept(recovered);
                            }
                        });
                    } catch (Throwable e) {
                        logger.error("", e);
                    } finally {
                        currentThread.setName(origName);
                    }
                }, builder.checkDuration, builder.checkDuration, MILLISECONDS));
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
        closed = true;
        tryCloseRecoveryScheduler();
    }

    private void tryCloseRecoveryScheduler() {
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
                if (weightOnMissingNode == null) {
                    logger.warn("invalid fail obj:{}, it's not in original list.", object);
                    return null;
                } else {
                    oldValue = weightOnMissingNode;
                    initWeightMap.putIfAbsent(object, weightOnMissingNode);
                }
            }
            int initWeight = initWeightMap.get(k);
            int result = max(minWeight, oldValue - failReduceWeight.applyAsInt(initWeight));
            if (onMinWeight != null) {
                if (result == minWeight && result != oldValue) {
                    onMinWeight.accept(object);
                }
            }
            if (result == 0) {
                logger.warn("found down object:{}", k);
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
                if (weightOnMissingNode == null) {
                    logger.warn("invalid fail obj:{}, it's not in original list.", object);
                    return null;
                } else {
                    oldValue = weightOnMissingNode;
                    initWeightMap.putIfAbsent(object, weightOnMissingNode);
                }
            }
            int result = minWeight;
            if (onMinWeight != null) {
                if (result != oldValue) {
                    onMinWeight.accept(object);
                }
            }
            if (result == 0) {
                logger.warn("found down object:{}", k);
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
        // TODO better using a snapshot current Weight<T> or a new stateful Weight<T>
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
        List<TwoTuple<T, Integer>> snapshot = new LinkedList<>();
        int sum = 0;
        for (Entry<T, Integer> entry : currentWeightMap.entrySet()) {
            int thisWeight = entry.getValue();
            snapshot.add(tuple(entry.getKey(), thisWeight));
            sum += thisWeight;
        }
        List<T> result = new ArrayList<>();
        if (sum > 0) {
            for (int i = 0; i < n; i++) {
                if (snapshot.isEmpty() || sum == 0) {
                    break;
                }
                int left = ThreadLocalRandom.current().nextInt(sum);
                Iterator<TwoTuple<T, Integer>> iterator = snapshot.iterator();
                while (iterator.hasNext()) {
                    TwoTuple<T, Integer> candidate = iterator.next();
                    int entryWeight = candidate.getSecond();
                    if (left < entryWeight) {
                        T obj = candidate.getFirst();
                        if (!exclusions.contains(obj)) {
                            result.add(obj);
                        }
                        iterator.remove();
                        sum -= entryWeight;
                        break;
                    }
                    left -= entryWeight;
                }
            }
        }
        return result;
    }

    @Override
    public void success(T object) {
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                if (weightOnMissingNode == null) {
                    logger.warn("invalid fail obj:{}, it's not in original list.", object);
                    return null;
                } else {
                    oldValue = weightOnMissingNode;
                    initWeightMap.putIfAbsent(object, weightOnMissingNode);
                }
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

    double currentWeight(T obj) {
        return currentWeightMap.get(obj);
    }

    @Override
    public String toString() {
        return "WeightFailover [" + initWeightMap + "]" + "@" + Integer.toHexString(hashCode());
    }
}
