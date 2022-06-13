package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.tuple.TwoTuple;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;
import com.google.common.collect.ImmutableList;

/**
 * 基于权重的failover，失败减，成功加，这是个早期的实现，用的也非常多，
 * 现在建议使用{@link com.github.phantomthief.failover.SimpleFailover SimpleFailover}接口
 * 和{@link PriorityFailover}实现。
 *
 * 需要注意的是：1、WeightFailover的权重是整数。2、WeightFailover不区分初始权重和最大权重（即初始权重总是等于最大权重）。
 *
 * <p>
 * 请先阅读README.md，可以到<a href="https://github.com/PhantomThief/simple-failover-java">这里</a>在线阅读。
 * </p>
 *
 * @author w.vela
 */
public class WeightFailover<T> implements Failover<T>, Closeable {

    private static final Logger logger = getLogger(WeightFailover.class);

    private final IntUnaryOperator failReduceWeight;
    private final IntUnaryOperator successIncreaseWeight;

    private final ConcurrentMap<T, Integer> initWeightMap;
    private final ConcurrentMap<T, Integer> currentWeightMap;
    @SuppressWarnings("checkstyle:VisibilityModifier")
    final CloseableSupplier<ScheduledFuture<?>> recoveryFuture;
    private final Consumer<T> onMinWeight;
    private final int minWeight;

    /**
     * {@code null} if this feature is off.
     */
    private final Integer weightOnMissingNode;

    /**
     * 用于实现基于上下文的重试过滤逻辑
     */
    @Nullable
    private final Predicate<T> filter;

    @SuppressWarnings("checkstyle:VisibilityModifier")
    AtomicBoolean closed = new AtomicBoolean(false);

    private AtomicInteger allAvailableVersion = new AtomicInteger();

    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    private static class AllAvailable<T> {
        int version;
        List<T> allAvailable;
    }

    private volatile AllAvailable<T> allAvailable;

    WeightFailover(WeightFailoverBuilder<T> builder) {
        this.minWeight = builder.minWeight;
        this.failReduceWeight = builder.failReduceWeight;
        this.successIncreaseWeight = builder.successIncreaseWeight;
        this.initWeightMap = new ConcurrentHashMap<>(builder.initWeightMap);
        this.currentWeightMap = new ConcurrentHashMap<>(builder.initWeightMap);
        this.onMinWeight = builder.onMinWeight;
        this.weightOnMissingNode = builder.weightOnMissingNode;
        this.filter = builder.filter;
        this.allAvailable = new AllAvailable<>();
        this.allAvailable.allAvailable = ImmutableList.copyOf(builder.initWeightMap.keySet());
        this.allAvailable.version = allAvailableVersion.get();
        WeightFailoverCheckTask t = new WeightFailoverCheckTask<>(this, builder, closed,
                initWeightMap, currentWeightMap, allAvailableVersion);
        this.recoveryFuture = t.lazyFuture();
    }

    /**
     * better use {@link #newGenericBuilder()} for type safe
     */
    @Deprecated
    public static WeightFailoverBuilder<Object> newBuilder() {
        return new WeightFailoverBuilder<>();
    }

    /**
     * 获取一个新的builder。
     * @param <E> 要构建的资源的类型
     * @return builder
     */
    public static <E> GenericWeightFailoverBuilder<E> newGenericBuilder() {
        return new GenericWeightFailoverBuilder<>(newBuilder());
    }

    /**
     * 关闭failover，释放内部的健康检查任务。
     */
    @Override
    public void close() {
        closed.set(true);
        tryCloseRecoveryScheduler(recoveryFuture, this::toString);
    }

    static void tryCloseRecoveryScheduler(CloseableSupplier<ScheduledFuture<?>> recoveryFuture,
            Supplier<String> nameSupplier) {
        synchronized (recoveryFuture) {
            recoveryFuture.ifPresent(future -> {
                if (!future.isCancelled()) {
                    if (!future.cancel(true)) {
                        logger.warn("fail to close failover:{}", nameSupplier.get());
                    }
                }
            });
        }
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
                recoveryFuture.get();
            }
            return result;
        });
        allAvailableVersion.incrementAndGet();
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
                recoveryFuture.get();
            }
            return result;
        });
        allAvailableVersion.incrementAndGet();
    }

    @Override
    public List<T> getAvailable() {
        // refresh if need
        int version = allAvailableVersion.get();
        boolean refreshed = false;
        if (allAvailable.version != version) {
            refreshed = true;
            AllAvailable tmp = new AllAvailable<>();
            tmp.version = version;
            tmp.allAvailable = doGetAvailable();
            allAvailable = tmp;
        }


        if (filter == null) {
            return allAvailable.allAvailable;
        } else {
            if (refreshed) {
                return allAvailable.allAvailable;
            } else {
                return doGetAvailable();
            }
        }
    }

    private List<T> doGetAvailable() {
        List<T> result = new ArrayList<>(currentWeightMap.size());
        for (Entry<T, Integer> entry : currentWeightMap.entrySet()) {
            T item = entry.getKey();
            if (entry.getValue() > 0 && (filter == null || filter.test(item))) {
                result.add(item);
            }
        }
        return unmodifiableList(result);
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

    @Nullable
    @Override
    public T getOneAvailableExclude(Collection<T> exclusions) {
        List<T> result = getAvailable(1, exclusions);
        return result.isEmpty() ? null : result.get(0);
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
            int size = snapshot.size();
            for (int i = 0; i < size; i++) {
                if (sum == 0) {
                    break;
                }
                if (result.size() == n) {
                    break;
                }
                int left = ThreadLocalRandom.current().nextInt(sum);
                Iterator<TwoTuple<T, Integer>> iterator = snapshot.iterator();
                while (iterator.hasNext()) {
                    TwoTuple<T, Integer> candidate = iterator.next();
                    int entryWeight = candidate.getSecond();
                    if (left < entryWeight) {
                        T obj = candidate.getFirst();
                        if (!exclusions.contains(obj) && (filter == null || filter.test(obj))) {
                            result.add(obj);
                        }
                        if (result.size() == n) {
                            break;
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
        boolean[] availableChanged = { false };
        currentWeightMap.compute(object, (k, oldValue) -> {
            if (oldValue == null) {
                if (weightOnMissingNode == null) {
                    logger.warn("invalid fail obj:{}, it's not in original list.", object);
                    return null;
                } else {
                    availableChanged[0] = true;
                    oldValue = weightOnMissingNode;
                    initWeightMap.putIfAbsent(object, weightOnMissingNode);
                }
            }
            int initWeight = initWeightMap.get(k);
            int weight = min(initWeight, oldValue + successIncreaseWeight.applyAsInt(initWeight));
            if (oldValue <= 0 && weight > 0) {
                availableChanged[0] = true;
            }
            return weight;
        });
        if (availableChanged[0]) {
            allAvailableVersion.incrementAndGet();
        }
    }

    @Override
    public Set<T> getFailed() {
        return currentWeightMap.entrySet().stream()
                .filter(entry -> entry.getValue() == 0)
                .map(Entry::getKey)
                .collect(toSet());
    }

    int currentWeight(T obj) {
        return currentWeightMap.get(obj);
    }

    int initWeight(T obj) {
        return initWeightMap.get(obj);
    }

    @Override
    public String toString() {
        return "WeightFailover [" + initWeightMap + "]" + "@" + Integer.toHexString(hashCode());
    }
}
