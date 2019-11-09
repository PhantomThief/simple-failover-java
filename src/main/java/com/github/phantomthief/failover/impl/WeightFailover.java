package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static com.github.phantomthief.util.MoreFunctions.catching;
import static com.github.phantomthief.util.MoreFunctions.runCatching;
import static com.github.phantomthief.util.MoreFunctions.runWithThreadName;
import static com.github.phantomthief.util.MoreFunctions.supplyWithThreadName;
import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.primitives.Ints.constrainToRange;
import static com.google.common.util.concurrent.Futures.addCallback;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.backoff.BackOff;
import com.github.phantomthief.failover.backoff.BackOffExecution;
import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;
import com.github.phantomthief.tuple.TwoTuple;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 默认权重记录
 * fail时权重下降
 * success时权重恢复
 *
 * @author w.vela
 */
public class WeightFailover<T> implements Failover<T>, Closeable {

    private static final Logger logger = getLogger(WeightFailover.class);

    private final String name;

    private final IntUnaryOperator failReduceWeight;
    private final IntUnaryOperator successIncreaseWeight;

    private final ConcurrentMap<T, Integer> initWeightMap;
    private final ConcurrentMap<T, Integer> currentWeightMap;
    private final CloseableSupplier<ScheduledFuture<?>> recoveryFuture;
    private final ConcurrentLinkedQueue<RecoveryTask> recoveryTasks;
    private final ConcurrentMap<T, RecoveryTask> runningRecoveryTasks;
    private final Consumer<T> onMinWeight;
    private final int minWeight;
    private final ToDoubleFunction<T> checker;

    /**
     * {@code null} if this feature is off.
     */
    private final Integer weightOnMissingNode;

    /**
     * 用于实现基于上下文的重试过滤逻辑
     */
    private final Predicate<T> filter;

    private volatile boolean closed;

    WeightFailover(WeightFailoverBuilder<T> builder) {
        this.name = builder.name;
        this.minWeight = builder.minWeight;
        this.failReduceWeight = builder.failReduceWeight;
        this.successIncreaseWeight = builder.successIncreaseWeight;
        this.initWeightMap = new ConcurrentHashMap<>(builder.initWeightMap);
        this.currentWeightMap = new ConcurrentHashMap<>(builder.initWeightMap);
        this.onMinWeight = builder.onMinWeight;
        this.weightOnMissingNode = builder.weightOnMissingNode;
        this.filter = builder.filter;
        this.checker = builder.checker;
        this.recoveryTasks = new ConcurrentLinkedQueue<>();
        this.runningRecoveryTasks = new ConcurrentHashMap<>();
        this.recoveryFuture = builder.backOff == null ? initDefaultChecker(builder) : initBackOffChecker(builder);
    }

    private CloseableSupplier<ScheduledFuture<?>> initDefaultChecker(WeightFailoverBuilder<T> builder) {
        return lazy(() -> SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(() -> {
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

    private CloseableSupplier<ScheduledFuture<?>> initBackOffChecker(WeightFailoverBuilder<T> builder) {
        return lazy(() -> SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(() -> {
            if (closed) {
                tryCloseRecoveryScheduler();
                return;
            }
            // 为权重为 0 的资源创建健康检查任务，放入待检查队列
            this.currentWeightMap.entrySet().stream()
                    .filter(entry -> entry.getValue() == 0)
                    .forEach((entry -> recoveryTasks.offer(new RecoveryTask(entry.getKey(), builder.backOff))));
            // 如果队列不为空则将其转发到 scheduler
            if (!recoveryTasks.isEmpty()) {
                while (true) {
                    RecoveryTask task = recoveryTasks.poll();
                    if (task == null) {
                        break;
                    }
                    // 如果健康检查耗时超过 checkDuration，可能导致对同一资源发起多次 hc 请求，这里规避一下
                    if (runningRecoveryTasks.containsKey(task.getObj())) {
                        continue;
                    }
                    T obj = task.getObj();
                    // 新建的 task 将其放入 running 队列中
                    runningRecoveryTasks.put(obj, task);
                    // 与外层共用一个线程池，貌似也没什么不妥...
                    ListenableFuture<Integer> future =
                            SharedCheckExecutorHolder.getInstance().schedule(task, task.nextBackOff(), MILLISECONDS);
                    addCallback(future, new FutureCallback<Integer>() {
                        @Override
                        public void onSuccess(@Nullable Integer recoveredWeight) {
                            runWithThreadName(origin -> origin + "-[" + name + "]", () -> runCatching(() -> {
                                // 不管成不成功执行完毕先将 task 删除
                                runningRecoveryTasks.remove(obj);
                                // 如果检查失败再次将其放入待检查队列，delay 时间顺延到下一周期
                                if (recoveredWeight == null || recoveredWeight == 0) {
                                    recoveryTasks.offer(task);
                                } else {
                                    currentWeightMap.put(obj, recoveredWeight);
                                    if (builder.onRecovered != null) {
                                        builder.onRecovered.accept(obj);
                                    }
                                }
                            }));
                        }

                        @Override
                        public void onFailure(@Nonnull Throwable t) {
                            // 不管成不成功执行完毕先将 task 删除
                            runningRecoveryTasks.remove(obj);
                        }
                    });
                }
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
                recoveryFuture.get();
            }
            return result;
        });
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
    }

    @Override
    public List<T> getAvailable() {
        List<T> result = new ArrayList<>(currentWeightMap.size());
        for (Entry<T, Integer> entry : currentWeightMap.entrySet()) {
            T item = entry.getKey();
            if (entry.getValue() > 0 && filter.test(item)) {
                result.add(item);
            }
        }
        return result;
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
                        if (!exclusions.contains(obj) && filter.test(obj)) {
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
        return currentWeightMap.entrySet().stream()
                .filter(entry -> entry.getValue() == 0)
                .map(Entry::getKey)
                .collect(toSet());
    }

    double currentWeight(T obj) {
        return currentWeightMap.get(obj);
    }

    @Override
    public String toString() {
        return "WeightFailover [" + initWeightMap + "]" + "@" + Integer.toHexString(hashCode());
    }

    class RecoveryTask implements Callable<Integer> {

        private final T obj;
        private final BackOffExecution backOff;

        RecoveryTask(T obj, BackOff backOff) {
            this.obj = obj;
            this.backOff = backOff.start();
        }

        @Override
        public Integer call() throws Exception {
            return supplyWithThreadName(origin -> origin + "-[" + name + "]", () -> catching(() -> {
                double recoverRate = checker.applyAsDouble(obj);
                if (recoverRate > 0) {
                    Integer initWeight = initWeightMap.get(obj);
                    if (initWeight == null) {
                        throw new IllegalStateException("obj:" + obj);
                    }
                    return constrainToRange((int) (initWeight * recoverRate), 1, initWeight);
                }
                return 0;
            }));
        }

        @Nonnull
        T getObj() {
            return obj;
        }

        long nextBackOff() {
            return backOff.nextBackOff();
        }

    }

}
