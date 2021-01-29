package com.github.phantomthief.failover.impl;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.github.phantomthief.failover.SimpleFailover;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.PriorityFailoverConfig;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.ResConfig;
import com.github.phantomthief.failover.util.AliasMethod;

/**
 * SimpleFailover的实现，绝大部分场景下可以代替WeightFailover，性能和功能都要更强一些。
 *
 * <p>
 * 请先阅读README.md，可以到<a href="https://github.com/PhantomThief/simple-failover-java">这里</a>在线阅读。
 * </p>
 *
 * @author huangli
 * Created on 2020-01-16
 */
@ThreadSafe
public class PriorityFailover<T> implements SimpleFailover<T>, AutoCloseable {

    private final PriorityFailoverConfig<T> config;
    private final PriorityFailoverCheckTask<T> checkTask;

    private final HashMap<T, ResInfo<T>> resourcesMap;
    private final GroupInfo<T>[] groups;

    private final boolean concurrentCtrl;
    private final boolean manualConcurrencyControl;
    private static final int MAX_CONCURRENCY = 100000;

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class ResInfo<T> {
        final T resource;
        final int priority;
        final double maxWeight;
        final double minWeight;

        /**
         * if not null, indicate that config.concurrencyControl=true
         */
        @Nullable
        final Concurrency concurrency;

        volatile double currentWeight;

        ResInfo(T resource, int priority, double maxWeight, double minWeight,
                double initWeight, boolean concurrencyCtrl) {
            this.resource = resource;
            this.priority = priority;
            this.maxWeight = maxWeight;
            this.minWeight = minWeight;
            this.currentWeight = initWeight;
            if (concurrencyCtrl) {
                concurrency = new Concurrency();
            } else {
                concurrency = null;
            }
        }
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class GroupInfo<T> {
        final int priority;

        @Nonnull
        final ResInfo<T>[] resources;

        final double totalMaxWeight;

        final boolean maxWeightSame;

        @Nullable
        final AliasMethod<ResInfo<T>> aliasMethod;

        int roundRobinIndex;

        volatile GroupWeightInfo groupWeightInfo;

        GroupInfo(int priority, @Nonnull ResInfo<T>[] resources, double totalMaxWeight,
                boolean maxWeightSame, @Nullable AliasMethod<ResInfo<T>> aliasMethod, GroupWeightInfo groupWeightInfo) {
            this.priority = priority;
            this.resources = resources;
            this.totalMaxWeight = totalMaxWeight;
            this.maxWeightSame = maxWeightSame;
            this.aliasMethod = aliasMethod;
            this.groupWeightInfo = groupWeightInfo;
        }
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class GroupWeightInfo {
        final boolean roundRobin;
        final boolean aliasMethod;

        final double totalCurrentWeight;

        final double[] currentWeightCopy;

        final double healthyRate;

        GroupWeightInfo(boolean maxWeightSame, double totalCurrentWeight,
                double totalMaxWeight, double[] currentWeightCopy, @Nullable AliasMethod<?> aliasMethod) {
            this.totalCurrentWeight = totalCurrentWeight;
            if (totalMaxWeight == 0) {
                this.healthyRate = 0;
            } else {
                this.healthyRate = totalCurrentWeight / totalMaxWeight;
            }
            this.currentWeightCopy = currentWeightCopy;
            this.roundRobin = maxWeightSame && totalCurrentWeight == totalMaxWeight && totalMaxWeight > 0;
            this.aliasMethod = aliasMethod != null && totalCurrentWeight == totalMaxWeight;
        }
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class Concurrency {
        final AtomicInteger atomicValue = new AtomicInteger(0);

        Concurrency() {
        }

        public void incr() {
            atomicValue.updateAndGet(v -> Math.min(v + 1, MAX_CONCURRENCY));
        }

        public void decr() {
            atomicValue.updateAndGet(v -> Math.max(v - 1, 0));
        }

        public int get() {
            return atomicValue.get();
        }

        public void reset() {
            atomicValue.set(0);
        }
    }

    /**
     * 用来表示一个资源的内部状态。
     * @see PriorityFailover#getResourceStatus(Object)
     */
    public static class ResStatus {
        private double maxWeight;
        private double minWeight;
        private int priority;
        private double currentWeight;
        private int concurrency;

        /**
         * 获取该资源的最大权重。
         * @return 最大权重
         */
        public double getMaxWeight() {
            return maxWeight;
        }

        /**
         * 获取该资源的最小权重。
         * @return 最小权重
         */
        public double getMinWeight() {
            return minWeight;
        }

        /**
         * 获取该资源的优先级。
         * @return 优先级
         */
        public int getPriority() {
            return priority;
        }

        /**
         * 获取该资源的当前权重。
         * @return 当前权重
         */
        public double getCurrentWeight() {
            return currentWeight;
        }

        /**
         * 获取该资源的并发度。
         * @return 并发度
         */
        public int getConcurrency() {
            return concurrency;
        }
    }

    @SuppressWarnings("unchecked")
    PriorityFailover(PriorityFailoverConfig<T> config) {
        Objects.requireNonNull(config.getResources());
        this.config = config;
        int resCount = config.getResources().size();
        this.resourcesMap = new HashMap<>(resCount * 3);
        this.concurrentCtrl = config.isConcurrencyControl();
        this.manualConcurrencyControl = config.isManualConcurrencyControl();
        TreeMap<Integer, ArrayList<ResInfo<T>>> priorityMap = new TreeMap<>();
        for (Entry<T, ResConfig> en : config.getResources().entrySet()) {
            T res = en.getKey();
            ResConfig rc = en.getValue();
            int priority = rc.getPriority();
            ResInfo<T> ri = new ResInfo<>(res, priority, rc.getMaxWeight(),
                    rc.getMinWeight(), rc.getInitWeight(), config.isConcurrencyControl());
            this.resourcesMap.put(res, ri);
            priorityMap.compute(priority, (k, v) -> {
                if (v == null) {
                    v = new ArrayList<>();
                }
                v.add(ri);
                return v;
            });
        }

        groups = new GroupInfo[priorityMap.size()];
        int groupIndex = 0;
        for (Entry<Integer, ArrayList<ResInfo<T>>> en : priorityMap.entrySet()) {
            final int priority = en.getKey();
            ArrayList<ResInfo<T>> list = en.getValue();
            Collections.shuffle(list);
            final ResInfo<T>[] resources = list.toArray(new ResInfo[list.size()]);
            double totalMaxWeight = 0;
            double totalCurrentWeight = 0;
            double firstMaxWeight = resources[0].maxWeight;
            boolean maxWeightSame = true;
            double[] currentWeightCopy = new double[resources.length];
            for (int i = 0; i < resources.length; i++) {
                ResInfo<T> ri = resources[i];
                totalMaxWeight += ri.maxWeight;
                totalCurrentWeight += ri.currentWeight;
                currentWeightCopy[i] = ri.currentWeight;
                if (ri.maxWeight != firstMaxWeight) {
                    maxWeightSame = false;
                }
            }
            AliasMethod<ResInfo<T>> aliasMethod = null;
            if (!maxWeightSame && resources.length > config.getAliasMethodThreshold()) {
                aliasMethod = new AliasMethod<>(stream(resources)
                        .collect(toMap(ri -> ri, ri -> ri.maxWeight)));
            }
            GroupWeightInfo groupWeightInfo = new GroupWeightInfo(maxWeightSame, totalCurrentWeight,
                    totalMaxWeight, currentWeightCopy, aliasMethod);

            groups[groupIndex++] = new GroupInfo<>(priority, resources,
                    totalMaxWeight, maxWeightSame, aliasMethod, groupWeightInfo);
        }

        checkTask = new PriorityFailoverCheckTask<>(config, this);
    }

    public static <T> PriorityFailoverBuilder<T> newBuilder() {
        return new PriorityFailoverBuilder<>();
    }


    /**
     * 个别情况下想要手工做并发度控制，调用此方法，并发度加1。
     */
    public void incrConcurrency(@Nullable T object) {
        if (object == null) {
            return;
        }
        ResInfo<T> resInfo = resourcesMap.get(object);
        if (resInfo == null) {
            return;
        }
        if (resInfo.concurrency != null) {
            resInfo.concurrency.incr();
        }
    }

    /**
     * 个别情况下想要手工做并发度控制，调用此方法，并发度减1。
     */
    public void decrConcurrency(@Nullable T object) {
        if (object == null) {
            return;
        }
        ResInfo<T> resInfo = resourcesMap.get(object);
        if (resInfo == null) {
            return;
        }
        if (resInfo.concurrency != null) {
            resInfo.concurrency.decr();
        }
    }

    /**
     * 个别情况下想要手工做并发度控制，调用此方法，并发度设置为0。
     */
    public void resetConcurrency(@Nullable T object) {
        if (object == null) {
            return;
        }
        ResInfo<T> resInfo = resourcesMap.get(object);
        if (resInfo == null) {
            return;
        }
        if (resInfo.concurrency != null) {
            resInfo.concurrency.reset();
        }
    }

    @Override
    public void success(@Nonnull T object) {
        processWeight(object, true);
    }

    @Override
    public void fail(@Nonnull T object) {
        processWeight(object, false);
        checkTask.ensureStart();
    }

    private void processWeight(@Nonnull T object, boolean success) {
        ResInfo<T> resInfo = resourcesMap.get(object);
        if (resInfo == null) {
            return;
        }
        if (resInfo.concurrency != null && !manualConcurrencyControl) {
            resInfo.concurrency.decr();
        }
        updateWeight(success, resInfo, config, groups);
    }

    static <T> void updateWeight(boolean success, ResInfo<T> resInfo, PriorityFailoverConfig<T> config,
            GroupInfo<T>[] groups) {
        double maxWeight = resInfo.maxWeight;
        double minWeight = resInfo.minWeight;

        double currentWeight = resInfo.currentWeight;
        if ((success && currentWeight >= maxWeight) || (!success && currentWeight <= minWeight)) {
            return;
        }
        int priority = resInfo.priority;
        T res = resInfo.resource;
        double newWeight;
        if (success) {
            newWeight = config.getWeightFunction().success(maxWeight,
                    minWeight, priority, currentWeight, res);
        } else {
            newWeight = config.getWeightFunction().fail(maxWeight,
                    minWeight, priority, currentWeight, res);
        }
        newWeight = Math.min(newWeight, maxWeight);
        newWeight = Math.max(newWeight, minWeight);
        if (newWeight == currentWeight) {
            return;
        }

        resInfo.currentWeight = newWeight;

        updateGroupHealthy(priority, groups);

        WeightListener<T> listener = config.getWeightListener();
        if (listener != null) {
            if (success) {
                listener.onSuccess(maxWeight, minWeight, priority, currentWeight, newWeight, res);
            } else {
                listener.onFail(maxWeight, minWeight, priority, currentWeight, newWeight, res);
            }
        }
    }

    static <T> void updateGroupHealthy(int priority, GroupInfo<T>[] groups) {
        for (GroupInfo<T> psi : groups) {
            if (psi.priority == priority) {
                double sumCurrentWeight = 0.0;
                ResInfo<T>[] resources = psi.resources;
                int resCount = resources.length;
                double[] weightCopy = new double[resCount];
                for (int i = 0; i < resCount; i++) {
                    ResInfo<T> ri = resources[i];
                    sumCurrentWeight += ri.currentWeight;
                    weightCopy[i] = ri.currentWeight;
                }
                psi.groupWeightInfo = new GroupWeightInfo(psi.maxWeightSame, sumCurrentWeight,
                        psi.totalMaxWeight, weightCopy, psi.aliasMethod);
            }
        }
    }

    @Override
    public void down(@Nonnull T object) {
        ResInfo<T> resInfo = resourcesMap.get(object);
        if (resInfo == null) {
            return;
        }
        if (resInfo.concurrency != null && !manualConcurrencyControl) {
            resInfo.concurrency.decr();
        }
        double oldWeight = resInfo.currentWeight;
        resInfo.currentWeight = resInfo.minWeight;
        updateGroupHealthy(resInfo.priority, groups);
        WeightListener<T> listener = config.getWeightListener();
        if (listener != null) {
            listener.onFail(resInfo.maxWeight, resInfo.minWeight,
                    resInfo.priority, oldWeight, resInfo.currentWeight, resInfo.resource);
        }
        checkTask.ensureStart();
    }

    @Nullable
    @Override
    public T getOneAvailable() {
        return getOneAvailableExclude(Collections.emptyList());
    }

    @Nullable
    @Override
    public T getOneAvailableExclude(@Nonnull Collection<T> exclusions) {
        int groupCount = groups.length;
        if (groupCount == 0) {
            return null;
        }
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        int preferGroupIndex;
        if (groupCount == 1) {
            preferGroupIndex = 0;
        } else {
            preferGroupIndex = selectGroup(threadLocalRandom);
        }

        for (int i = 0; i < groupCount; i++) {
            ResInfo<T> ri = findOneInRegion(threadLocalRandom, preferGroupIndex, exclusions);
            if (ri != null) {
                if (ri.concurrency != null && !manualConcurrencyControl) {
                    ri.concurrency.incr();
                }
                return ri.resource;
            }
            preferGroupIndex = (preferGroupIndex + 1) % groupCount;
        }
        return null;
    }

    private ResInfo<T> findOneInRegion(ThreadLocalRandom threadLocalRandom, int preferGroupIndex,
            @Nonnull Collection<T> exclusions) {
        GroupInfo<T> groupInfo = groups[preferGroupIndex];
        ResInfo<T>[] resources = groupInfo.resources;
        int resCount = resources.length;

        GroupWeightInfo groupWeightInfo = groupInfo.groupWeightInfo;
        boolean conCtrl = this.concurrentCtrl;
        if (exclusions.isEmpty() && !conCtrl) {
            if (groupWeightInfo.roundRobin) {
                int roundRobinIndex = groupInfo.roundRobinIndex;
                groupInfo.roundRobinIndex = (roundRobinIndex + 1) % resCount;
                return resources[roundRobinIndex];
            } else if (groupWeightInfo.aliasMethod) {
                return groupInfo.aliasMethod.get();
            } else {
                double totalCurrentWeight = groupWeightInfo.totalCurrentWeight;
                if (totalCurrentWeight <= 0) {
                    return null;
                }
                double random = threadLocalRandom.nextDouble(totalCurrentWeight);
                double x = 0;
                double[] weightCopy = groupWeightInfo.currentWeightCopy;
                for (int i = 0; i < resCount; i++) {
                    x += weightCopy[i];
                    if (random < x) {
                        return resources[i];
                    }
                }
            }
        } else {
            double sumWeight = 0;
            double[] weights = new double[resCount];
            double[] weightCopy = groupWeightInfo.currentWeightCopy;
            for (int i = 0; i < resCount; i++) {
                ResInfo<T> ri = resources[i];
                if (!exclusions.contains(ri.resource)) {
                    double w = weightCopy[i];
                    if (conCtrl) {
                        int c = ri.concurrency.get();
                        w = w / (1.0 + c);
                    }
                    weights[i] = w;
                    sumWeight += w;
                } else {
                    weights[i] = -1;
                }
            }

            if (sumWeight <= 0) {
                return null;
            }
            double random = threadLocalRandom.nextDouble(sumWeight);
            double x = 0;
            for (int i = 0; i < resCount; i++) {
                double w = weights[i];
                if (w < 0) {
                    continue;
                } else {
                    x += w;
                }
                if (random < x) {
                    return resources[i];
                }
            }
        }

        // maybe precise problem, return first one which is not excluded
        for (ResInfo<T> ri : resources) {
            if (ri.currentWeight > 0 && !exclusions.contains(ri.resource)) {
                return ri;
            }
        }

        return null;
    }

    int selectGroup(ThreadLocalRandom random) {
        GroupInfo<T>[] gs = this.groups;
        int groupCount = gs.length;
        double factor = config.getPriorityFactor();
        double groupRandom = -1.0;
        for (int i = 0; i < groupCount; i++) {
            GroupInfo<T> group = gs[i];
            double healthyRate = group.groupWeightInfo.healthyRate;
            if (factor == Double.MAX_VALUE && healthyRate > 0) {
                return i;
            }
            double finalHealthyRate = healthyRate * factor;
            if (finalHealthyRate >= 1.0) {
                return i;
            } else {
                if (groupRandom < 0) {
                    groupRandom = random.nextDouble();
                }
                if (groupRandom < finalHealthyRate) {
                    return i;
                } else {
                    // regenerate a random value between 0 and 1 for next use
                    groupRandom = (groupRandom - finalHealthyRate) / (1.0 - finalHealthyRate);
                }
            }
        }
        return 0;
    }

    /**
     * 关闭failover内部的健康检查任务。
     */
    @Override
    public void close() {
        checkTask.close();
    }

    /**
     * 获取一个资源的内部状态。
     * @param resource 资源
     * @return 资源内部状态，如果根据传入的参数找不到结果，返回null
     */
    public ResStatus getResourceStatus(T resource) {
        ResInfo<T> resInfo = resourcesMap.get(resource);
        if (resInfo == null) {
            return null;
        } else {
            ResStatus status = new ResStatus();
            status.maxWeight = resInfo.maxWeight;
            status.minWeight = resInfo.minWeight;
            status.priority = resInfo.priority;
            status.currentWeight = resInfo.currentWeight;
            if (resInfo.concurrency != null) {
                status.concurrency = resInfo.concurrency.get();
            }
            return status;
        }
    }

    /**
     * 获取所有的资源，返回的是一个List副本。
     * @return 所有资源的副本
     */
    public List<T> getAll() {
        return new ArrayList<>(resourcesMap.keySet());
    }

    HashMap<T, ResInfo<T>> getResourcesMap() {
        return resourcesMap;
    }

    GroupInfo<T>[] getGroups() {
        return groups;
    }

    PriorityFailoverCheckTask<T> getCheckTask() {
        return checkTask;
    }

    PriorityFailoverConfig<T> getConfig() {
        return config;
    }
}
