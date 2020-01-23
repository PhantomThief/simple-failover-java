package com.github.phantomthief.failover.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

/**
 * @author huangli
 * Created on 2020-01-16
 */
@ThreadSafe
public class PriorityFailover<T> implements SimpleFailover<T>, AutoCloseable {

    private final PriorityFailoverConfig<T> config;
    private final PriorityFailoverCheckTask<T> checkTask;

    private final HashMap<T, ResInfo<T>> resourcesMap;
    private final PrioritySectionInfo<T>[] prioritySections;

    private final boolean concurrentCtrl;

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
    static class PrioritySectionInfo<T> {
        final int priority;

        @Nonnull
        final ResInfo<T>[] resources;

        final double totalMaxWeight;

        final boolean maxWeightSame;

        int roundRobinIndex;

        volatile SectionWeightInfo sectionWeightInfo;

        PrioritySectionInfo(int priority, @Nonnull ResInfo<T>[] resources, double totalMaxWeight,
                boolean maxWeightSame, SectionWeightInfo sectionWeightInfo) {
            this.priority = priority;
            this.resources = resources;
            this.totalMaxWeight = totalMaxWeight;
            this.maxWeightSame = maxWeightSame;
            this.sectionWeightInfo = sectionWeightInfo;
        }
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class SectionWeightInfo {
        final boolean roundRobin;

        final double totalCurrentWeight;

        //
        final double[] currentWeightCopy;

        final double healthyRate;

        SectionWeightInfo(double totalCurrentWeight, double healthyRate, double[] currentWeightCopy,
                boolean roundRobin) {
            this.totalCurrentWeight = totalCurrentWeight;
            this.healthyRate = healthyRate;
            this.currentWeightCopy = currentWeightCopy;
            this.roundRobin = roundRobin;
        }
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class Concurrency {
        final AtomicInteger atomicValue = new AtomicInteger(0);

        Concurrency() {
        }

        public void incr() {
            atomicValue.updateAndGet(v -> Math.max(v + 1, 0));
        }

        public void decr() {
            atomicValue.updateAndGet(v -> Math.max(v - 1, 0));
        }

        public int get() {
            return atomicValue.get();
        }
    }

    @SuppressWarnings("unchecked")
    PriorityFailover(PriorityFailoverConfig<T> config) {
        Objects.requireNonNull(config.getResources());
        this.config = config;
        int resCount = config.getResources().size();
        this.resourcesMap = new HashMap<>(resCount * 3);
        this.concurrentCtrl = config.isConcurrencyControl();
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

        prioritySections = new PrioritySectionInfo[priorityMap.size()];
        int sectionIndex = 0;
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
            SectionWeightInfo sectionWeightInfo = new SectionWeightInfo(totalCurrentWeight,
                    totalCurrentWeight / totalMaxWeight, currentWeightCopy,
                    maxWeightSame && totalCurrentWeight == totalMaxWeight);
            prioritySections[sectionIndex++] = new PrioritySectionInfo<>(priority, resources,
                    totalMaxWeight, maxWeightSame, sectionWeightInfo);
        }

        checkTask = new PriorityFailoverCheckTask<>(config, this);
    }

    public static <T> PriorityFailoverBuilder<T> newBuilder() {
        return new PriorityFailoverBuilder<>();
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
        if (resInfo.concurrency != null) {
            resInfo.concurrency.decr();
        }
        updateWeight(success, resInfo, config, prioritySections);
    }

    static <T> void updateWeight(boolean success, ResInfo<T> resInfo, PriorityFailoverConfig<T> config,
            PrioritySectionInfo<T>[] sections) {
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

        updateSectionHealthy(priority, sections);

        WeightListener<T> listener = config.getWeightListener();
        if (listener != null) {
            if (success) {
                listener.onSuccess(maxWeight, minWeight, priority, newWeight, res);
            } else {
                listener.onFail(maxWeight, minWeight, priority, newWeight, res);
            }
        }
    }

    static <T> void updateSectionHealthy(int priority, PrioritySectionInfo<T>[] sections) {
        for (PrioritySectionInfo<T> psi : sections) {
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
                psi.sectionWeightInfo = new SectionWeightInfo(sumCurrentWeight,
                        sumCurrentWeight / psi.totalMaxWeight, weightCopy,
                        psi.maxWeightSame && sumCurrentWeight == psi.totalMaxWeight);
            }
        }
    }

    @Override
    public void down(@Nonnull T object) {
        ResInfo<T> resInfo = resourcesMap.get(object);
        if (resInfo == null) {
            return;
        }
        if (resInfo.concurrency != null) {
            resInfo.concurrency.decr();
        }
        resInfo.currentWeight = resInfo.minWeight;
        updateSectionHealthy(resInfo.priority, prioritySections);
        WeightListener<T> listener = config.getWeightListener();
        if (listener != null) {
            listener.onFail(resInfo.maxWeight, resInfo.minWeight,
                    resInfo.priority, resInfo.currentWeight, resInfo.resource);
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
        int sectionCount = prioritySections.length;
        if (sectionCount == 0) {
            return null;
        }
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        int preferSectionIndex;
        if (sectionCount == 1) {
            preferSectionIndex = 0;
        } else {
            preferSectionIndex = selectSection(threadLocalRandom);
        }

        for (int i = 0; i < sectionCount; i++) {
            ResInfo<T> ri = findOneInRegion(threadLocalRandom, preferSectionIndex, exclusions);
            if (ri != null) {
                if (ri.concurrency != null) {
                    ri.concurrency.incr();
                }
                return ri.resource;
            }
            preferSectionIndex = (preferSectionIndex + 1) % sectionCount;
        }
        return null;
    }

    private ResInfo<T> findOneInRegion(ThreadLocalRandom threadLocalRandom, int preferSectionIndex,
            @Nonnull Collection<T> exclusions) {
        PrioritySectionInfo<T> sectionInfo = prioritySections[preferSectionIndex];
        ResInfo<T>[] resources = sectionInfo.resources;
        int resCount = resources.length;

        SectionWeightInfo sectionWeightInfo = sectionInfo.sectionWeightInfo;
        boolean conCtrl = this.concurrentCtrl;
        if (exclusions.isEmpty() && !conCtrl) {
            if (sectionWeightInfo.roundRobin) {
                int roundRobinIndex = sectionInfo.roundRobinIndex;
                sectionInfo.roundRobinIndex = (roundRobinIndex + 1) % resCount;
                return resources[roundRobinIndex];
            } else {
                double totalCurrentWeight = sectionWeightInfo.totalCurrentWeight;
                if (totalCurrentWeight <= 0) {
                    return null;
                }
                double random = threadLocalRandom.nextDouble(totalCurrentWeight);
                double x = 0;
                double[] weightCopy = sectionWeightInfo.currentWeightCopy;
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
            double[] weightCopy = sectionWeightInfo.currentWeightCopy;
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

    int selectSection(ThreadLocalRandom random) {
        PrioritySectionInfo<T>[] sections = this.prioritySections;
        int sectionCount = sections.length;
        double factor = config.getPriorityFactor();
        double sectionRandom = -1.0;
        for (int i = 0; i < sectionCount; i++) {
            PrioritySectionInfo<T> section = sections[i];
            double healthyRate = section.sectionWeightInfo.healthyRate;
            if (factor == Double.MAX_VALUE && healthyRate > 0) {
                return i;
            }
            double finalHealthyRate = healthyRate * factor;
            if (finalHealthyRate >= 1.0) {
                return i;
            } else {
                if (sectionRandom < 0) {
                    sectionRandom = random.nextDouble();
                }
                if (sectionRandom < finalHealthyRate) {
                    return i;
                } else {
                    // regenerate a random value between 0 and 1 for next use
                    sectionRandom = (sectionRandom - finalHealthyRate) / (1.0 - finalHealthyRate);
                }
            }
        }
        return 0;
    }

    @Override
    public void close() {
        checkTask.close();
    }

    HashMap<T, ResInfo<T>> getResourcesMap() {
        return resourcesMap;
    }

    PrioritySectionInfo<T>[] getPrioritySections() {
        return prioritySections;
    }

    PriorityFailoverCheckTask<T> getCheckTask() {
        return checkTask;
    }
}
