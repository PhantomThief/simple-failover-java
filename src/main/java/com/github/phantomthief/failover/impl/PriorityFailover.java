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
    private final ArrayList<PrioritySectionInfo<T>> prioritySections;

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
        final ArrayList<ResInfo<T>> resources = new ArrayList<>();

        // the range is from 0 to 1.0
        volatile double healthyRate = 1.0;

        PrioritySectionInfo(int priority) {
            this.priority = priority;
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

    PriorityFailover(PriorityFailoverConfig<T> config) {
        Objects.requireNonNull(config.getResources());
        this.config = config;
        this.resourcesMap = new HashMap<>();
        TreeMap<Integer, PrioritySectionInfo<T>> priorityMap = new TreeMap<>();
        for (Entry<T, ResConfig> en : config.getResources().entrySet()) {
            T res = en.getKey();
            ResConfig rc = en.getValue();
            int priority = rc.getPriority();
            ResInfo<T> ri = new ResInfo<>(res, priority, rc.getMaxWeight(),
                    rc.getMinWeight(), rc.getInitWeight(), config.isConcurrencyControl());
            this.resourcesMap.put(res, ri);
            priorityMap.compute(priority, (k, v) -> {
                if (v == null) {
                    v = new PrioritySectionInfo<>(priority);
                }
                v.resources.add(ri);
                return v;
            });
        }
        prioritySections = new ArrayList<>();
        prioritySections.addAll(priorityMap.values());
        for (int i = 0; i < prioritySections.size(); i++) {
            updateSectionHealthy(i, prioritySections);
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
            ArrayList<PrioritySectionInfo<T>> sections) {
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

    static <T> void updateSectionHealthy(int priority, ArrayList<PrioritySectionInfo<T>> sections) {
        for (PrioritySectionInfo<T> psi : sections) {
            if (psi.priority == priority) {
                double sumMaxWeight = 0.0;
                double sumCurrentWeight = 0.0;
                for (ResInfo<T> ri : psi.resources) {
                    sumMaxWeight += ri.maxWeight;
                    sumCurrentWeight += ri.currentWeight;
                }
                psi.healthyRate = sumCurrentWeight / sumMaxWeight;
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
    public T getOneAvailableExclude(Collection<T> exclusions) {
        if (prioritySections.size() == 0) {
            return null;
        }
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        int preferSectionIndex = selectSection(threadLocalRandom);
        for (int i = 0; i < prioritySections.size(); i++) {
            ResInfo<T> ri = findOneInRegion(threadLocalRandom, preferSectionIndex, exclusions);
            if (ri != null) {
                if (ri.concurrency != null) {
                    ri.concurrency.incr();
                }
                return ri.resource;
            }
            preferSectionIndex = (preferSectionIndex + 1) % prioritySections.size();
        }
        return null;
    }

    private ResInfo<T> findOneInRegion(ThreadLocalRandom threadLocalRandom, int preferSectionIndex,
            Collection<T> exclusions) {
        PrioritySectionInfo<T> sectionInfo = prioritySections.get(preferSectionIndex);
        double sumWeight = 0;

        double[] weights = new double[sectionInfo.resources.size()];
        if (exclusions.size() == weights.length) {
            return null;
        }

        for (int i = 0; i < weights.length; i++) {
            ResInfo<T> ri = sectionInfo.resources.get(i);
            if (!exclusions.contains(ri.resource)) {
                double w = ri.currentWeight;
                if (ri.concurrency != null) {
                    int c = ri.concurrency.get();
                    w = w / (1.0 + c);
                }
                sumWeight += w;
                weights[i] = w;
            } else {
                weights[i] = -1;
            }
        }
        if (sumWeight <= 0) {
            return null;
        }
        double random = threadLocalRandom.nextDouble(sumWeight);
        double x = 0;
        for (int i = 0; i < weights.length; i++) {
            if (weights[i] < 0) {
                continue;
            } else {
                x += weights[i];
            }
            if (random < x) {
                return sectionInfo.resources.get(i);
            }
        }

        // something wrong or there are float precision problem,
        // return first one which is not excluded
        for (int i = 0; i < weights.length; i++) {
            if (weights[i] > 0) {
                ResInfo<T> resInfo = sectionInfo.resources.get(i);
                if (!exclusions.contains(resInfo.resource)) {
                    return resInfo;
                }
            }
        }

        // assert false;
        return null;
    }

    int selectSection(ThreadLocalRandom random) {
        if (prioritySections.size() == 1) {
            return 0;
        }
        double factor = config.getPriorityFactor();
        double sectionRandom = -1.0;
        for (int i = 0; i < prioritySections.size(); i++) {
            double healthyRate = prioritySections.get(i).healthyRate;
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

    ArrayList<PrioritySectionInfo<T>> getPrioritySections() {
        return prioritySections;
    }

    PriorityFailoverCheckTask<T> getCheckTask() {
        return checkTask;
    }
}
