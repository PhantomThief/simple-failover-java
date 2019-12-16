package com.github.phantomthief.failover.impl;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.phantomthief.failover.Failover;

/**
 * @author huangli
 * Created on 2019-12-10
 */
public class PartitionFailover<T> implements Failover<T>, Closeable {

    private final WeightFailover<T> weightFailover;
    private final long maxExternalPoolIdleMillis;
    private final int totalResourceSize;
    private volatile ResEntry<T>[] resources;

    @SuppressWarnings("checkstyle:VisibilityModifier")
    private static class ResEntry<T> {
        final T object;
        final int initWeight;

        volatile long lastReturnNanoTime;
        AtomicInteger concurrency;

        ResEntry(T object, int initWeight, int initConcurrency) {
            this.object = object;
            this.initWeight = initWeight;
            this.concurrency = new AtomicInteger(initConcurrency);
        }

    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    private static class ResEntryEx<T> extends ResEntry<T> {
        double scoreWeight;

        ResEntryEx(T object, int initWeight, int initConcurrency) {
            super(object, initWeight, initConcurrency);
        }
    }

    @SuppressWarnings("unchecked")
    PartitionFailover(PartitionFailoverBuilder<T> partitionFailoverBuilder,
            WeightFailover<T> weightFailover) {
        this.weightFailover = weightFailover;
        this.totalResourceSize = weightFailover.getAll().size();
        this.maxExternalPoolIdleMillis = partitionFailoverBuilder.maxExternalPoolIdleMillis;
        int corePartitionSize = partitionFailoverBuilder.corePartitionSize;
        List<T> available = weightFailover.getAvailable(corePartitionSize);
        this.resources = new ResEntry[corePartitionSize];
        for (int i = 0; i < corePartitionSize; i++) {
            T one = available.get(i);
            resources[i] = new ResEntry<>(one, weightFailover.initWeight(one), 0);
        }
    }

    public static <T> PartitionFailoverBuilder<T> newBuilder() {
        return new PartitionFailoverBuilder<>();
    }

    private ResEntryEx<T>[] deepCopyResource() {
        ResEntry<T>[] refCopy = resources;
        @SuppressWarnings("unchecked")
        ResEntryEx<T>[] copy = new ResEntryEx[refCopy.length];
        for (int i = 0; i < refCopy.length; i++) {
            ResEntry<T> res = refCopy[i];
            ResEntryEx<T> resEx = new ResEntryEx<>(res.object, res.initWeight, res.concurrency.get());
            resEx.lastReturnNanoTime = res.lastReturnNanoTime;
            copy[i] = resEx;
        }
        return copy;
    }

    @Override
    public List<T> getAll() {
        return weightFailover.getAll();
    }

    @Override
    public void fail(@Nonnull T object) {
        weightFailover.fail(object);
        subtractConcurrency(object);
        if (weightFailover.currentWeight(object) <= 0) {
            replaceDownResource(object);
        }
    }

    @Override
    public void down(@Nonnull T object) {
        weightFailover.down(object);
        subtractConcurrency(object);
        replaceDownResource(object);
    }

    @Override
    public void success(@Nonnull T object) {
        weightFailover.success(object);
        subtractConcurrency(object);
    }

    @Nullable
    private ResEntry<T> lookup(Object object) {
        ResEntry<T>[] refCopy = resources;
        for (ResEntry<T> res : refCopy) {
            if (res.object == object) {
                return res;
            }
        }
        return null;
    }

    private void subtractConcurrency(@Nonnull T object) {
        ResEntry<T> resEntry = lookup(object);
        if (resEntry == null) {
            return;
        }
        resEntry.lastReturnNanoTime = System.nanoTime();
        resEntry.concurrency.updateAndGet(oldValue -> Math.max(oldValue - 1, 0));
    }

    private void addConcurrency(@Nonnull T object) {
        ResEntry<T> resEntry = lookup(object);
        if (resEntry == null) {
            return;
        }
        resEntry.lastReturnNanoTime = System.nanoTime();
        resEntry.concurrency.updateAndGet(oldValue -> Math.max(oldValue + 1, 1));
    }

    private synchronized void replaceDownResource(T object) {
        ResEntry<T>[] resourceRefCopy = resources;
        if (resourceRefCopy.length == totalResourceSize) {
            // so there is no more resource in weightFailover
            return;
        }
        @SuppressWarnings("unchecked")
        ResEntry<T>[] newList = new ResEntry[resourceRefCopy.length];
        int index = -1;
        for (int i = 0; i < resourceRefCopy.length; i++) {
            newList[i] = resourceRefCopy[i];
            if (newList[i].object == object) {
                index = i;
            }
        }
        if (index == -1) {
            // maybe replaced by another thread
            return;
        }
        List<T> excludes = Stream.of(resourceRefCopy).map(r -> r.object).collect(toList());
        T newOne = weightFailover.getOneAvailableExclude(excludes);
        if (newOne == null) {
            //no more available
            return;
        }
        newList[index] = new ResEntry<>(newOne, weightFailover.initWeight(newOne), 0);
        resources = newList;
    }


    @Nullable
    @Override
    public T getOneAvailable() {
        return getOneAvailableExclude(Collections.emptyList());
    }

    @Nullable
    @Override
    public T getOneAvailableExclude(Collection<T> exclusions) {
        // we use recent resource when:
        // 1, tps of caller is slow (all concurrency is 0)
        boolean noCallInProgress = true;
        // 2, all resources are healthy (all currentWeight == initWeight)
        boolean allResIsHealthy = true;
        // 3, at least there is one call returned in recent
        boolean hasRecentReturnedCall = false;
        final long nowNanoTime = System.nanoTime();
        final ResEntryEx<T>[] resourcesCopy = deepCopyResource();

        double sumOfScoreWeight = 0;
        int recentestIndex = 0;
        long maxTime = 0;

        for (int i = 0; i < resourcesCopy.length; i++) {
            ResEntryEx<T> res = resourcesCopy[i];
            int currentWeight = weightFailover.currentWeight(res.object);
            res.scoreWeight = 1.0 * currentWeight / (res.concurrency.get() + 1);
            if (res.scoreWeight < 0) {
                // something wrong
                res.scoreWeight = 0;
            }
            if (!exclusions.contains(res.object)) {
                sumOfScoreWeight += res.scoreWeight;
                if (maxTime < res.lastReturnNanoTime) {
                    maxTime = res.lastReturnNanoTime;
                    recentestIndex = i;
                }
            }
            if (res.concurrency.get() > 0) {
                noCallInProgress = false;
            }
            if (currentWeight != res.initWeight) {
                allResIsHealthy = false;
            }
            long elapseMillis = (nowNanoTime - res.lastReturnNanoTime) / (1000 * 1000);
            if (elapseMillis >= 0 && elapseMillis < maxExternalPoolIdleMillis) {
                hasRecentReturnedCall = true;
            }
        }
        T one;
        if (maxExternalPoolIdleMillis > 0 && noCallInProgress && allResIsHealthy && hasRecentReturnedCall) {
            one = resourcesCopy[recentestIndex].object;
        } else {
            one = selectByScore(resourcesCopy, exclusions, sumOfScoreWeight);
        }
        if (one != null) {
            addConcurrency(one);
        }
        return one;
    }

    private static <T> T selectByScore(ResEntryEx<T>[] resourcesCopy, Collection<T> exclusions, double sumOfScoreWeight) {
        if (sumOfScoreWeight <= 0) {
            // all down
            return null;
        }
        double selectValue = ThreadLocalRandom.current().nextDouble(sumOfScoreWeight);
        double x = 0;
        for (ResEntryEx<T> res : resourcesCopy) {
            if (exclusions.contains(res.object)) {
                continue;
            }
            x += res.scoreWeight;
            if (selectValue < x) {
                return res.object;
            }
        }
        // something wrong or there are float precision problem
        return resourcesCopy[0].object;
    }

    @Override
    public List<T> getAvailable() {
        ResEntry<T>[] resourceRefCopy = resources;
        if (resourceRefCopy.length == totalResourceSize) {
            return weightFailover.getAvailable();
        } else {
            return Stream.of(resourceRefCopy)
                    .filter(r -> weightFailover.currentWeight(r.object) > 0)
                    .map(r -> r.object)
                    .collect(collectingAndThen(toList(), Collections::unmodifiableList));
        }
    }

    @Override
    public Set<T> getFailed() {
        return weightFailover.getFailed();
    }

    @Override
    public void close() {
        weightFailover.close();
    }

    @Override
    public List<T> getAvailable(int n) {
        // we don't know which resource is used, so this method is not supported
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> getAvailableExclude(Collection<T> exclusions) {
        // we don't know which resource is used, so this method is not supported
        throw new UnsupportedOperationException();
    }
}
