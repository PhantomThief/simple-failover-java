package com.github.phantomthief.failover.impl;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author huangli
 * Created on 2020-05-06
 */
public abstract class AbstractWeightFunction<T> implements WeightFunction<T> {

    public static final int DEFAULT_RECOVER_THRESHOLD = 1;

    private final int recoverThreshold;

    private final ConcurrentHashMap<T, Integer> recoverCountMap;

    public AbstractWeightFunction() {
        this(DEFAULT_RECOVER_THRESHOLD);
    }

    public AbstractWeightFunction(int recoverThreshold) {
        if (recoverThreshold < 1) {
            throw new IllegalArgumentException("bad recoverThreshold:" + recoverThreshold);
        }
        this.recoverThreshold = recoverThreshold;
        if (recoverThreshold > 1) {
            recoverCountMap = new ConcurrentHashMap<>();
        } else {
            recoverCountMap = null;
        }
    }

    @Override
    public double success(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource) {
        if (recoverCountMap != null && currentOldWeight <= minWeight) {
            int count = recoverCountMap.compute(resource, (k, v) -> {
                if (v == null) {
                    return 1;
                } else {
                    return v + 1;
                }
            });
            if (count < recoverThreshold) {
                return currentOldWeight;
            }
        }

        double newWeight = computeSuccess(maxWeight, minWeight, priority, currentOldWeight, resource);

        if (recoverCountMap != null && currentOldWeight <= minWeight && newWeight > minWeight) {
            recoverCountMap.remove(resource);
        }
        return Math.min(newWeight, maxWeight);
    }

    protected abstract double computeSuccess(double maxWeight, double minWeight,
            int priority, double currentOldWeight, T resource);

    @Override
    public double fail(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource) {
        double newWeight = computeFail(maxWeight, minWeight, priority, currentOldWeight, resource);
        if (recoverCountMap != null && newWeight <= minWeight) {
            recoverCountMap.put(resource, 0);
        }
        return Math.max(newWeight, minWeight);
    }

    protected abstract double computeFail(double maxWeight, double minWeight,
            int priority, double currentOldWeight, T resource);

    @Override
    public boolean needCheck(double maxWeight, double minWeight, int priority, double currentWeight, T resource) {
        return currentWeight <= minWeight && currentWeight < maxWeight;
    }
}
