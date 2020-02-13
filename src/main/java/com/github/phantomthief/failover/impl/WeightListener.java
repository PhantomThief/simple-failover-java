package com.github.phantomthief.failover.impl;

/**
 * @author huangli
 * Created on 2020-01-20
 */
public interface WeightListener<T> {
    default void onSuccess(double maxWeight, double minWeight, int priority, double currentOldWeight,
            double currentNewWeight, T resource) {
    }

    default void onFail(double maxWeight, double minWeight, int priority, double currentOldWeight,
            double currentNewWeight, T resource) {
    }
}
