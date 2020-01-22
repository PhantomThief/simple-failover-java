package com.github.phantomthief.failover.impl;

/**
 * @author huangli
 * Created on 2020-01-16
 */
public interface WeightFunction<T> {

    double success(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource);

    double fail(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource);

    boolean needCheck(double maxWeight, double minWeight, int priority, double currentWeight, T resource);
}
