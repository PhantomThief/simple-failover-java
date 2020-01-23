package com.github.phantomthief.failover.impl;

/**
 * @author huangli
 * Created on 2020-01-20
 */
public class SimpleWeightFunction<T> implements WeightFunction<T> {

    private double failDecreaseRate = 0.05;
    private double successIncreaseRate = 0.01;

    public SimpleWeightFunction() {
    }

    public SimpleWeightFunction(double failDecreaseRate, double successIncreaseRate) {
        this.failDecreaseRate = failDecreaseRate;
        this.successIncreaseRate = successIncreaseRate;
    }

    @Override
    public double success(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource) {
        return currentOldWeight + maxWeight * successIncreaseRate;
    }

    @Override
    public double fail(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource) {
        return currentOldWeight - maxWeight * failDecreaseRate;
    }

    @Override
    public boolean needCheck(double maxWeight, double minWeight, int priority, double currentWeight, T resource) {
        return currentWeight <= minWeight;
    }
}
