package com.github.phantomthief.failover.impl;

/**
 * @author huangli
 * Created on 2020-01-20
 */
public class SimpleWeightFunction<T> extends AbstractWeightFunction<T> {

    private static final double DEFAULT_FAIL_DECREASE_RATE = 0.05;
    private static final double DEFAULT_SUCCESS_INCREASE_RATE = 0.01;

    private final double failDecreaseRate;
    private final double successIncreaseRate;

    public SimpleWeightFunction() {
        this(DEFAULT_FAIL_DECREASE_RATE, DEFAULT_SUCCESS_INCREASE_RATE);
    }

    public SimpleWeightFunction(double failDecreaseRate, double successIncreaseRate) {
        this(failDecreaseRate, successIncreaseRate, DEFAULT_RECOVER_THRESHOLD);
    }

    public SimpleWeightFunction(double failDecreaseRate, double successIncreaseRate, int recoverThreshold) {
        super(recoverThreshold);
        this.failDecreaseRate = failDecreaseRate;
        this.successIncreaseRate = successIncreaseRate;
    }

    @Override
    public double computeSuccess(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource) {
        return currentOldWeight + maxWeight * successIncreaseRate;
    }

    @Override
    public double computeFail(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource) {
        return currentOldWeight - maxWeight * failDecreaseRate;
    }

}
