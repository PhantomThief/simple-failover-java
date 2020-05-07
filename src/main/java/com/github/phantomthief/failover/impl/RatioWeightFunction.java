package com.github.phantomthief.failover.impl;

/**
 * @author huangli
 * Created on 2020-05-06
 */
public class RatioWeightFunction<T> extends AbstractWeightFunction<T> {

    private static final double DEFAULT_FAIL_DECREASE_RATE = 0.5;
    private static final double DEFAULT_SUCCESS_INCREASE_RATE = 0.01;

    private final double failDecreaseRateOfCurrentWeight;
    private final double successIncreaseRateOfMaxWeight;
    private final double downThreshold;

    public RatioWeightFunction() {
        this(DEFAULT_FAIL_DECREASE_RATE, DEFAULT_SUCCESS_INCREASE_RATE);
    }

    public RatioWeightFunction(double failDecreaseRateOfCurrentWeight, double successIncreaseRateOfMaxWeight) {
        this(failDecreaseRateOfCurrentWeight, successIncreaseRateOfMaxWeight, DEFAULT_RECOVER_THRESHOLD);
    }

    public RatioWeightFunction(double failDecreaseRateOfCurrentWeight, double successIncreaseRateOfMaxWeight,
            int recoverThreshold) {
        this(failDecreaseRateOfCurrentWeight, successIncreaseRateOfMaxWeight, recoverThreshold, 0);
    }

    public RatioWeightFunction(double failDecreaseRateOfCurrentWeight, double successIncreaseRateOfMaxWeight,
            int recoverThreshold, double downThreshold) {
        super(recoverThreshold);
        this.failDecreaseRateOfCurrentWeight = failDecreaseRateOfCurrentWeight;
        this.successIncreaseRateOfMaxWeight = successIncreaseRateOfMaxWeight;
        this.downThreshold = downThreshold;
    }

    @Override
    protected double computeSuccess(double maxWeight, double minWeight, int priority, double currentOldWeight,
            T resource) {
        return currentOldWeight + maxWeight * successIncreaseRateOfMaxWeight;
    }

    @Override
    protected double computeFail(double maxWeight, double minWeight, int priority, double currentOldWeight,
            T resource) {
        double x = currentOldWeight - currentOldWeight * failDecreaseRateOfCurrentWeight;
        return x < downThreshold ? minWeight : x;
    }
}
