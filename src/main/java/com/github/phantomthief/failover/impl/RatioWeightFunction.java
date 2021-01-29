package com.github.phantomthief.failover.impl;

/**
 * 这个函数等比递减权重，在失败的时候可以更加迅速的扣减权重。
 * 比如默认情况下，假设初始权重为1，失败一次后剩0.5，再失败一次剩0.25，连续失败7次会down
 * （依据downThresholdRateOfMaxWeight，默认值0.01）。
 *
 *  @author huangli
 * Created on 2020-05-06
 */
public class RatioWeightFunction<T> extends AbstractWeightFunction<T> {

    private static final double DEFAULT_FAIL_DECREASE_RATE = 0.5;
    private static final double DEFAULT_SUCCESS_INCREASE_RATE = 0.01;
    private static final double DEFAULT_RECOVER_RATE = 0.1;
    private static final double DEFAULT_DOWN_THRESHOLD_RATE = 0.01;

    private final double failKeepRateOfCurrentWeight;
    private final double successIncreaseRateOfMaxWeight;
    private final double downThreshold;

    private double downThresholdRateOfMaxWeight = DEFAULT_DOWN_THRESHOLD_RATE;
    private double recoverRateOfMaxWeight = DEFAULT_RECOVER_RATE;

    /**
     * 使用默认failKeepRateOfCurrentWeight为0.5，successIncreaseRateOfMaxWeight为0.01，
     * recoverThreshold为1，downThreshold为0。
     */
    public RatioWeightFunction() {
        this(DEFAULT_FAIL_DECREASE_RATE, DEFAULT_SUCCESS_INCREASE_RATE);
    }

    /**
     * 使用默认recoverThreshold为1，downThreshold为0。
     * @param failKeepRateOfCurrentWeight
     * @param successIncreaseRateOfMaxWeight
     */
    public RatioWeightFunction(double failKeepRateOfCurrentWeight, double successIncreaseRateOfMaxWeight) {
        this(failKeepRateOfCurrentWeight, successIncreaseRateOfMaxWeight, DEFAULT_RECOVER_THRESHOLD);
    }

    /**
     * 使用默认downThreshold为0。
     * @param failKeepRateOfCurrentWeight 失败以后保留的权重比例（相对于当前权重）
     * @param successIncreaseRateOfMaxWeight 成功以后增加权重的比例（相对于最大权重）
     * @param recoverThreshold 探活的时候，几次探活成功开始增加权重
     */
    public RatioWeightFunction(double failKeepRateOfCurrentWeight, double successIncreaseRateOfMaxWeight,
            int recoverThreshold) {
        this(failKeepRateOfCurrentWeight, successIncreaseRateOfMaxWeight, recoverThreshold, 0);
    }

    /**
     * 构造一个实例，指定4个参数。
     * @param failKeepRateOfCurrentWeight 失败以后扣减权重的比例（相对于当前权重）
     * @param successIncreaseRateOfMaxWeight 成功以后增加权重的比例（相对于最大权重）
     * @param recoverThreshold 探活的时候，几次探活成功开始增加权重
     * @param downThreshold 权重低于多少直接down（设置为最小权重），这是为了防止等比递减总是减不到0
     */
    public RatioWeightFunction(double failKeepRateOfCurrentWeight, double successIncreaseRateOfMaxWeight,
            int recoverThreshold, double downThreshold) {
        super(recoverThreshold);
        if (failKeepRateOfCurrentWeight < 0 || failKeepRateOfCurrentWeight > 1) {
            throw new IllegalArgumentException(
                    "bad failDecreaseRateOfCurrentWeight:" + failKeepRateOfCurrentWeight);
        }
        if (successIncreaseRateOfMaxWeight < 0 || successIncreaseRateOfMaxWeight > 1) {
            throw new IllegalArgumentException("bad successIncreaseRateOfMaxWeight:" + successIncreaseRateOfMaxWeight);
        }
        if (downThreshold < 0) {
            throw new IllegalArgumentException("bad downThreshold:" + downThreshold);
        }
        this.failKeepRateOfCurrentWeight = failKeepRateOfCurrentWeight;
        this.successIncreaseRateOfMaxWeight = successIncreaseRateOfMaxWeight;
        this.downThreshold = downThreshold;
    }

    @Override
    protected double computeSuccess(double maxWeight, double minWeight, int priority, double currentOldWeight,
            T resource) {
        if (currentOldWeight > minWeight) {
            return currentOldWeight + maxWeight * successIncreaseRateOfMaxWeight;
        } else {
            return currentOldWeight + maxWeight * recoverRateOfMaxWeight;
        }
    }

    @Override
    protected double computeFail(double maxWeight, double minWeight, int priority, double currentOldWeight,
            T resource) {
        double x = currentOldWeight * failKeepRateOfCurrentWeight;
        if (downThreshold == 0) {
            return x < maxWeight * downThresholdRateOfMaxWeight ? minWeight : x;
        } else {
            return x < downThreshold ? minWeight : x;
        }
    }

    public void setDownThresholdRateOfMaxWeight(double downThresholdRateOfMaxWeight) {
        this.downThresholdRateOfMaxWeight = downThresholdRateOfMaxWeight;
    }

    public void setRecoverRateOfMaxWeight(double recoverRateOfMaxWeight) {
        this.recoverRateOfMaxWeight = recoverRateOfMaxWeight;
    }
}
