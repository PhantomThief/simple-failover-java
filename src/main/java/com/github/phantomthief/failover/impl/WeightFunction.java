package com.github.phantomthief.failover.impl;

/**
 * 权重控制的回调函数，如果你需要定制权重增减算法，或者定制健康检查的时机，就需要用到这个接口。
 * 如果没有太多要求用默认的就可以，{@link PriorityFailover}默认内置一个{@link RatioWeightFunction}。
 *
 * @author huangli
 * Created on 2020-01-16
 */
public interface WeightFunction<T> {

    /**
     * 一个资源访问成功后，会调用本方法，计算新的权重。
     * Failover内部可能会优化，在当前权重已经等于最大权重的情况下，
     * 继续调用{@link com.github.phantomthief.failover.Failover#success(Object)}方法，
     * 不会导致本接口的本方法被调用。
     *
     * @param maxWeight 这个资源的最大权重
     * @param minWeight 这个资源的最小权重
     * @param priority 这个资源优先级
     * @param currentOldWeight 这个资源的当前权重（计算前）
     * @param resource 这个资源
     * @return 新的权重
     */
    double success(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource);

    /**
     * 一个资源访问失败后，会调用本方法，计算新的权重。
     * Failover内部可能会优化，在当前权重已经等于最小权重的情况下，
     * 继续调用{@link com.github.phantomthief.failover.Failover#fail(Object)}方法，
     * 不会导致本接口的本方法被调用。
     *
     * @param maxWeight 这个资源的最大权重
     * @param minWeight 这个资源的最小权重
     * @param priority 这个资源优先级
     * @param currentOldWeight 这个资源的当前权重（计算前）
     * @param resource 这个资源
     * @return 新的权重
     */
    double fail(double maxWeight, double minWeight, int priority, double currentOldWeight, T resource);

    /**
     * 用来控制一个资源是否需要健康检查，健康检查程序运行时，针对每个资源，逐个调用本方法，如果本方法返回true那么就对相关资源进行
     * 健康检查。
     *
     * @param maxWeight 这个资源的最大权重
     * @param minWeight 这个资源的最小权重
     * @param priority 这个资源优先级
     * @param currentWeight 当前权重
     * @param resource 这个资源
     * @return 是否需要健康检查
     */
    boolean needCheck(double maxWeight, double minWeight, int priority, double currentWeight, T resource);
}
