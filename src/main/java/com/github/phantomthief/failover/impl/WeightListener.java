package com.github.phantomthief.failover.impl;

/**
 * 使用者可以注册权重监听器，用来监听特定的变化事件，比如资源调用失败，或者权重降低到最小值。
 *
 * @author huangli
 * Created on 2020-01-20
 */
public interface WeightListener<T> {
    /**
     * 资源访问成功时被调用，当然，failover本身并不知道资源调用是否成功，它实际上是使用者调用
     * {@link com.github.phantomthief.failover.Failover#success(Object)}后触发的。
     *
     * 当权重没有变化的时候可能不会被调用，比如当前权重=最大权重的情况下，再次调用success。
     *
     * @param maxWeight 最大权重
     * @param minWeight 最小权重
     * @param priority 优先级
     * @param currentOldWeight 调用成功前的权重
     * @param currentNewWeight 调用成功后的权重
     * @param resource 相关资源
     */
    default void onSuccess(double maxWeight, double minWeight, int priority, double currentOldWeight,
            double currentNewWeight, T resource) {
    }

    /**
     * 资源访问失败时被调用，当然，failover本身并不知道资源调用是否失败，它实际上是使用者调用
     * {@link com.github.phantomthief.failover.Failover#fail(Object)}和
     * {@link com.github.phantomthief.failover.Failover#down(Object)}后触发的。
     *
     * 当权重没有变化的时候可能不会被调用，比如当前权重=最小权重的情况下，再次调用fail。
     *
     * @param maxWeight 最大权重
     * @param minWeight 最小权重
     * @param priority 优先级
     * @param currentOldWeight 调用失败前的权重
     * @param currentNewWeight 调用失败后的权重
     * @param resource 相关资源
     */
    default void onFail(double maxWeight, double minWeight, int priority, double currentOldWeight,
            double currentNewWeight, T resource) {
    }
}
