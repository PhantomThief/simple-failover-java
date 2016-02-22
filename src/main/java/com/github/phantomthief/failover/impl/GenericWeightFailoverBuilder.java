/**
 * 
 */
package com.github.phantomthief.failover.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * @author w.vela
 */
public class GenericWeightFailoverBuilder<E> {

    private final WeightFailoverBuilder<Object> builder;

    GenericWeightFailoverBuilder(WeightFailoverBuilder<Object> builder) {
        this.builder = builder;
    }

    public GenericWeightFailoverBuilder<E> failReduce(int weight) {
        builder.failReduce(weight);
        return this;
    }

    public GenericWeightFailoverBuilder<E> successIncrease(int weight) {
        builder.successIncrease(weight);
        return this;
    }

    public GenericWeightFailoverBuilder<E> recoveiedInit(int weight) {
        builder.recoveiedInit(weight);
        return this;
    }

    public GenericWeightFailoverBuilder<E> checkDuration(long time, TimeUnit unit) {
        builder.checkDuration(time, unit);
        return this;
    }

    public GenericWeightFailoverBuilder<E> checker(Predicate<? super E> failChecker) {
        builder.checker(failChecker);
        return this;
    }

    public WeightFailover<E> build(Collection<? extends E> original) {
        return builder.build(original);
    }

    public WeightFailover<E> build(Collection<? extends E> original, int initWeight) {
        return builder.build(original, initWeight);
    }

    public WeightFailover<E> build(Map<? extends E, Integer> original) {
        return builder.build(original);
    }

}
