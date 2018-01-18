package com.github.phantomthief.failover.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import com.github.phantomthief.util.ThrowableFunction;
import com.github.phantomthief.util.ThrowablePredicate;

/**
 * @author w.vela
 */
public class GenericWeightFailoverBuilder<E> {

    private final WeightFailoverBuilder<Object> builder;

    public GenericWeightFailoverBuilder(WeightFailoverBuilder<Object> builder) {
        this.builder = builder;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> name(String value) {
        builder.name(value);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> autoAddOnMissing(int weight) {
        builder.autoAddOnMissing(weight);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> failReduceRate(double rate) {
        builder.failReduceRate(rate);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> failReduce(int weight) {
        builder.failReduce(weight);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> onMinWeight(Consumer<E> listener) {
        builder.onMinWeight(listener);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> onRecovered(Consumer<E> listener) {
        builder.onRecovered(listener);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> successIncreaseRate(double rate) {
        builder.successIncreaseRate(rate);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> successIncrease(int weight) {
        builder.successIncrease(weight);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> minWeight(int weight) {
        builder.minWeight(weight);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> checkDuration(long time, TimeUnit unit) {
        builder.checkDuration(time, unit);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E>
            checker(@Nonnull ThrowableFunction<? super E, Double, Throwable> failChecker) {
        builder.checker(failChecker);
        return this;
    }

    @CheckReturnValue
    public GenericWeightFailoverBuilder<E> checker(
            @Nonnull ThrowablePredicate<? super E, Throwable> failChecker,
            @Nonnegative double recoveredInitRate) {
        builder.checker(failChecker, recoveredInitRate);
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
