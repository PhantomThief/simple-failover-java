package com.github.phantomthief.failover.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.phantomthief.failover.backoff.BackOff;
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
    @Nonnull
    public GenericWeightFailoverBuilder<E> name(String value) {
        builder.name(value);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> autoAddOnMissing(int weight) {
        builder.autoAddOnMissing(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> failReduceRate(double rate) {
        builder.failReduceRate(rate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> failReduce(int weight) {
        builder.failReduce(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> onMinWeight(Consumer<E> listener) {
        builder.onMinWeight(listener);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> onRecovered(Consumer<E> listener) {
        builder.onRecovered(listener);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> successIncreaseRate(double rate) {
        builder.successIncreaseRate(rate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> successIncrease(int weight) {
        builder.successIncrease(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> minWeight(int weight) {
        builder.minWeight(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> checkDuration(long time, TimeUnit unit) {
        builder.checkDuration(time, unit);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> checker(
            @Nonnull ThrowableFunction<? super E, Double, Throwable> failChecker) {
        builder.checker(failChecker);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> checker(
            @Nonnull ThrowableFunction<? super E, Double, Throwable> failChecker,
            @Nullable BackOff backOff) {
        builder.checker(failChecker, backOff);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> checker(
            @Nonnull ThrowablePredicate<? super E, Throwable> failChecker,
            @Nonnegative double recoveredInitRate) {
        builder.checker(failChecker, recoveredInitRate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> checker(
            @Nonnull ThrowablePredicate<? super E, Throwable> failChecker,
            @Nonnegative double recoveredInitRate,
            @Nullable BackOff backOff) {
        builder.checker(failChecker, recoveredInitRate, backOff);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericWeightFailoverBuilder<E> filter(Predicate<E> filter) {
        builder.filter(filter);
        return this;
    }

    @Nonnull
    public WeightFailover<E> build(Collection<? extends E> original) {
        return builder.build(original);
    }

    @Nonnull
    public WeightFailover<E> build(Collection<? extends E> original, int initWeight) {
        return builder.build(original, initWeight);
    }

    @Nonnull
    public WeightFailover<E> build(Map<? extends E, Integer> original) {
        return builder.build(original);
    }
}
