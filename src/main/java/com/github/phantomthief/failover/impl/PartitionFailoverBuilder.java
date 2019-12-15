package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import com.github.phantomthief.util.ThrowableFunction;
import com.github.phantomthief.util.ThrowablePredicate;

@SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:HiddenField"})
public class PartitionFailoverBuilder<T> {

    private WeightFailoverBuilder<T> weightFailoverBuilder = new WeightFailoverBuilder<>();

    int corePartitionSize;

    long maxExternalPoolIdleMillis;

    public static <T> PartitionFailoverBuilder<T> newBuilder() {
        return new PartitionFailoverBuilder<>();
    }

    @Nonnull
    public PartitionFailover<T> build(Collection<T> original) {
        checkNotNull(original);
        ensure(original.size());
        WeightFailover<T> weightFailover = weightFailoverBuilder.build(original);
        return new PartitionFailover<>(this, weightFailover);
    }

    @Nonnull
    public PartitionFailover<T> build(Collection<T> original, int initWeight) {
        checkNotNull(original);
        ensure(original.size());
        WeightFailover<T> weightFailover = weightFailoverBuilder.build(original, initWeight);
        return new PartitionFailover<>(this, weightFailover);
    }

    @Nonnull
    public PartitionFailover<T> build(Map<T, Integer> original) {
        checkNotNull(original);
        ensure(original.size());
        WeightFailover<T> weightFailover = weightFailoverBuilder.build(original);
        return new PartitionFailover<>(this, weightFailover);
    }

    private void ensure(int allResourceCount) {
        checkArgument(corePartitionSize > 0, "corePartitionSize has to be positive");
        checkArgument(corePartitionSize <= allResourceCount, "corePartitionSize should less or equal than size of original");
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> corePartitionSize(int corePartitionSize) {
        this.corePartitionSize = corePartitionSize;
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> reuseRecentResource(long maxExternalPoolIdleMillis) {
        this.maxExternalPoolIdleMillis = maxExternalPoolIdleMillis;
        return this;
    }

    //-------------------------methods delegate to weightFailoverBuilder below---------------------

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> name(String value) {
        weightFailoverBuilder.name(value);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> autoAddOnMissing(int weight) {
        weightFailoverBuilder.autoAddOnMissing(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> onMinWeight(Consumer<T> listener) {
        weightFailoverBuilder.onMinWeight(listener);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> onRecovered(Consumer<T> listener) {
        weightFailoverBuilder.onRecovered(listener);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> minWeight(int value) {
        weightFailoverBuilder.minWeight(value);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> failReduceRate(double rate) {
        weightFailoverBuilder.failReduceRate(rate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> failReduce(int weight) {
        weightFailoverBuilder.failReduce(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> successIncreaseRate(double rate) {
        weightFailoverBuilder.successIncreaseRate(rate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> successIncrease(int weight) {
        weightFailoverBuilder.successIncrease(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> checkDuration(long time, TimeUnit unit) {
        weightFailoverBuilder.checkDuration(time, unit);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> filter(@Nonnull Predicate<T> filter) {
        weightFailoverBuilder.filter(filter);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T>
            checker(@Nonnull ThrowableFunction<? super T, Double, Throwable> failChecker) {
        weightFailoverBuilder.checker(failChecker);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> checker(
            @Nonnull ThrowablePredicate<? super T, Throwable> failChecker,
            @Nonnegative double recoveredInitRate) {
        weightFailoverBuilder.checker(failChecker, recoveredInitRate);
        return this;
    }

}