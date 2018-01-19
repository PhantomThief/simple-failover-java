package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.ToDoubleFunction;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableFunction;
import com.github.phantomthief.util.ThrowablePredicate;

public class WeightFailoverBuilder<T> {

    private static final Logger logger = getLogger(WeightFailoverBuilder.class);

    private static final int DEFAULT_INIT_WEIGHT = 100;
    private static final int DEFAULT_FAIL_REDUCE_WEIGHT = 5;
    private static final int DEFAULT_SUCCESS_INCREASE_WEIGHT = 1;
    private static final long DEFAULT_CHECK_DURATION = SECONDS.toMillis(1);

    IntUnaryOperator failReduceWeight;
    IntUnaryOperator successIncreaseWeight;

    Map<T, Integer> initWeightMap;
    ToDoubleFunction<T> checker;
    long checkDuration;
    Consumer<T> onMinWeight;
    Consumer<T> onRecovered;
    int minWeight = 0;
    Integer weightOnMissingNode;
    String name;

    @CheckReturnValue
    public <E> WeightFailoverBuilder<E> name(String value) {
        this.name = value;
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        return thisBuilder;
    }

    @CheckReturnValue
    public <E> WeightFailoverBuilder<E> autoAddOnMissing(int weight) {
        checkArgument(weight >= 0);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        this.weightOnMissingNode = weight;
        return thisBuilder;
    }

    @CheckReturnValue
    @SuppressWarnings("unchecked")
    public <E> WeightFailoverBuilder<E> onMinWeight(Consumer<E> listener) {
        checkNotNull(listener);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.onMinWeight = listener;
        return thisBuilder;
    }

    @CheckReturnValue
    @SuppressWarnings("unchecked")
    public <E> WeightFailoverBuilder<E> onRecovered(Consumer<E> listener) {
        checkNotNull(listener);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.onRecovered = listener;
        return thisBuilder;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> minWeight(int value) {
        checkArgument(value >= 0);
        this.minWeight = value;
        return this;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> failReduceRate(double rate) {
        checkArgument(rate > 0 && rate <= 1);
        failReduceWeight = i -> Math.max(1, (int) (rate * i));
        return this;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> failReduce(int weight) {
        checkArgument(weight > 0);
        failReduceWeight = i -> weight;
        return this;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> successIncreaseRate(double rate) {
        checkArgument(rate > 0 && rate <= 1);
        successIncreaseWeight = i -> Math.max(1, (int) (rate * i));
        return this;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> successIncrease(int weight) {
        checkArgument(weight > 0);
        successIncreaseWeight = i -> weight;
        return this;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> checkDuration(long time, TimeUnit unit) {
        checkNotNull(unit);
        checkArgument(time > 0);
        checkDuration = unit.toMillis(time);
        return this;
    }

    @SuppressWarnings("unchecked")
    @CheckReturnValue
    public <E> WeightFailoverBuilder<E>
            checker(@Nonnull ThrowableFunction<? super E, Double, Throwable> failChecker) {
        checkNotNull(failChecker);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.checker = t -> {
            try {
                return failChecker.apply(t);
            } catch (Throwable e) {
                logger.error("", e);
                return 0;
            }
        };
        return thisBuilder;
    }

    @SuppressWarnings("unchecked")
    @CheckReturnValue
    public <E> WeightFailoverBuilder<E> checker(
            @Nonnull ThrowablePredicate<? super E, Throwable> failChecker,
            @Nonnegative double recoveredInitRate) {
        checkArgument(recoveredInitRate >= 0 && recoveredInitRate <= 1);
        checkNotNull(failChecker);
        return checker(it -> failChecker.test(it) ? recoveredInitRate : 0);
    }

    public <E> WeightFailover<E> build(Collection<? extends E> original) {
        return build(original, DEFAULT_INIT_WEIGHT);
    }

    public <E> WeightFailover<E> build(Collection<? extends E> original, int initWeight) {
        checkNotNull(original);
        checkArgument(initWeight > 0);
        return build(original.stream().collect(toMap(identity(), i -> initWeight, (u, v) -> u)));
    }

    @SuppressWarnings("unchecked")
    public <E> WeightFailover<E> build(Map<? extends E, Integer> original) {
        checkNotNull(original);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.initWeightMap = (Map<E, Integer>) original;
        return thisBuilder.build();
    }

    private WeightFailover<T> build() {
        ensure();
        return new WeightFailover<>(this);
    }

    private void ensure() {
        if (minWeight <= 0) { // if min weight>0, there is no checker need.
            checkNotNull(checker);
        } else {
            if (checker != null) {
                logger.warn(
                        "a failover checker found but minWeight>0. the checker would never reached.");
            }
        }
        if (failReduceWeight == null) {
            failReduceWeight = i -> DEFAULT_FAIL_REDUCE_WEIGHT;
        }
        if (successIncreaseWeight == null) {
            successIncreaseWeight = i -> DEFAULT_SUCCESS_INCREASE_WEIGHT;
        }
        if (checkDuration == 0) {
            checkDuration = DEFAULT_CHECK_DURATION;
        }
    }
}