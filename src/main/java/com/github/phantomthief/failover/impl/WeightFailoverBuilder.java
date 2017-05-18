/**
 * 
 */
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
import java.util.function.Predicate;

import javax.annotation.CheckReturnValue;

import org.slf4j.Logger;

public class WeightFailoverBuilder<T> {

    private static final Logger logger = getLogger(WeightFailoverBuilder.class);

    private static final int DEFAULT_INIT_WEIGHT = 100;
    private static final int DEFAULT_FAIL_REDUCE_WEIGHT = 5;
    private static final int DEFAULT_SUCCESS_INCREASE_WEIGHT = 1;
    private static final int DEFAULT_RECOVERED_INIT_WEIGHT = 1;
    private static final long DEFAULT_CHECK_DURATION = SECONDS.toMillis(1);

    private IntUnaryOperator failReduceWeight;
    private IntUnaryOperator successIncreaseWeight;
    private IntUnaryOperator recoveredInitWeight;

    private Map<T, Integer> initWeightMap;
    private Predicate<T> checker;
    private long checkDuration;
    private Consumer<T> onMinWeight;
    private Consumer<T> onRecovered;
    private int minWeight = 0;

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
    public WeightFailoverBuilder<T> recoveredInitRate(double rate) {
        checkArgument(rate > 0 && rate <= 1);
        recoveredInitWeight = i -> Math.max(1, (int) (rate * i));
        return this;
    }

    @CheckReturnValue
    public WeightFailoverBuilder<T> recoveredInit(int weight) {
        checkArgument(weight > 0);
        recoveredInitWeight = i -> weight;
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
    public <E> WeightFailoverBuilder<E> checker(Predicate<? super E> failChecker) {
        checkNotNull(failChecker);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.checker = t -> {
            try {
                return failChecker.test(t);
            } catch (Throwable e) {
                logger.error("", e);
                return false;
            }
        };
        return thisBuilder;
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
        return new WeightFailover<>(failReduceWeight, successIncreaseWeight, recoveredInitWeight,
                initWeightMap, minWeight, checkDuration, checker, onMinWeight, onRecovered);
    }

    private void ensure() {
        checkNotNull(checker);
        if (failReduceWeight == null) {
            failReduceWeight = i -> DEFAULT_FAIL_REDUCE_WEIGHT;
        }
        if (successIncreaseWeight == null) {
            successIncreaseWeight = i -> DEFAULT_SUCCESS_INCREASE_WEIGHT;
        }
        if (recoveredInitWeight == null) {
            recoveredInitWeight = i -> DEFAULT_RECOVERED_INIT_WEIGHT;
        }
        if (checkDuration == 0) {
            checkDuration = DEFAULT_CHECK_DURATION;
        }
    }
}