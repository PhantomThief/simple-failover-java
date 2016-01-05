/**
 * 
 */
package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public final class WeightFailoverBuilder<T> {

    private static final int DEFAULT_INIT_WEIGHT = 100;
    private static final int DEFAULT_FAIL_REDUCE_WEIGHT = 5;
    private static final int DEFAULT_SUCCESS_INCREASE_WEIGHT = 1;
    private static final int DEFAULT_RECOVERIED_INIT_WEIGHT = 1;
    private static final long DEFAULT_CHECK_DURATION = SECONDS.toMillis(5);

    private int failReduceWeight;
    private int successIncreaceWeight;
    private int recoveriedInitWeight;
    private Map<T, Integer> initWeightMap;
    private Predicate<T> checker;
    private long checkDuration;

    public WeightFailoverBuilder<T> failReduce(int weight) {
        checkArgument(weight > 0);
        failReduceWeight = weight;
        return this;
    }

    public WeightFailoverBuilder<T> successIncrease(int weight) {
        checkArgument(weight > 0);
        successIncreaceWeight = weight;
        return this;
    }

    public WeightFailoverBuilder<T> recoveiedInit(int weight) {
        checkArgument(weight > 0);
        recoveriedInitWeight = weight;
        return this;
    }

    public WeightFailoverBuilder<T> checkDuration(long time, TimeUnit unit) {
        checkNotNull(unit);
        checkArgument(time > 0);
        checkDuration = unit.toMillis(time);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <E> WeightFailoverBuilder<E> checker(Predicate<? super E> failChecker) {
        checkNotNull(failChecker);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.checker = t -> {
            try {
                return failChecker.test(t);
            } catch (Throwable e) {
                WeightFailover.logger.error("Ops.", e);
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
        return build(original.stream().collect(toMap(identity(), i -> initWeight)));
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
        return new WeightFailover<>(failReduceWeight, successIncreaceWeight, recoveriedInitWeight,
                initWeightMap, checkDuration, checker);
    }

    private void ensure() {
        checkNotNull(checker);
        if (failReduceWeight == 0) {
            failReduceWeight = DEFAULT_FAIL_REDUCE_WEIGHT;
        }
        if (successIncreaceWeight == 0) {
            successIncreaceWeight = DEFAULT_SUCCESS_INCREASE_WEIGHT;
        }
        if (recoveriedInitWeight == 0) {
            recoveriedInitWeight = DEFAULT_RECOVERIED_INIT_WEIGHT;
        }
        if (checkDuration == 0) {
            checkDuration = DEFAULT_CHECK_DURATION;
        }
    }
}