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
import java.util.function.ToDoubleFunction;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableFunction;
import com.github.phantomthief.util.ThrowablePredicate;

/**
 * WeightFailover的builder。
 *
 * @param <T> 要构建的资源的类型
 * @see WeightFailover#newGenericBuilder()
 */
@SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:HiddenField"})
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

    Predicate<T> filter;

    /**
     * 设定failover的name。
     * @param value 名称
     * @param <E> 要构建的failover中的资源类型
     * @return this
     */
    @CheckReturnValue
    @SuppressWarnings("unchecked")
    @Nonnull
    public <E> WeightFailoverBuilder<E> name(String value) {
        this.name = value;
        return (WeightFailoverBuilder<E>) this;
    }

    /**
     * 设定自动添加的资源的权重，自动添加指的是调用
     * {@link com.github.phantomthief.failover.SimpleFailover#success(Object)}、
     * {@link com.github.phantomthief.failover.SimpleFailover#fail(Object)}、
     * {@link com.github.phantomthief.failover.SimpleFailover#down(Object)}
     * 这三个方法时，如果传入的参数不在原本的资源列表中，WeightFailover将自动添加该资源。
     *
     * 如果本方法没有被调用，那么构建出来的{@link WeightFailover}将不支持自动添加资源。可以看出
     * {@link WeightFailover}内部的资源列表是可变的，
     * 作为对比{@link PriorityFailover}的资源列表是不可变的，它使用{@link PriorityFailoverManager}来
     * 协助资源列表的变更。
     *
     * @param weight 权重
     * @param <E> 要构建的failover中的资源类型
     * @return this
     */
    @CheckReturnValue
    @SuppressWarnings("unchecked")
    @Nonnull
    public <E> WeightFailoverBuilder<E> autoAddOnMissing(int weight) {
        checkArgument(weight >= 0);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        this.weightOnMissingNode = weight;
        return thisBuilder;
    }

    /**
     * 设置一个回调器，当资源的当前权重从非最小权重减少到最小权重时，触发一次回调。
     * 也就是说，一直连续失败不会导致这个回调多次被调用，除非中间夹杂了成功导致权重上升到非最小权重。
     * @param listener 回调器
     * @param <E> 要构建的failover中的资源类型
     * @return this
     */
    @CheckReturnValue
    @SuppressWarnings("unchecked")
    @Nonnull
    public <E> WeightFailoverBuilder<E> onMinWeight(Consumer<E> listener) {
        checkNotNull(listener);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.onMinWeight = listener;
        return thisBuilder;
    }

    /**
     * 设置一个回调器，当后台检查检查任务恢复某个资源的权重以后，触发一次回调。
     * @param listener 回调器
     * @param <E> 要构建的failover中的资源类型
     * @return this
     */
    @CheckReturnValue
    @SuppressWarnings("unchecked")
    @Nonnull
    public <E> WeightFailoverBuilder<E> onRecovered(Consumer<E> listener) {
        checkNotNull(listener);
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.onRecovered = listener;
        return thisBuilder;
    }

    /**
     * 设置权重最小值
     * @param value 最小权重
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public WeightFailoverBuilder<T> minWeight(int value) {
        checkArgument(value >= 0);
        this.minWeight = value;
        return this;
    }

    /**
     * 指定失败时要扣减的最大权重比例，应该在0到1之间，例如最大权重100，rate=0.05，那么失败以后当前权重扣5。
     * @param rate 扣减最大权重的比例
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public WeightFailoverBuilder<T> failReduceRate(double rate) {
        checkArgument(rate > 0 && rate <= 1);
        failReduceWeight = i -> Math.max(1, (int) (rate * i));
        return this;
    }

    /**
     * 指定失败时要扣减的权重数值。
     * @param weight 要扣减的权重
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public WeightFailoverBuilder<T> failReduce(int weight) {
        checkArgument(weight > 0);
        failReduceWeight = i -> weight;
        return this;
    }

    /**
     * 指定成功后要增加的最大权重比例，应该在0到1之间，例如最大权重100，rate=0.05，那么成功以后当前权重加5。
     * @param rate 最大权重比例
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public WeightFailoverBuilder<T> successIncreaseRate(double rate) {
        checkArgument(rate > 0 && rate <= 1);
        successIncreaseWeight = i -> Math.max(1, (int) (rate * i));
        return this;
    }

    /**
     * 指定成功以后要增加的权重数值。
     * @param weight 权重数值
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public WeightFailoverBuilder<T> successIncrease(int weight) {
        checkArgument(weight > 0);
        successIncreaseWeight = i -> weight;
        return this;
    }

    /**
     * 指定健康检查时间间隔。
     * @param time 时间数值
     * @param unit 时间单位
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public WeightFailoverBuilder<T> checkDuration(long time, TimeUnit unit) {
        checkNotNull(unit);
        checkArgument(time > 0);
        checkDuration = unit.toMillis(time);
        return this;
    }

    /**
     * 指定一个过滤器，从failover获取资源的时候会过滤掉部分资源。
     * @param filter 用户定义的过滤器
     * @param <E> 资源类型
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public <E> WeightFailoverBuilder<E> filter(@Nonnull Predicate<E> filter) {
        checkNotNull(filter);

        @SuppressWarnings("unchecked")
        WeightFailoverBuilder<E> thisBuilder = (WeightFailoverBuilder<E>) this;
        thisBuilder.filter = filter;
        return thisBuilder;
    }

    /**
     * 指定健康检查器，健康检查器将在资源达到最小权重后开始针对该资源调度。
     * @param failChecker 健康检查器，输入是资源，输出是健康检查成功后，需要恢复的最大权重比例
     * @param <E> 资源类型
     * @return this
     */
    @SuppressWarnings("unchecked")
    @CheckReturnValue
    @Nonnull
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

    /**
     * 指定健康检查器，健康检查器将在资源达到最小权重后开始针对该资源调度。
     * @param failChecker 健康检查器，输入是资源，输出是资源是否健康
     * @param recoveredInitRate 健康检查成功后，需要恢复的最大权重的比例
     * @param <E> 资源类型
     * @return this
     */
    @CheckReturnValue
    @Nonnull
    public <E> WeightFailoverBuilder<E> checker(
            @Nonnull ThrowablePredicate<? super E, Throwable> failChecker,
            @Nonnegative double recoveredInitRate) {
        checkArgument(recoveredInitRate >= 0 && recoveredInitRate <= 1);
        checkNotNull(failChecker);
        return checker(it -> failChecker.test(it) ? recoveredInitRate : 0);
    }

    /**
     * 构造一个WeightFailover实例，使用100作为默认的初始/最大权重。WeightFailover没有区分初始权重和最大权重，初始权重和最大权重是相等的。
     * @param original 资源列表
     * @param <E> 资源类型
     * @return 构造出来的WeightFailover实例
     */
    @Nonnull
    public <E> WeightFailover<E> build(Collection<? extends E> original) {
        return build(original, DEFAULT_INIT_WEIGHT);
    }

    /**
     * 构造一个WeightFailover实例。注意WeightFailover没有区分初始权重和最大权重，初始权重和最大权重是相等的。
     * @param original 资源列表
     * @param initWeight 初始权重
     * @param <E> 资源类型
     * @return 构造出来的WeightFailover实例
     */
    @Nonnull
    public <E> WeightFailover<E> build(Collection<? extends E> original, int initWeight) {
        checkNotNull(original);
        checkArgument(initWeight > 0);
        return build(original.stream().collect(toMap(identity(), i -> initWeight, (u, v) -> u)));
    }

    /**
     * 构造一个WeightFailover实例。注意WeightFailover没有区分初始权重和最大权重，初始权重和最大权重是相等的。
     * @param original 资源列表，map里面的key是资源，value是初始权重。
     * @param <E> 资源类型
     * @return 构造出来的WeightFailover实例
     */
    @SuppressWarnings("unchecked")
    @Nonnull
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