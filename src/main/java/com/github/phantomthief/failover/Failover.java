package com.github.phantomthief.failover;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.github.phantomthief.failover.util.FailoverUtils;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;

/**
 * Failover接口，有多个实现。
 * 这是原来的接口，方法比较多，子类要实现这些方法负担也比较大，重构后抽取了SimpleFailover接口，
 * 现在建议使用SimpleFailover，更简单，也能实现绝大部分功能。
 *
 * <p>
 * 请先阅读README.md，可以到<a href="https://github.com/PhantomThief/simple-failover-java">这里</a>在线阅读。
 * </p>
 *
 * @author w.vela
 */
public interface Failover<T> extends SimpleFailover<T> {

    /**
     * 获取所有资源的列表，包括不可用资源。
     * @return 所有资源
     */
    List<T> getAll();

    /**
     * 返回可用的资源列表。
     * @return 可用资源列表
     * @see #getOneAvailable()
     * @see #getAvailable(int)
     */
    List<T> getAvailable();

    /**
     * 返回可用资源列表，但是排除掉参数指定的资源
     * @param exclusions 需要从结果中排除的资源
     * @return 满足条件的资源列表
     */
    default List<T> getAvailableExclude(Collection<T> exclusions) {
        return getAvailable().stream().filter(e -> !exclusions.contains(e)).collect(toList());
    }

    /**
     * 获取不健康（当前权重=最小权重）的资源。
     * @return 资源列表
     */
    Set<T> getFailed();

    /**
     * 获取一个可使用的资源，如果所有的资源都down了，可能会返回null，注意判空。
     *
     * @return 一个用于执行调用的资源
     */
    @Nullable
    @Override
    default T getOneAvailable() {
        return getRandom(getAvailable());
    }

    /**
     * 当调用失败后，使用方想要换一个资源重试，可使用本方法重新获取另一个资源，注意判空。
     *
     * @param exclusions 需要排除的资源列表，不会出现在返回的结果中，通常是之前调用失败的资源
     * @return 一个用于执行调用的资源
     */
    @Nullable
    @Override
    default T getOneAvailableExclude(Collection<T> exclusions) {
        return getRandom(getAvailableExclude(exclusions));
    }

    /**
     * 获取可用的资源列表，但是限制只返回n个。
     * @param n 返回值的限制数
     * @return 资源列表
     */
    default List<T> getAvailable(int n) {
        return getRandom(getAvailable(), n);
    }

    /**
     * 选定一个资源执行业务操作，失败了就再选另一个，直到成功或者所有的资源都试过一遍。
     * 通常来说，建议不要用这个方法，因为它的默认实现行为不固定，但是现在也不敢改了，使用者自己来重试更好控制一些。
     * @param func 业务操作回调，有返回值
     * @return 业务回调的返回值
     * @throws X 最后一次失败的业务异常，有时候还会丢出NoAvailableResourceException
     * @see FailoverUtils#supplyWithRetry
     */
    default <E, X extends Throwable> E supplyWithRetry(ThrowableFunction<T, E, X> func) throws X {
        return FailoverUtils.supplyWithRetry(getAll().size(), 0, this, func);
    }

    /**
     * 选定一个资源执行业务操作，失败了就再选另一个，直到成功或者所有的资源都试过一遍。
     * 通常来说，建议不要用这个方法，因为它的默认实现行为不固定，但是现在也不敢改了，使用者自己来重试更好控制一些。
     * @param func 业务操作回调，没有返回值
     * @throws X 最后一次失败的业务异常，有时候还会丢出NoAvailableResourceException
     * @see FailoverUtils#runWithRetry
     */
    default <X extends Throwable> void runWithRetry(ThrowableConsumer<T, X> func) throws X {
        FailoverUtils.runWithRetry(getAll().size(), 0, this, func);
    }
}