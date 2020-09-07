package com.github.phantomthief.failover;

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 简化的Failover接口，提供的方法更少更简单，以达到更好的性能，简单的接口也有利于实现类做组合。
 *
 * <p>
 * 请先阅读README.md，可以到<a href="https://github.com/PhantomThief/simple-failover-java">这里</a>在线阅读。
 * </p>
 *
 * @author huangli
 * Created on 2020-01-15
 */
public interface SimpleFailover<T> {

    /**
     * 资源调用成功后，由使用方调用本方法，通知failover组件资源调用成功，随后failover内部通常会增加这个资源的权重（不超过最大值）。
     *
     * 这个方法以前居然是个默认方法，为了保持兼容还是别改了。
     *
     * @param object 被调用的资源
     */
    default void success(@Nonnull T object) {
        // default behavior: do nothing
    }

    /**
     * 资源调用失败后，由使用方调用本方法，通知failover组件资源调用失败，随后failover内部通常会扣减这个资源的权重（不低于最小值）。
     *
     * @param object 被调用的资源
     */
    void fail(@Nonnull T object);

    /**
     * 有时候使用方可能想把资源直接的权重直接扣减到最小值，可调用本方法。
     *
     * @param object 被调用的资源
     */
    void down(@Nonnull T object);

    /**
     * 获取一个可使用的资源，如果所有的资源都down了，可能会返回null，注意判空。
     *
     * @return 一个用于执行调用的资源
     */
    @Nullable
    T getOneAvailable();

    /**
     * 当调用失败后，使用方想要换一个资源重试，可使用本方法重新获取另一个资源，注意判空。
     *
     * @param exclusions 需要排除的资源列表，不会出现在返回的结果中，通常是之前调用失败的资源
     * @return 一个用于执行调用的资源
     */
    @Nullable
    T getOneAvailableExclude(Collection<T> exclusions);
}