package com.github.phantomthief.failover;

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author huangli
 * Created on 2020-01-15
 */
public interface SimpleFailover<T> {
    default void success(@Nonnull T object) {
        // default behavior: do nothing
    }

    void fail(@Nonnull T object);

    void down(@Nonnull T object);

    @Nullable
    T getOneAvailable();

    @Nullable
    T getOneAvailableExclude(Collection<T> exclusions);
}