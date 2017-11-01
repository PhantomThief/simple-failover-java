package com.github.phantomthief.failover;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.phantomthief.failover.util.FailoverUtils;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 */
public interface Failover<T> {

    List<T> getAll();

    default void success(@Nonnull T object) {
        // default behavior: do nothing
    }

    void fail(@Nonnull T object);

    void down(@Nonnull T object);

    /**
     * better use {@code #getAvailable(int)} or {@code #getOneAvailable()}
     */
    List<T> getAvailable();

    default List<T> getAvailableExclude(Collection<T> exclusions) {
        return getAvailable().stream().filter(e -> !exclusions.contains(e)).collect(toList());
    }

    Set<T> getFailed();

    @Nullable
    default T getOneAvailable() {
        return getRandom(getAvailable());
    }

    @Nullable
    default T getOneAvailableExclude(Collection<T> exclusions) {
        return getRandom(getAvailableExclude(exclusions));
    }

    default List<T> getAvailable(int n) {
        return getRandom(getAvailable(), n);
    }

    /**
     * @see FailoverUtils#supplyWithRetry
     */
    default <E, X extends Throwable> E supplyWithRetry(ThrowableFunction<T, E, X> func) throws X {
        return FailoverUtils.supplyWithRetry(getAll().size(), 0, this, func);
    }

    /**
     * @see FailoverUtils#runWithRetry
     */
    default <X extends Throwable> void runWithRetry(ThrowableConsumer<T, X> func) throws X {
        FailoverUtils.runWithRetry(getAll().size(), 0, this, func);
    }
}
