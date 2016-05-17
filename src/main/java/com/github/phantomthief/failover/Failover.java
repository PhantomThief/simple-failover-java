/**
 * 
 */
package com.github.phantomthief.failover;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author w.vela
 */
public interface Failover<T> {

    List<T> getAll();

    default void success(T object) {
        // default behavior: do nothing
    }

    void fail(T object);

    /**
     * better use {@code #getAvailable(int)} or {@code #getOneAvailable()}
     */
    List<T> getAvailable();

    default List<T> getAvailableExclude(Collection<T> exclusions) {
        return getAvailable().stream().filter(e -> !exclusions.contains(e)).collect(toList());
    }

    Set<T> getFailed();

    default T getOneAvailable() {
        return getRandom(getAvailable());
    }

    default T getOneAvailableExclude(Collection<T> exclusions) {
        return getRandom(getAvailableExclude(exclusions));
    }

    default List<T> getAvailable(int n) {
        return getRandom(getAvailable(), n);
    }
}
