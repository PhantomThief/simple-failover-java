/**
 * 
 */
package com.github.phantomthief.failover;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandom;

import java.util.List;
import java.util.Set;

import com.github.phantomthief.failover.util.FailoverUtils;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 */
public interface Failover<T> {

    List<T> getAll();

    default void success(T object) {
        // default behavor: do nothing
    }

    void fail(T object);

    /**
     * better use {@code #getAvailable(int)} or {@code #getOneAvailable()}
     */
    List<T> getAvailable();

    Set<T> getFailed();

    default T getOneAvailable() {
        return getRandom(getAvailable());
    }

    default List<T> getAvailable(int n) {
        return getRandom(getAvailable(), n);
    }

    default <X extends Throwable> void call(ThrowableConsumer<T, Throwable> func) {
        FailoverUtils.call(this, func, null);
    }

    default <R, X extends Throwable> R run(ThrowableFunction<T, R, Throwable> func) {
        return FailoverUtils.run(this, func, null);
    }
}
