/**
 * 
 */
package com.github.phantomthief.failover;

import java.util.List;
import java.util.Set;

import com.github.phantomthief.failover.util.RandomListUtils;

/**
 * @author w.vela
 */
public interface Failover<T> {

    public List<T> getAll();

    public void fail(T object);

    /**
     * better use {@code #getAvailable(int)} or {@code #getOneAvailable()}
     */
    @Deprecated
    public List<T> getAvailable();

    public Set<T> getFailed();

    public default T getOneAvailable() {
        return RandomListUtils.getRandom(getAvailable());
    }

    public default List<T> getAvailable(int n) {
        return RandomListUtils.getRandom(getAvailable(), n);
    }
}
