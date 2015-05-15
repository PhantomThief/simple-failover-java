/**
 * 
 */
package com.github.phantomthief.failover;

import java.util.List;
import java.util.Set;

/**
 * @author w.vela
 */
public interface Failover<T> {

    public List<T> getAll();

    public void fail(T object);

    public List<T> getAvailable();

    public Set<T> getFailed();
}
