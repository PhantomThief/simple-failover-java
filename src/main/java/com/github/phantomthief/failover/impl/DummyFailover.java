package com.github.phantomthief.failover.impl;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.github.phantomthief.failover.Failover;

/**
 * @author w.vela
 * Created on 16/6/2.
 */
public class DummyFailover<T> implements Failover<T> {

    private final Collection<T> all;

    private DummyFailover(Collection<T> all) {
        this.all = all;
    }

    public static <T> DummyFailover<T> ofCollection(Collection<T> all) {
        return new DummyFailover<>(all);
    }

    public static <T> DummyFailover<T> ofSingle(T single) {
        return new DummyFailover<>(singletonList(single));
    }

    @Override
    public List<T> getAll() {
        if (all instanceof List) {
            return unmodifiableList((List<? extends T>) all);
        } else {
            return new ArrayList<>(all);
        }
    }

    @Override
    public void fail(T object) {
        // do nothing
    }

    @Override
    public void down(T object) {
        // do nothing
    }

    @Override
    public List<T> getAvailable() {
        return getAll();
    }

    @Override
    public Set<T> getFailed() {
        return emptySet();
    }
}
