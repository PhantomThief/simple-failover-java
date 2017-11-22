package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.github.phantomthief.failover.Failover;

/**
 * @author w.vela
 * Created on 16/6/2.
 */
public class DummyFailover<T> implements Failover<T> {

    private final Supplier<Collection<T>> supplier;

    private DummyFailover(@Nonnull Supplier<Collection<T>> all) {
        this.supplier = checkNotNull(all);
    }

    public static <T> DummyFailover<T> ofCollection(@Nonnull Collection<T> all) {
        checkNotNull(all);
        return new DummyFailover<>(() -> all);
    }

    public static <T> DummyFailover<T> ofSingle(T single) {
        return new DummyFailover<>(() -> singletonList(single));
    }

    public static <T> DummyFailover<T> ofCollectionSupplier(@Nonnull Supplier<Collection<T>> supplier) {
        return new DummyFailover<>(supplier);
    }

    public static <T> DummyFailover<T> ofSingleSupplier(@Nonnull Supplier<T> single) {
        checkNotNull(single);
        return new DummyFailover<>(() -> singletonList(single.get()));
    }

    @Override
    public List<T> getAll() {
        Collection<T> all = supplier.get();
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
