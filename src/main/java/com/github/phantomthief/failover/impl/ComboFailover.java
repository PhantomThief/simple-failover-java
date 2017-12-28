package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.tuple.TwoTuple;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * @author w.vela
 * Created on 2017-12-28.
 */
public class ComboFailover<T> implements Failover<T>, Iterable<Failover<T>> {

    private final List<Failover<T>> failoverList;
    private final boolean recheckOnMiss;

    private volatile Multimap<T, Failover<T>> mapByObject;

    private ComboFailover(ComboFailoverBuilder<T> builder) {
        this.failoverList = builder.list;
        this.recheckOnMiss = builder.recheckOnMiss;
        mapByObject = groupByObjects();
    }

    public static <T> ComboFailoverBuilder<T> builder() {
        return new ComboFailoverBuilder<>();
    }

    private HashMultimap<T, Failover<T>> groupByObjects() {
        return failoverList.stream() //
                .flatMap(failover -> failover.getAll().stream() //
                        .map(it -> tuple(it, failover)))
                .collect(toMultimap(TwoTuple::getFirst, TwoTuple::getSecond, HashMultimap::create));
    }

    @Override
    public List<T> getAll() {
        return failoverList.stream() //
                .map(Failover::getAll) //
                .flatMap(List::stream) //
                .collect(toList());
    }

    @Nullable
    @Override
    public T getOneAvailableExclude(Collection<T> exclusions) {
        return failoverList.stream() //
                .map(failover -> failover.getOneAvailableExclude(exclusions)) //
                .filter(Objects::nonNull) //
                .findAny() //
                .orElse(null);
    }

    @Override
    public List<T> getAvailableExclude(Collection<T> exclusions) {
        return failoverList.stream() //
                .map(failover -> failover.getAvailableExclude(exclusions)) //
                .flatMap(List::stream) //
                .collect(toList());
    }

    @Nullable
    @Override
    public T getOneAvailable() {
        return failoverList.stream() //
                .map(Failover::getOneAvailable) //
                .filter(Objects::nonNull) //
                .findAny() //
                .orElse(null);
    }

    @Override
    public List<T> getAvailable(int n) {
        return failoverList.stream() //
                .map(failover -> failover.getAvailable(n)) //
                .flatMap(List::stream) //
                .limit(n) //
                .collect(toList());
    }

    @Override
    public void fail(@Nonnull T object) {
        getByObject(object).forEach(failover -> failover.fail(object));
    }

    @Override
    public void down(@Nonnull T object) {
        getByObject(object).forEach(failover -> failover.down(object));
    }

    @Override
    public List<T> getAvailable() {
        return failoverList.stream() //
                .map(Failover::getAvailable) //
                .flatMap(List::stream)//
                .collect(toList());
    }

    @Override
    public Set<T> getFailed() {
        return failoverList.stream() //
                .map(Failover::getFailed) //
                .flatMap(Set::stream) //
                .collect(toSet());
    }

    @Override
    public void success(@Nonnull T object) {
        getByObject(object).forEach(failover -> failover.success(object));
    }

    private Collection<Failover<T>> getByObject(T object) {
        Collection<Failover<T>> list = mapByObject.get(object);
        if (recheckOnMiss && list.isEmpty()) { // surely it's wrong. build it again.
            mapByObject = groupByObjects();
            list = mapByObject.get(object);
        }
        return list;
    }

    @Override
    public Iterator<Failover<T>> iterator() {
        return failoverList.iterator();
    }

    @NotThreadSafe
    public static class ComboFailoverBuilder<T> {

        private final List<Failover<T>> list = new ArrayList<>();
        private boolean recheckOnMiss;

        private ComboFailoverBuilder() {
        }

        @CheckReturnValue
        public ComboFailoverBuilder<T> add(@Nonnull Failover<T> failover) {
            list.add(checkNotNull(failover));
            return this;
        }

        @CheckReturnValue
        public ComboFailoverBuilder<T>
                addAll(@Nonnull Collection<? extends Failover<T>> failoverList) {
            list.addAll(checkNotNull(failoverList));
            return this;
        }

        @CheckReturnValue
        public ComboFailoverBuilder<T> recheckOnMiss(boolean value) {
            recheckOnMiss = value;
            return this;
        }

        public ComboFailover<T> build() {
            return new ComboFailover<>(this);
        }
    }
}
