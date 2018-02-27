package com.github.phantomthief.failover.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nonnull;

/**
 * base on https://en.wikipedia.org/wiki/Linear_congruential_generator
 * 
 * @author w.vela
 * Created on 2018-02-26.
 */
class LcgRandomIterator<T> implements Iterator<T> {

    private static final int c = 11;
    private static final long a = 25214903917L;

    private final List<T> original;
    private final long seed;
    private final long m;
    private final long n;

    private long next;
    private boolean hasNext = true;

    LcgRandomIterator(@Nonnull List<T> original) {
        if (!(original instanceof RandomAccess)) {
            throw new IllegalArgumentException();
        }
        this.original = checkNotNull(original);
        this.n = original.size();
        m = (long) Math.pow(2, Math.ceil(Math.log(n) / Math.log(2)));
        next = seed = ThreadLocalRandom.current().nextLong(Math.min(n, Integer.MAX_VALUE));
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        next = (a * next + c) % m;
        while (next >= n) {
            next = (a * next + c) % m;
        }
        if (next == seed) {
            hasNext = false;
        }
        return original.get((int) next);
    }
}
