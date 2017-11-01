package com.github.phantomthief.failover.util;

import static com.google.common.collect.Range.closedOpen;
import static com.google.common.collect.TreeRangeMap.create;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.RangeMap;

/**
 * 带权重的树
 * 
 * @author w.vela
 * @param <T>
 */
public class Weight<T> {

    private final Map<T, Long> weightMap = new HashMap<>();
    private final RangeMap<Long, T> nodes = create();
    private long maxWeight = 0;

    public Weight<T> add(@Nonnull T node, long weight) {
        if (weight > 0) {
            weightMap.put(node, weight);
            nodes.put(closedOpen(maxWeight, maxWeight + weight), node);
            maxWeight += weight;
        }
        return this;
    }

    @Nullable
    public T get() {
        if (isEmpty()) {
            return null;
        }
        long resultIndex = nextLong(0, maxWeight);
        return nodes.get(resultIndex);
    }

    @Nullable
    public T getWithout(Set<T> exclusions) {
        if (weightMap.size() == exclusions.size()) {
            return null;
        }
        while (true) {
            T t = get();
            if (!exclusions.contains(t)) {
                return t;
            }
        }
    }

    public boolean isEmpty() {
        return maxWeight == 0;
    }

    public Set<T> allNodes() {
        return weightMap.keySet();
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    private static long nextLong(long startInclusive, long endExclusive) {
        if (startInclusive == endExclusive) {
            return startInclusive;
        }

        return (long) ThreadLocalRandom.current().nextDouble(startInclusive, endExclusive);
    }
}
