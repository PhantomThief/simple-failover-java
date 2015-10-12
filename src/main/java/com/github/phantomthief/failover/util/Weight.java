/**
 * 
 */
package com.github.phantomthief.failover.util;

import org.apache.commons.lang3.RandomUtils;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * 带权重的树
 * 
 * @author w.vela
 * @param <T>
 */
public class Weight<T> {

    private final RangeMap<Long, T> nodes = TreeRangeMap.create();
    private long maxWeight = 0;

    public Weight<T> add(T node, long weight) {
        nodes.put(Range.closedOpen(maxWeight, maxWeight + weight), node);
        maxWeight += weight;
        return this;
    }

    public T get() {
        if (isEmpty()) {
            return null;
        }
        long resultIndex = RandomUtils.nextLong(0, maxWeight);
        return nodes.get(resultIndex);
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    public boolean isEmpty() {
        return maxWeight == 0;
    }

}
