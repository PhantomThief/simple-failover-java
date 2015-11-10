/**
 * 
 */
package com.github.phantomthief.failover.util;

import java.util.HashMap;
import java.util.Map;

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

    private final Map<T, Long> weightMap = new HashMap<>();
    private final RangeMap<Long, T> nodes = TreeRangeMap.create();
    private long maxWeight = 0;
    private int size;

    public Weight<T> add(T node, long weight) {
        if (weight > 0) {
            weightMap.put(node, weight);
            nodes.put(Range.closedOpen(maxWeight, maxWeight + weight), node);
            maxWeight += weight;
            size++;
        }
        return this;
    }

    public T get() {
        if (isEmpty()) {
            return null;
        }
        long resultIndex = RandomUtils.nextLong(0, maxWeight);
        return nodes.get(resultIndex);
    }

    public boolean isEmpty() {
        return maxWeight == 0;
    }

    @Override
    public String toString() {
        return nodes.toString();
    }
}
