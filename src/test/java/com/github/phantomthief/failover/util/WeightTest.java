package com.github.phantomthief.failover.util;

import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * @author w.vela
 * Created on 2019-01-04.
 */
class WeightTest {

    @Test
    void test() {
        Weight<String> weight = new Weight<String>() //
                .add("s1", 1) //
                .add("s2", 2) //
                .add("s3", 3);
        Multiset<String> result = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            result.add(weight.get());
        }
        assertTrue(between((double) result.count("s2") / result.count("s1"), 1.8, 2.2));
        assertTrue(between((double) result.count("s3") / result.count("s1"), 2.8, 3.2));

        result.clear();

        for (int i = 0; i < 10000; i++) {
            result.add(weight.getWithout(singleton("s3")));
        }
        assertTrue(between((double) result.count("s2") / result.count("s1"), 1.8, 2.2));
        assertEquals(0, result.count("s3"));

        assertEquals(3, weight.allNodes().size());
    }

    private boolean between(double k, double min, double max) {
        return min <= k && k <= max;
    }
}