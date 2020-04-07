package com.github.phantomthief.failover.util;

import static com.github.phantomthief.failover.WeighTestUtils.checkRatio;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;

/**
 * @author w.vela
 * Created on 2019-01-04.
 */
class WeightTest {

    @Test
    void test() {
        Weight<String> weight = new Weight<String>()
                .add("s1", 1)
                .add("s2", 2)
                .add("s3", 3);
        Multiset<String> result = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            result.add(weight.get());
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 2));
        assertTrue(checkRatio(result.count("s3"), result.count("s1"), 3));

        result.clear();

        for (int i = 0; i < 10000; i++) {
            result.add(weight.getWithout(singleton("s3")));
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 2));
        assertEquals(0, result.count("s3"));

        assertEquals(3, weight.allNodes().size());
    }

    @Test
    void testAliasMethod() {
        AliasMethod<String> weight = new AliasMethod<>(ImmutableMap.<String, Integer> builder()
                .put("s1", 1)
                .put("s2", 2)
                .put("s3", 3)
                .build()
        );
        Multiset<String> result = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            result.add(weight.get());
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 2));
        assertTrue(checkRatio(result.count("s3"), result.count("s1"), 3));

        result.clear();

        weight = new AliasMethod<>(ImmutableMap.<String, Integer> builder()
                .put("s1", 2)
                .put("s2", 5)
                .put("s3", 10)
                .put("s4", 20)
                .put("s5", 0)
                .build()
        );

        for (int i = 0; i < 100000; i++) {
            result.add(weight.get());
        }
        assertTrue(checkRatio(result.count("s3"), result.count("s1"), 5));
        assertTrue(checkRatio(result.count("s3"), result.count("s2"), 2));
        assertTrue(checkRatio(result.count("s4"), result.count("s2"), 4));
        assertTrue(checkRatio(result.count("s4"), result.count("s1"), 10));

        assertThrows(IllegalArgumentException.class, () -> {
            new AliasMethod<>(ImmutableMap.<String, Integer> builder()
                    .put("s5", 0)
                    .build()
            );
        });
    }
}