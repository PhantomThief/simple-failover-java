package com.github.phantomthief.failover.util;

import static java.lang.ThreadLocal.withInitial;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.impl.WeightFailover;

/**
 * @author w.vela
 * Created on 2018-12-30.
 */
class FilteredFailoverTest {

    @Test
    void test() {
        Set<String> blocked = new HashSet<>();
        Predicate<String> filter = it -> !blocked.contains(it);
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(it -> 1.0D) //
                .filter(filter) //
                .build(rangeClosed(1, 10) //
                        .mapToObj(it -> "s" + it) //
                        .collect(toList()), 10);
        blocked.add("s1");
        for (int i = 0; i < 100; i++) {
            assertTrue(!failover.getAvailable().contains("s1"));
            assertNotEquals("s1", failover.getOneAvailable());
        }
    }

    @Test
    void testThreadRetry() {
        ThreadLocal<Set<String>> tried = withInitial(HashSet::new);
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(it -> 1.0D) //
                .filter(it -> !tried.get().contains(it)) //
                .build(rangeClosed(1, 10) //
                        .mapToObj(it -> "s" + it) //
                        .collect(toList()), 10);

        for (int i = 0; i < 100; i++) {
            try {
                String one = failover.getOneAvailable();
                if ("s1".equals(one)) {
                    tried.get().add(one);
                    one = failover.getOneAvailable();
                    assertNotEquals("s1", one);
                }
            } finally {
                tried.remove();
            }
        }
    }
}