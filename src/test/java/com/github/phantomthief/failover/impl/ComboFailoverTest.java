package com.github.phantomthief.failover.impl;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * @author w.vela
 * Created on 2017-12-28.
 */
class ComboFailoverTest {

    private static final Logger logger = LoggerFactory.getLogger(ComboFailoverTest.class);

    @Test
    void test() {
        ComboFailover<String> combo = ComboFailover.<String> builder() //
                .add(WeightFailover.<String> newGenericBuilder() //
                        .checker(this::check) //
                        .checkDuration(1, SECONDS) //
                        .failReduceRate(0.1) //
                        .build(of("test1", "test2"))) //
                .add(WeightFailover.<String> newGenericBuilder() //
                        .checker(this::check) //
                        .checkDuration(1, SECONDS) //
                        .failReduceRate(0.1) //
                        .build(of("test3", "test4"))) //
                .build();
        ImmutableList<String> all = of("test1", "test2", "test3", "test4");
        assertTrue(all.containsAll(combo.getAll()));
        assertTrue(all.containsAll(combo.getAvailable()));
        assertTrue(all.contains(combo.getOneAvailable()));
        for (int i = 0; i < 10; i++) {
            assertTrue(of("test1", "test2")
                    .contains(combo.getOneAvailableExclude(of("test3", "test4"))));
            assertFalse(of("test3", "test4")
                    .contains(combo.getOneAvailableExclude(of("test3", "test4"))));
        }

        for (int i = 0; i < 10; i++) {
            combo.fail("test1");
        }
        assertEquals(singleton("test1"), combo.getFailed());
        for (int i = 0; i < 10; i++) {
            assertFalse(of("test3").contains(combo.getOneAvailable()));
        }
        sleepUninterruptibly(2, SECONDS);
        assertTrue(combo.getFailed().isEmpty());
        for (int i = 0; i < 10; i++) {
            assertTrue(of("test3")
                    .contains(combo.getOneAvailableExclude(of("test1", "test2", "test4"))));
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(of("test1", "test2").contains(combo.getOneAvailable()));
            assertFalse(of("test3", "test4").contains(combo.getOneAvailable()));
        }
        combo.down("test1");
        combo.down("test2");
        for (int i = 0; i < 10; i++) {
            assertFalse(of("test1", "test2").contains(combo.getOneAvailable()));
            assertTrue(of("test3", "test4").contains(combo.getOneAvailable()));
        }
        sleepUninterruptibly(2, SECONDS);

        combo.forEach(failover -> {
            if (failover instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) failover).close();
                } catch (Exception e) {
                    throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private double check(String value) {
        logger.info("check:{}", value);
        return 0.5D;
    }
}