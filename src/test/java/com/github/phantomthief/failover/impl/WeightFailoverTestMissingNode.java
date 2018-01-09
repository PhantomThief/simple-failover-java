package com.github.phantomthief.failover.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2017-12-18.
 */
class WeightFailoverTestMissingNode {

    @Test
    void testMissingEnable() {
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(s -> {
                    System.err.println("check:" + s);
                    return s.equals("test1");
                }, 1) //
                .checkDuration(1, SECONDS) //
                .autoAddOnMissing(10) //
                .build(emptyList());
        assertTrue(failover.getAll().isEmpty());
        assertTrue(failover.getAvailable().isEmpty());
        assertTrue(failover.getFailed().isEmpty());

        failover.success("test");

        assertTrue(failover.getAll().contains("test"));
        assertTrue(failover.getAvailable().contains("test"));
        assertFalse(failover.getFailed().contains("test"));

        failover.fail("test2");
        assertTrue(failover.getAll().contains("test2"));
        assertTrue(failover.getAvailable().contains("test2"));
        assertFalse(failover.getFailed().contains("test2"));

        failover.down("test1");
        assertTrue(failover.getAll().contains("test1"));
        assertFalse(failover.getAvailable().contains("test1"));
        assertTrue(failover.getFailed().contains("test1"));

        sleepUninterruptibly(2, SECONDS);

        assertTrue(failover.getAll().contains("test1"));
        assertTrue(failover.getAvailable().contains("test1"));
        assertFalse(failover.getFailed().contains("test1"));
    }

    @Test
    void testNormal() {
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(s -> {
                    System.err.println("check:" + s);
                    return s.equals("test1");
                }, 1) //
                .checkDuration(1, SECONDS) //
                .build(emptyList());
        assertTrue(failover.getAll().isEmpty());
        assertTrue(failover.getAvailable().isEmpty());
        assertTrue(failover.getFailed().isEmpty());

        failover.success("test");

        assertFalse(failover.getAll().contains("test"));
        assertFalse(failover.getAvailable().contains("test"));
        assertFalse(failover.getFailed().contains("test"));

        failover.fail("test2");
        assertFalse(failover.getAll().contains("test2"));
        assertFalse(failover.getAvailable().contains("test2"));
        assertFalse(failover.getFailed().contains("test2"));

        failover.down("test1");
        assertFalse(failover.getAll().contains("test1"));
        assertFalse(failover.getAvailable().contains("test1"));
        assertFalse(failover.getFailed().contains("test1"));

        sleepUninterruptibly(2, SECONDS);

        assertFalse(failover.getAll().contains("test1"));
        assertFalse(failover.getAvailable().contains("test1"));
        assertFalse(failover.getFailed().contains("test1"));
    }
}
