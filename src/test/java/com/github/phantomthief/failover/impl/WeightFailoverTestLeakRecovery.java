package com.github.phantomthief.failover.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2017-12-07.
 */
class WeightFailoverTestLeakRecovery {

    @Test
    void test() {
        boolean[] check = { false };
        WeightFailover<String> weightFailover = WeightFailover.<String> newGenericBuilder() //
                .checkDuration(10, MILLISECONDS) //
                .checker(str -> {
                    check[0] = true;
                    return false;
                }).build(singletonList("test"), 10);
        weightFailover.close();
        weightFailover.down("test");
        sleepUninterruptibly(100, MILLISECONDS);
        check[0] = false;
        assertFalse(check[0]);
    }
}