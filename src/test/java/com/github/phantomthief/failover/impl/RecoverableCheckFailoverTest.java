package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.failover.WeighTestUtils.checkRatio;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

/**
 * @author w.vela
 * Created on 2019-01-04.
 */
class RecoverableCheckFailoverTest {

    @Test
    void test() {
        boolean[] checkerSwitcher = { false };
        Predicate<String> checker = it -> checkerSwitcher[0];
        RecoverableCheckFailover<String> failover = RecoverableCheckFailover
                .<String> newGenericBuilder().setChecker(checker) // 
                .setFailCount(10) //
                .setFailDuration(100, MILLISECONDS) //
                .setRecoveryCheckDuration(100, MILLISECONDS) //
                .setReturnOriginalWhileAllFailed(false) //
                .build(ImmutableList.of("s1", "s2"));
        for (int i = 0; i < 10; i++) {
            failover.fail("s2");
        }
        for (int i = 0; i < 10; i++) {
            assertEquals("s1", failover.getOneAvailable());
        }
        checkerSwitcher[0] = true;
        sleepUninterruptibly(100, MILLISECONDS);
        Multiset<String> result = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            result.add(failover.getOneAvailable());
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 1));
    }
}