/**
 * 
 */
package com.github.phantomthief.failover.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import com.github.phantomthief.failover.Failover;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * @author w.vela
 */
public class WeightFailoverTest {

    @Test
    public void testCommon() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.newBuilder() //
                .checker(this::check) //
                .build(original);
        Multiset<String> result = HashMultiset.create();
        Multiset<Integer> getCount = HashMultiset.create();
        for (int i = 0; i < 500; i++) {
            List<String> available = failover.getAvailable(2);
            assertTrue(available.size() <= 2);
            getCount.add(available.size());
            available.forEach(obj -> {
                assertNotNull(obj);
                if (doSomething(obj, failover)) {
                    result.add(obj);
                }
            });
            sleepUninterruptibly(10, MILLISECONDS);
        }
        System.out.println(getCount);
        System.out.println(result);
    }

    @Test
    public void testMinWeight() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.newBuilder() //
                .checker(this::check) //
                .minWeight(1) //
                .build(original);
        Multiset<String> result = HashMultiset.create();
        Multiset<Integer> getCount = HashMultiset.create();
        for (int i = 0; i < 500; i++) {
            List<String> available = failover.getAvailable(2);
            assertTrue(available.size() == 2);
            getCount.add(available.size());
            available.forEach(obj -> {
                assertNotNull(obj);
                if (doSomething(obj, failover)) {
                    result.add(obj);
                }
            });
            sleepUninterruptibly(10, MILLISECONDS);
        }
        System.out.println(getCount);
        System.out.println(result);
    }

    private boolean check(String test) {
        System.out.println("test:" + test);
        return true;
    }

    private boolean doSomething(String obj, Failover<String> failover) {
        boolean result = ThreadLocalRandom.current().nextInt(10) > Integer.parseInt(obj);
        if (result) {
            failover.success(obj);
        } else {
            failover.fail(obj);
        }
        return result;
    }
}
