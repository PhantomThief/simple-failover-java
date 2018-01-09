package com.github.phantomthief.failover.impl;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.Failover;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/**
 * @author w.vela
 */
class WeightFailoverTest {

    @Test
    void testCommon() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.newBuilder() //
                .checker(this::check, 1) //
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
    void testMinWeight() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.newBuilder() //
                .checker(this::check, 1) //
                .minWeight(1) //
                .onMinWeight(i -> System.out.println("onMin:" + i)) //
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

    @Test
    void testDown() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(it -> false, 1) //
                .onMinWeight(i -> System.out.println("onMin:" + i)) //
                .build(original);
        Multiset<String> result = HashMultiset.create();
        Multiset<Integer> getCount = HashMultiset.create();
        failover.down("1");
        for (int i = 0; i < 500; i++) {
            String available = failover.getOneAvailable();
            assertNotEquals(available, "1");
            sleepUninterruptibly(10, MILLISECONDS);
        }
        System.out.println(getCount);
        System.out.println(result);
    }

    @Test
    void testLarge() {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 108; i++) {
            map.put("i" + i, 32518);
        }
        for (int i = 0; i < 331; i++) {
            map.put("j" + i, 2652);
        }
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(it -> true, 1) //
                .build(map);
        Multiset<String> counter = HashMultiset.create();
        for (int i = 0; i < 100000; i++) {
            String oneAvailable = failover.getOneAvailable();
            counter.add(oneAvailable.substring(0, 1));
        }
        System.out.println(counter);
    }

    @Test
    void testRateRecover() {
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> 0.5) //
                .build(of("s1", "s2"), 100);
        assertEquals(100, failover.currentWeight("s1"));
        assertEquals(100, failover.currentWeight("s2"));
        failover.down("s2");
        assertEquals(0, failover.currentWeight("s2"));
        sleepUninterruptibly(2, SECONDS);
        assertEquals(50, failover.currentWeight("s2"));
    }

    @Test
    void testRateRecover2() {
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> true, 0.00001) //
                .build(of("s1", "s2"), 100);
        assertEquals(100, failover.currentWeight("s1"));
        assertEquals(100, failover.currentWeight("s2"));
        failover.down("s2");
        assertEquals(0, failover.currentWeight("s2"));
        sleepUninterruptibly(2, SECONDS);
        assertEquals(1, failover.currentWeight("s2"));
    }

    @Test
    void testPerf() {
        Map<String, Integer> map = IntStream.range(1, 10).boxed()
                .collect(toMap(it -> "s" + it, identity()));
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder() //
                .checker(it -> 1.0) //
                .build(map);
        long s = currentTimeMillis();
        Multiset<String> counter = TreeMultiset.create();
        for (int i = 0; i < 100000; i++) {
            List<String> available = failover.getAvailable(2);
            counter.addAll(available);
        }
        // old 260~270
        System.out.println(counter + ", cost:" + (currentTimeMillis() - s));
        s = currentTimeMillis();
        counter = TreeMultiset.create();
        for (int i = 0; i < 100000; i++) {
            counter.add(failover.getOneAvailable());
        }
        // old 60~70
        System.out.println(counter + ", cost:" + (currentTimeMillis() - s));
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
