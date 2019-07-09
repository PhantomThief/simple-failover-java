package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.failover.WeighTestUtils.checkRatio;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.math3.stat.inference.AlternativeHypothesis.TWO_SIDED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.inference.BinomialTest;
import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.Failover;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/**
 * @author w.vela
 */
class WeightFailoverTest {

    @Test
    void testCommon() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.newBuilder()
                .checker(this::check, 1)
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

        assertEquals(new HashSet<>(failover.getAvailable()),
                new HashSet<>(failover.getAvailable(original.size())));
        System.out.println(getCount);
        System.out.println(result);
    }

    @Test
    void testMinWeight() {
        List<String> original = Arrays.asList("1", "2", "3");
        Failover<String> failover = WeightFailover.newBuilder()
                .checker(this::check, 1)
                .minWeight(1)
                .onMinWeight(i -> System.out.println("onMin:" + i))
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
        Failover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> false, 1)
                .onMinWeight(i -> System.out.println("onMin:" + i))
                .build(original);
        Multiset<String> result = HashMultiset.create();
        Multiset<Integer> getCount = HashMultiset.create();
        failover.down("1");
        for (int i = 0; i < 500; i++) {
            String available = failover.getOneAvailable();
            assertNotEquals(available, "1");
            sleepUninterruptibly(10, MILLISECONDS);
        }
        assertEquals(new HashSet<>(failover.getAvailable()),
                new HashSet<>(failover.getAvailable(original.size())));
        System.out.println(getCount);
        System.out.println(result);
    }

    @Test
    void testLarge() {
        Map<String, Integer> map = new HashMap<>();
        double sum = 0;
        double iSum = 0;
        for (int i = 0; i < 108; i++) {
            int iWeight = 32518;
            map.put("i" + i, iWeight);
            iSum += iWeight;
            sum += iWeight;
        }
        for (int i = 0; i < 331; i++) {
            int jWeight = 2652;
            map.put("j" + i, jWeight);
            sum += jWeight;
        }
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> true, 1)
                .build(map);
        Multiset<String> counter = HashMultiset.create();
        int trials = 100000;
        for (int i = 0; i < trials; i++) {
            String oneAvailable = failover.getOneAvailable();
            counter.add(oneAvailable.substring(0, 1));
        }
        System.out.println(counter);
        assertFalse(new BinomialTest().binomialTest(trials, counter.count("i"), iSum / sum,
                TWO_SIDED, 0.01));
    }

    @Test
    void testRateRecover() {
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> 0.5)
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
                .checker(it -> true, 0.00001)
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
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> 1.0)
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

    @Test
    void testWeight() {
        boolean[] block3 = { false };
        Predicate<String> filter = it -> {
            if (block3[0]) {
                return !it.equals("s3");
            } else {
                return true;
            }
        };
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .filter(filter)
                .checker(it -> 0.0)
                .build(ImmutableMap.of("s1", 1, "s2", 2, "s3", 3));
        Multiset<String> result = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            result.add(failover.getOneAvailable());
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 2));
        assertTrue(checkRatio(result.count("s3"), result.count("s1"), 3));

        block3[0] = true;

        result.clear();
        for (int i = 0; i < 10000; i++) {
            result.add(failover.getOneAvailable());
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 2));
        assertEquals(0, result.count("s3"));

        failover.down("s2");
        block3[0] = false;
        result.clear();
        for (int i = 0; i < 10000; i++) {
            result.add(failover.getOneAvailable());
        }
        assertEquals(0, result.count("s2"));
        assertTrue(checkRatio(result.count("s3"), result.count("s1"), 3));
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
