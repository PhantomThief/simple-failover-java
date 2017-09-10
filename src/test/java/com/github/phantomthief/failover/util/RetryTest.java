package com.github.phantomthief.failover.util;

import static com.google.common.base.Predicates.alwaysFalse;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.github.phantomthief.failover.impl.WeightFailover;

/**
 * @author w.vela
 * Created on 2017-06-09.
 */
public class RetryTest {

    @Test
    public void test() {
        WeightFailover<Integer> failover = buildNewFailover();
        for (int i = 0; i < 50; i++) {
            String x = failover.supplyWithRetry(this::doSomething);
            System.out.println(x);
            assertEquals(x, "r:1");
        }
        failover = buildNewFailover();
        System.out.println("test fail");
        for (int i = 0; i < 50; i++) {
            try {
                String x = failover.supplyWithRetry(this::alwaysFail);
                System.out.println(x);
                Assert.fail();
            } catch (Throwable e) {
                Assert.assertTrue(true);
                System.out.println("fail, pass.");
            }
        }
        failover = buildNewFailover();
        System.out.println("test some failed.");
        for (int i = 0; i < 50; i++) {
            try {
                String x = failover.supplyWithRetry(this::doSomething);
                System.out.println(x);
                assertEquals(x, "r:1");
            } catch (Throwable e) {
                Assert.assertTrue(true);
                System.out.println("fail, pass.");
            }
        }
    }

    private WeightFailover<Integer> buildNewFailover() {
        Map<Integer, Integer> weightMap = IntStream.range(1, 4).boxed()
                .collect(toMap(identity(), i -> 5));
        return WeightFailover.<Integer> newGenericBuilder() //
                .checker(alwaysFalse()) //
                .build(weightMap);
    }

    private String doSomething(Integer i) {
        if (i > 1) {
            throw new RuntimeException("failed");
        }
        return "r:" + i;
    }

    private String alwaysFail(Integer i) {
        if (i > 0) {
            throw new RuntimeException("failed");
        }
        return "r:" + i;
    }
}
