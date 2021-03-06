package com.github.phantomthief.failover.util;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.failover.impl.WeightFailover;
import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 * Created on 2017-06-09.
 */
class RetryTest {

    private static final Logger logger = LoggerFactory.getLogger(RetryTest.class);

    @Test
    void test() {
        WeightFailover<Integer> failover = buildNewFailover();
        for (int i = 0; i < 50; i++) {
            String x = failover.supplyWithRetry(this::doSomething);
            logger.debug("test:{}", x);
            assertEquals(x, "r:1");
        }
        failover = buildNewFailover();
        System.out.println("test fail");
        WeightFailover<Integer> thisFailover = failover;
        for (int i = 0; i < 50; i++) {
            assertThrows(Throwable.class, () -> {
                String x = thisFailover.supplyWithRetry(this::alwaysFail);
                logger.debug("test:{}", x);
            });
        }
        failover = buildNewFailover();
        logger.info("test some failed.");
        for (int i = 0; i < 50; i++) {
            try {
                String x = failover.supplyWithRetry(this::doSomething);
                logger.debug("test:{}", x);
                assertEquals(x, "r:1");
            } catch (Throwable e) {
                assertTrue(true);
                logger.info("fail, pass.");
            }
        }
    }

    @Test
    void testRetry(){
        WeightFailover<Integer> failover = buildNewFailover();
        assertThrows(TimeoutException.class, () -> failover.supplyWithRetry(i -> {
            throw new TimeoutException();
        }));
        WeightFailover<Integer> failover2 = buildNewFailover();
        assertThrows(ArithmeticException.class, () -> {
            ThrowableFunction<Integer, Object, TimeoutException> func = i -> {
                int j = 0;
                System.out.println(j / 0);
                throw new TimeoutException();
            };
            failover2.supplyWithRetry(func);
        });
    }

    @Test
    void testRunRetry() {
        WeightFailover<Integer> failover = buildNewFailover();
        for (int i = 0; i < 10; i++) {
            failover.runWithRetry(it -> {
                if (it == 1) {
                    throw new IllegalStateException();
                }
            });
        }
    }

    private WeightFailover<Integer> buildNewFailover() {
        Map<Integer, Integer> weightMap = IntStream.range(1, 4).boxed()
                .collect(toMap(identity(), i -> 5));
        return WeightFailover.<Integer> newGenericBuilder()
                .checker(it -> false, 1)
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
