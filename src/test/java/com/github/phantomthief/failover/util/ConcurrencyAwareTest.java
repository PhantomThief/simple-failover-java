package com.github.phantomthief.failover.util;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

/**
 * @author w.vela
 * Created on 2018-01-22.
 */
class ConcurrencyAwareTest {

    @Test
    void test() throws InterruptedException {
        List<String> all = ImmutableList.of("t1", "t2", "t3");
        ConcurrencyAware<String> aware = ConcurrencyAware.create();
        for (int i = 0; i < 1000; i++) {
            assertTrue(all.contains(aware.supply(all, it -> {
                System.out.println("select:" + it);
                return it;
            })));
        }
        for (int i = 0; i < 1000; i++) {
            AtomicReference<String> current = new AtomicReference<>();
            CountDownLatch c1 = new CountDownLatch(1);
            CountDownLatch c2 = new CountDownLatch(1);
            new Thread(() -> { //
                try {
                    aware.run(all, it -> {
                        current.set(it);
                        c1.countDown();
                        c2.await();
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
            c1.await();
            assertNotEquals(current.get(), aware.supply(all, it -> it));
            c2.countDown();
        }
    }
}