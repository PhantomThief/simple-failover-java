package com.github.phantomthief.failover.util;

import static java.util.Map.Entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.exception.NoAvailableResourceException;
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
            assertTrue(all.contains(aware.supply(all, it -> it)));
        }
        for (int i = 0; i < 1000; i++) {
            checkIdlest(all, aware);
        }

        Map<String, Integer> concurrency = new ConcurrentHashMap<>();
        for (int i = 0; i < 1000; i++) {
            List<CountDownLatch> countDownLatches1 = new ArrayList<>();
            List<CountDownLatch> countDownLatches2 = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            for (int j = 0; j < 4; j++) {
                CountDownLatch c1 = new CountDownLatch(1);
                countDownLatches1.add(c1);
                CountDownLatch c2 = new CountDownLatch(1);
                countDownLatches2.add(c2);
                Thread thread = new Thread(() -> { //
                    try {
                        aware.run(all, it -> {
                            concurrency.merge(it, 1, Integer::sum);
                            c1.countDown();
                            c2.await();
                            concurrency.merge(it, -1, Integer::sum);
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (CountDownLatch c1 : countDownLatches1) {
                c1.await();
            }
            assertNotEquals(concurrency.entrySet().stream() //
                    .sorted(Comparator.<Entry<?, Integer>> comparingInt(Entry::getValue).reversed()) //
                    .map(Entry::getKey) //
                    .findAny() //
                    .orElse(null), aware.supply(all, it -> it));
            for (CountDownLatch c2 : countDownLatches2) {
                c2.countDown();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            concurrency.values().forEach(it -> assertEquals(0, it.intValue()));
        }

        assertThrows(NoAvailableResourceException.class,
                () -> aware.supply(Collections.emptyList(), it -> it));
    }

    private void checkIdlest(List<String> all, ConcurrencyAware<String> aware)
            throws InterruptedException {
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