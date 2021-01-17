package com.github.phantomthief.failover.util;

import static com.github.phantomthief.failover.WeighTestUtils.checkRatio;
import static com.github.phantomthief.failover.util.FailoverUtils.isHostUnavailable;
import static com.github.phantomthief.failover.util.FailoverUtils.runWithRetry;
import static com.github.phantomthief.failover.util.FailoverUtils.supplyWithRetry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.impl.WeightFailover;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;

/**
 * @author w.vela
 * Created on 2019-01-04.
 */
class FailoverUtilsTest {

    @Test
    void test() {
        test("localhost", 1, 100);
        test("111.222.222.112", 2, 100);
    }

    @Test
    void testRetry() {
        WeightFailover<String> failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> 1.0D)
                .failReduceRate(0.00001D)
                .build(ImmutableMap.of("s1", 1000, "s2", 2000));
        for (int i = 0; i < 100; i++) {
            runWithRetry(2, 10, failover, this::run);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("s2", supplyWithRetry(2, 10, failover, this::supply));
        }
        Multiset<Object> result = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            try {
                FailoverUtils.run(failover, this::run, t -> false);
                result.add("s2");
            } catch (IllegalStateException e) {
                result.add("s1");
            }
        }
        assertTrue(checkRatio(result.count("s2"), result.count("s1"), 2));
    }

    private void run(String client) {
        if (client.equals("s1")) {
            throw new IllegalStateException();
        }
    }

    private String supply(String client) {
        if (client.equals("s1")) {
            throw new IllegalStateException();
        } else {
            return client;
        }
    }

    private void test(String host, int port, int i) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), i);
        } catch (Throwable e) {
            assertTrue(isHostUnavailable(e));
        }
    }
}