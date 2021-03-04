package com.github.phantomthief.failover.impl;

import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;
import com.google.common.collect.ImmutableMap;

/**
 * @author huangli
 * Created on 2019-12-17
 */
class WeightFailoverCheckTaskTest {
    @Test
    public void testGc() throws Exception {
        WeightFailover<String> failover = WeightFailover
                .<String>newGenericBuilder()
                .checker(o -> 1.0)
                .checkDuration(1, TimeUnit.SECONDS)
                .build(ImmutableMap.of("1", 1));
        AtomicBoolean closed = failover.closed;
        CloseableSupplier<ScheduledFuture<?>> recoveryFuture = failover.recoveryFuture;
        failover.down("1");
        Assertions.assertTrue(recoveryFuture.isInitialized());
        failover = null;

        for (int i = 0; i < 5000; i++) {
            byte[] bs = new byte[1 * 1024 * 1024];
        }
        System.gc();

        Thread.sleep((1 + WeightFailoverCheckTask.CLEAN_DELAY_SECONDS) * 1000);

        Assertions.assertTrue(closed.get());
        Assertions.assertTrue(recoveryFuture.get().isCancelled());
    }

    private static class MockResource {
        private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);
        private final String resource;
        private volatile WeightFailover<MockResource> failover;

        private MockResource(String resource) {
            this.resource = resource;
            INSTANCE_COUNTER.incrementAndGet();
            GcUtil.register(this, INSTANCE_COUNTER::decrementAndGet);
        }

        void setFailover(WeightFailover<MockResource> failover) {
            this.failover = failover;
        }
    }

    @Test
    public void testGc2() throws Exception {
        MockResource mockResource = new MockResource("str");
        WeightFailover<MockResource> failover = WeightFailover
                .<MockResource>newGenericBuilder()
                .checker(o -> 1.0)
                .checkDuration(1, TimeUnit.SECONDS)
                .build(Arrays.asList(mockResource));
        mockResource.setFailover(failover);
        CloseableSupplier<ScheduledFuture<?>> recoveryFuture = failover.recoveryFuture;
        failover.down(mockResource);
        Assertions.assertTrue(recoveryFuture.isInitialized());
        failover.close();
        failover = null;
        mockResource = null;

        for (int i = 0; i < 5000; i++) {
            byte[] bs = new byte[1 * 1024 * 1024];
        }
        System.gc();

        Thread.sleep((1 + WeightFailoverCheckTask.CLEAN_DELAY_SECONDS) * 1000);
        GcUtil.doClean();
        Assertions.assertEquals(0, MockResource.INSTANCE_COUNTER.get());
    }
}
