package com.github.phantomthief.failover.impl;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public void test() throws Exception {
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

        Thread.sleep(WeightFailoverCheckTask.CLEAN_INIT_DELAY_SECONDS * 1000);

        Assertions.assertTrue(closed.get());
        Assertions.assertTrue(recoveryFuture.get().isCancelled());
    }
}
