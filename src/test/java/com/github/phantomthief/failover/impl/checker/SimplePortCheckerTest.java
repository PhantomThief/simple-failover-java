package com.github.phantomthief.failover.impl.checker;

import static com.github.phantomthief.failover.impl.checker.SimplePortChecker.asyncCheck;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author w.vela
 * Created on 2018-06-25.
 */
class SimplePortCheckerTest {

    @Disabled
    @Test
    void test() throws InterruptedException, ExecutionException, TimeoutException {
        ListenableFuture<Void> future = asyncCheck("www.baidu.com", 80);
        assertThrows(TimeoutException.class, () -> future.get(1, MILLISECONDS));
        assertThrows(CancellationException.class, future::get);

        ListenableFuture<Void> future2 = asyncCheck("www.baidu.com", 80);
        future2.get(1, SECONDS);
        sleepUninterruptibly(1, MINUTES);
    }
}