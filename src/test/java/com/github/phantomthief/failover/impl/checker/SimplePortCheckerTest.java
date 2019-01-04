package com.github.phantomthief.failover.impl.checker;

import static com.github.phantomthief.failover.impl.checker.SimplePortChecker.asyncCheck;
import static com.github.phantomthief.failover.impl.checker.SimplePortChecker.check;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author w.vela
 * Created on 2018-06-25.
 */
class SimplePortCheckerTest {

    private ServerSocket serverSocket;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        port = ThreadLocalRandom.current().nextInt(1025, 10240);
        serverSocket = new ServerSocket(port);
    }

    @Test
    void testSync() {
        assertTrue(check("localhost", port, 100));
    }

    @Test
    void testAsync() throws InterruptedException, ExecutionException, TimeoutException {
        ListenableFuture<Void> future = asyncCheck("localhost", port);
        assertThrows(TimeoutException.class, () -> future.get(1, NANOSECONDS));
        assertThrows(CancellationException.class, future::get);

        ListenableFuture<Void> future2 = asyncCheck("localhost", port);
        future2.get(1, SECONDS);
    }

    @AfterEach
    void tearDown() throws IOException {
        serverSocket.close();
    }
}