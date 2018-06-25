package com.github.phantomthief.failover.impl.checker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author w.vela
 */
public class SimplePortChecker {

    private static final Logger logger = LoggerFactory.getLogger(SimplePortChecker.class);
    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;

    public static boolean check(String host, int port) {
        return check(host, port, DEFAULT_CONNECTION_TIMEOUT);
    }

    public static boolean check(HostAndPort hostAndPort) {
        return check(hostAndPort.getHost(), hostAndPort.getPort());
    }

    public static boolean check(String host, int port, int connectionTimeoutInMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), connectionTimeoutInMs);
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    @Nonnull
    public static ListenableFuture<Void> asyncCheck(@Nonnull HostAndPort hostAndPort) {
        return asyncCheck(hostAndPort.getHost(), hostAndPort.getPort());
    }

    @Nonnull
    public static ListenableFuture<Void> asyncCheck(@Nonnull String host, @Nonnegative int port) {
        AsynchronousSocketChannel[] channels = { null };
        CheckListenableFuture future = new CheckListenableFuture(() -> channels[0]);
        try {
            InetSocketAddress hostAddress = new InetSocketAddress(host, port);
            AsynchronousSocketChannel client = AsynchronousSocketChannel.open();
            channels[0] = client;
            client.connect(hostAddress, null, new CompletionHandler<Void, Object>() {

                @Override
                public void completed(Void result, Object attachment) {
                    future.set(result);
                    try {
                        client.close();
                    } catch (IOException e) {
                        logger.error("", e);
                    }
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    future.setException(exc);
                    try {
                        client.close();
                    } catch (IOException e) {
                        logger.error("", e);
                    }
                }
            });
            return future;
        } catch (Throwable e) {
            future.setException(e);
            return future;
        }
    }

    private static class CheckListenableFuture extends AbstractFuture<Void> {

        private final Supplier<AsynchronousSocketChannel> client;

        private CheckListenableFuture(Supplier<AsynchronousSocketChannel> client) {
            this.client = client;
        }

        @Override
        protected boolean set(@Nullable Void value) {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }

        @Override
        public Void get(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException, ExecutionException {
            try {
                return super.get(timeout, unit);
            } catch (TimeoutException e) {
                cancel(true);
                AsynchronousSocketChannel channel = client.get();
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (IOException e1) {
                        logger.error("", e1);
                    }
                }
                throw e;
            }
        }
    }
}
