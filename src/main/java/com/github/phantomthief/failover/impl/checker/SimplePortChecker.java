/**
 * 
 */
package com.github.phantomthief.failover.impl.checker;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * @author w.vela
 */
public class SimplePortChecker {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;

    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimplePortChecker.class);

    private final int connectTimeout;

    /**
     * @param connectTimeout
     */
    private SimplePortChecker(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    private static class LazyHolder {

        private static final SimplePortChecker INSTANCE = new SimplePortChecker(
                DEFAULT_CONNECTION_TIMEOUT);
    }

    public static SimplePortChecker withConnectTimeout(int connectTimeoutInMs) {
        return new SimplePortChecker(connectTimeoutInMs);
    }

    public static SimplePortChecker getDefault() {
        return LazyHolder.INSTANCE;
    }

    public boolean test(String host, int port) {
        SocketAddress sockaddr = new InetSocketAddress(host, port);
        try (Socket socket = new Socket()) {
            socket.connect(sockaddr, connectTimeout);
            logger.info("[{}:{}] is reachable.", host, port);
            return true;
        } catch (Throwable e) {
            logger.warn("[{}:{}] is NOT reachable.", host, port);
            return false;
        }
    }
}
