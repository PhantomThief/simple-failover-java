/**
 * 
 */
package com.github.phantomthief.failover.impl.checker;

import static org.slf4j.LoggerFactory.getLogger;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * @author w.vela
 */
public class SimplePortChecker {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;

    private static org.slf4j.Logger logger = getLogger(SimplePortChecker.class);

    @Deprecated
    private final int connectTimeout;

    /**
     * @param connectTimeout
     */
    @Deprecated
    private SimplePortChecker(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    private static class LazyHolder {

        private static final SimplePortChecker INSTANCE = new SimplePortChecker(
                DEFAULT_CONNECTION_TIMEOUT);
    }

    /**
     * better use static method {@code #check(String, int, int)}
     */
    @Deprecated
    public static SimplePortChecker withConnectTimeout(int connectTimeoutInMs) {
        return new SimplePortChecker(connectTimeoutInMs);
    }

    /**
     * better use static method {@code #check(String, int, int)}
     */
    @Deprecated
    public static SimplePortChecker getDefault() {
        return LazyHolder.INSTANCE;
    }

    public static boolean check(String host, int port) {
        return check(host, port, DEFAULT_CONNECTION_TIMEOUT);
    }

    public static boolean check(String host, int port, int connectionTimeoutInMs) {
        try (Socket socket = new Socket()) {
            SocketAddress sockaddr = new InetSocketAddress(host, port);
            socket.connect(sockaddr, connectionTimeoutInMs);
            logger.info("[{}:{}] is reachable.", host, port);
            return true;
        } catch (Throwable e) {
            logger.warn("[{}:{}] is NOT reachable.", host, port);
            return false;
        }
    }

    /**
     * better use static method {@code #check(String, int, int)}
     */
    @Deprecated
    public boolean test(String host, int port) {
        return SimplePortChecker.check(host, port, connectTimeout);
    }
}
