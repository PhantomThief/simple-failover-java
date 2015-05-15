/**
 * 
 */
package com.github.phantomthief.failover.impl.checker;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author w.vela
 */
public class SimplePortChecker {

    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(SimplePortChecker.class);

    private static final long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    public static boolean test(String host, int port) {
        SocketAddress sockaddr = new InetSocketAddress(host, port);
        try (Socket socket = new Socket()) {
            socket.connect(sockaddr, (int) DEFAULT_CONNECTION_TIMEOUT);
            logger.info("[{}:{}] is reachable.", host, port);
            return true;
        } catch (Throwable e) {
            logger.warn("[{}:{}] is NOT reachable.", host, port);
            return false;
        }
    }
}
