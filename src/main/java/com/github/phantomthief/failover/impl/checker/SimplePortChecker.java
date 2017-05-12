/**
 * 
 */
package com.github.phantomthief.failover.impl.checker;

import static org.slf4j.LoggerFactory.getLogger;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.google.common.net.HostAndPort;

/**
 * @author w.vela
 */
public class SimplePortChecker {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;

    private static org.slf4j.Logger logger = getLogger(SimplePortChecker.class);

    public static boolean check(String host, int port) {
        return check(null, host, port);
    }

    public static boolean check(String friendlyName, String host, int port) {
        return check(friendlyName, host, port, DEFAULT_CONNECTION_TIMEOUT);
    }

    public static boolean check(HostAndPort hostAndPort) {
        return check(null, hostAndPort);
    }

    public static boolean check(String friendlyName, HostAndPort hostAndPort) {
        return check(friendlyName, hostAndPort.getHostText(), hostAndPort.getPort(),
                DEFAULT_CONNECTION_TIMEOUT);
    }

    public static boolean check(String host, int port, int connectionTimeoutInMs) {
        return check(null, host, port, connectionTimeoutInMs);
    }

    public static boolean check(String friendlyName, String host, int port,
            int connectionTimeoutInMs) {
        String logName = friendlyName == null ? "" : "[" + friendlyName + "]";
        try (Socket socket = new Socket()) {
            SocketAddress sockaddr = new InetSocketAddress(host, port);
            socket.connect(sockaddr, connectionTimeoutInMs);
            logger.info("{}[{}:{}] is reachable.", logName, host, port);
            return true;
        } catch (Throwable e) {
            logger.warn("{}[{}:{}] is NOT reachable.", logName, host, port);
            return false;
        }
    }
}
