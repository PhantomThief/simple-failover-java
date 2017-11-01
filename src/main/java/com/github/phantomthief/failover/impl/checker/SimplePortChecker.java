package com.github.phantomthief.failover.impl.checker;

import java.net.InetSocketAddress;
import java.net.Socket;

import com.google.common.net.HostAndPort;

/**
 * @author w.vela
 */
public class SimplePortChecker {

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
}
