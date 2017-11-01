package com.github.phantomthief.failover.exception;

/**
 * @author w.vela
 */
public class NoAvailableResourceException extends RuntimeException {

    private static final long serialVersionUID = -8580979490752713492L;

    public NoAvailableResourceException() {
        super();
    }

    public NoAvailableResourceException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoAvailableResourceException(String message) {
        super(message);
    }

    public NoAvailableResourceException(Throwable cause) {
        super(cause);
    }
}
