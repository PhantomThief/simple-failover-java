package com.github.phantomthief.failover.backoff;

/**
 * A simple {@link BackOff} implementation that provides a fixed interval
 * between two attempts and a maximum number of retries.
 */
public class FixedBackOff implements BackOff {

    /**
     * The default recovery interval: 5000 ms = 5 seconds.
     */
    public static final long DEFAULT_INTERVAL = 5000;

    /**
     * Constant value indicating an unlimited number of attempts.
     */
    public static final long UNLIMITED_ATTEMPTS = Long.MAX_VALUE;

    private long interval = DEFAULT_INTERVAL;

    private long maxAttempts = UNLIMITED_ATTEMPTS;


    /**
     * Create an instance with an interval of {@value #DEFAULT_INTERVAL}
     * ms and an unlimited number of attempts.
     */
    public FixedBackOff() {
    }

    /**
     * Create an instance.
     *
     * @param interval the interval between two attempts
     * @param maxAttempts the maximum number of attempts
     */
    public FixedBackOff(long interval, long maxAttempts) {
        this.interval = interval;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Set the interval between two attempts in milliseconds.
     */
    public void setInterval(long interval) {
        this.interval = interval;
    }

    /**
     * Return the interval between two attempts in milliseconds.
     */
    public long getInterval() {
        return this.interval;
    }

    /**
     * Set the maximum number of attempts in milliseconds.
     */
    public void setMaxAttempts(long maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * Return the maximum number of attempts in milliseconds.
     */
    public long getMaxAttempts() {
        return this.maxAttempts;
    }

    @Override
    public BackOffExecution start() {
        return new FixedBackOffExecution();
    }


    private class FixedBackOffExecution implements BackOffExecution {

        private long currentAttempts = 0;

        @Override
        public long nextBackOff() {
            this.currentAttempts++;
            if (this.currentAttempts <= getMaxAttempts()) {
                return getInterval();
            } else {
                return STOP;
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FixedBackOff{");
            sb.append("interval=").append(FixedBackOff.this.interval);
            String attemptValue = FixedBackOff.this.maxAttempts == Long.MAX_VALUE
                                  ? "unlimited"
                                  : String.valueOf(FixedBackOff.this.maxAttempts);
            sb.append(", currentAttempts=").append(this.currentAttempts);
            sb.append(", maxAttempts=").append(attemptValue);
            sb.append('}');
            return sb.toString();
        }
    }

}