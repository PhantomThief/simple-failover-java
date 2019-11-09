package com.github.phantomthief.failover.backoff;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * FixedBackOffTests
 */
class FixedBackOffTests {

    @Test
    void defaultInstance() {
        FixedBackOff backOff = new FixedBackOff();
        BackOffExecution execution = backOff.start();
        for (int i = 0; i < 100; i++) {
            assertThat(execution.nextBackOff()).isEqualTo(FixedBackOff.DEFAULT_INTERVAL);
        }
    }

    @Test
    void noAttemptAtAll() {
        FixedBackOff backOff = new FixedBackOff(100L, 0L);
        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void maxAttemptsReached() {
        FixedBackOff backOff = new FixedBackOff(200L, 2);
        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(200L);
        assertThat(execution.nextBackOff()).isEqualTo(200L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void startReturnDifferentInstances() {
        FixedBackOff backOff = new FixedBackOff(100L, 1);
        BackOffExecution execution = backOff.start();
        BackOffExecution execution2 = backOff.start();

        assertThat(execution.nextBackOff()).isEqualTo(100L);
        assertThat(execution2.nextBackOff()).isEqualTo(100L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
        assertThat(execution2.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void liveUpdate() {
        FixedBackOff backOff = new FixedBackOff(100L, 1);
        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(100L);

        backOff.setInterval(200L);
        backOff.setMaxAttempts(2);

        assertThat(execution.nextBackOff()).isEqualTo(200L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void toStringContent() {
        FixedBackOff backOff = new FixedBackOff(200L, 10);
        BackOffExecution execution = backOff.start();
        assertThat(execution.toString()).isEqualTo("FixedBackOff{interval=200, currentAttempts=0, maxAttempts=10}");
        execution.nextBackOff();
        assertThat(execution.toString()).isEqualTo("FixedBackOff{interval=200, currentAttempts=1, maxAttempts=10}");
        execution.nextBackOff();
        assertThat(execution.toString()).isEqualTo("FixedBackOff{interval=200, currentAttempts=2, maxAttempts=10}");
    }

}
