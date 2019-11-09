package com.github.phantomthief.failover.backoff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

/**
 * ExponentialBackOffTests
 */
class ExponentialBackOffTests {

    @Test
    void defaultInstance() {
        ExponentialBackOff backOff = new ExponentialBackOff();
        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(3000L);
        assertThat(execution.nextBackOff()).isEqualTo(4500L);
    }

    @Test
    void simpleIncrease() {
        ExponentialBackOff backOff = new ExponentialBackOff(100L, 2.0);
        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(100L);
        assertThat(execution.nextBackOff()).isEqualTo(200L);
        assertThat(execution.nextBackOff()).isEqualTo(400L);
        assertThat(execution.nextBackOff()).isEqualTo(800L);
    }

    @Test
    void fixedIncrease() {
        ExponentialBackOff backOff = new ExponentialBackOff(100L, 1.0);
        backOff.setMaxElapsedTime(300L);

        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(100L);
        assertThat(execution.nextBackOff()).isEqualTo(100L);
        assertThat(execution.nextBackOff()).isEqualTo(100L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void maxIntervalReached() {
        ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
        backOff.setMaxInterval(4000L);

        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(4000L);
        // max reached
        assertThat(execution.nextBackOff()).isEqualTo(4000L);
        assertThat(execution.nextBackOff()).isEqualTo(4000L);
    }

    @Test
    void maxAttemptsReached() {
        ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
        backOff.setMaxElapsedTime(4000L);

        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(4000L);
        // > 4 sec wait in total
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void startReturnDifferentInstances() {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(2000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxElapsedTime(4000L);

        BackOffExecution execution = backOff.start();
        BackOffExecution execution2 = backOff.start();

        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution2.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(4000L);
        assertThat(execution2.nextBackOff()).isEqualTo(4000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
        assertThat(execution2.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void invalidInterval() {
        ExponentialBackOff backOff = new ExponentialBackOff();
        assertThatIllegalArgumentException().isThrownBy(() ->
                backOff.setMultiplier(0.9));
    }

    @Test
    void maxIntervalReachedImmediately() {
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(50L);

        BackOffExecution execution = backOff.start();
        assertThat(execution.nextBackOff()).isEqualTo(50L);
        assertThat(execution.nextBackOff()).isEqualTo(50L);
    }

    @Test
    void toStringContent() {
        ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
        BackOffExecution execution = backOff.start();
        assertThat(execution.toString()).isEqualTo("ExponentialBackOff{currentInterval=n/a, multiplier=2.0}");
        execution.nextBackOff();
        assertThat(execution.toString()).isEqualTo("ExponentialBackOff{currentInterval=2000ms, multiplier=2.0}");
        execution.nextBackOff();
        assertThat(execution.toString()).isEqualTo("ExponentialBackOff{currentInterval=4000ms, multiplier=2.0}");
    }

}
