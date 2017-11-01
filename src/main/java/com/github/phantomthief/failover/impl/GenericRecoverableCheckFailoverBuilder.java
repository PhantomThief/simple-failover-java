package com.github.phantomthief.failover.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.CheckReturnValue;

/**
 * @author w.vela
 */
public class GenericRecoverableCheckFailoverBuilder<E> {

    private final RecoverableCheckFailoverBuilder<Object> builder;

    GenericRecoverableCheckFailoverBuilder(RecoverableCheckFailoverBuilder<Object> builder) {
        this.builder = builder;
    }

    @CheckReturnValue
    public RecoverableCheckFailoverBuilder<Object> setFailCount(int failCount) {
        return builder.setFailCount(failCount);
    }

    @CheckReturnValue
    public GenericRecoverableCheckFailoverBuilder<E> setChecker(Predicate<? super E> checker) {
        builder.setChecker(checker);
        return this;
    }

    @CheckReturnValue
    public GenericRecoverableCheckFailoverBuilder<E>
            setRecoveryCheckDuration(long recoveryCheckDuration, TimeUnit unit) {
        builder.setRecoveryCheckDuration(recoveryCheckDuration, unit);
        return this;
    }

    @CheckReturnValue
    public GenericRecoverableCheckFailoverBuilder<E> setFailDuration(long failDuration,
            TimeUnit unit) {
        builder.setFailDuration(failDuration, unit);
        return this;
    }

    @CheckReturnValue
    public GenericRecoverableCheckFailoverBuilder<E>
            setReturnOriginalWhileAllFailed(boolean returnOriginalWhileAllFailed) {
        builder.setReturnOriginalWhileAllFailed(returnOriginalWhileAllFailed);
        return this;
    }

    public RecoverableCheckFailover<E> build(List<? extends E> original) {
        return builder.build(original);
    }
}
