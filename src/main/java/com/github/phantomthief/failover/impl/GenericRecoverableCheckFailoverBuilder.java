package com.github.phantomthief.failover.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

/**
 * @author w.vela
 */
@Deprecated
public class GenericRecoverableCheckFailoverBuilder<E> {

    private final RecoverableCheckFailoverBuilder<Object> builder;

    GenericRecoverableCheckFailoverBuilder(RecoverableCheckFailoverBuilder<Object> builder) {
        this.builder = builder;
    }

    @CheckReturnValue
    @Nonnull
    public RecoverableCheckFailoverBuilder<Object> setFailCount(int failCount) {
        return builder.setFailCount(failCount);
    }

    @CheckReturnValue
    @Nonnull
    public GenericRecoverableCheckFailoverBuilder<E> setChecker(Predicate<? super E> checker) {
        builder.setChecker(checker);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericRecoverableCheckFailoverBuilder<E>
            setRecoveryCheckDuration(long recoveryCheckDuration, TimeUnit unit) {
        builder.setRecoveryCheckDuration(recoveryCheckDuration, unit);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericRecoverableCheckFailoverBuilder<E> setFailDuration(long failDuration,
            TimeUnit unit) {
        builder.setFailDuration(failDuration, unit);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public GenericRecoverableCheckFailoverBuilder<E>
            setReturnOriginalWhileAllFailed(boolean returnOriginalWhileAllFailed) {
        builder.setReturnOriginalWhileAllFailed(returnOriginalWhileAllFailed);
        return this;
    }

    @Nonnull
    public RecoverableCheckFailover<E> build(List<? extends E> original) {
        return builder.build(original);
    }
}
