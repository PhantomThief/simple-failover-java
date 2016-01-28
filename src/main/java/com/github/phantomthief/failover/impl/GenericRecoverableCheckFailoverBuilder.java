/**
 * 
 */
package com.github.phantomthief.failover.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * @author w.vela
 */
public class GenericRecoverableCheckFailoverBuilder<E> {

    private final RecoverableCheckFailoverBuilder<Object> builder;

    GenericRecoverableCheckFailoverBuilder(RecoverableCheckFailoverBuilder<Object> builder) {
        this.builder = builder;
    }

    public RecoverableCheckFailoverBuilder<Object> setFailCount(int failCount) {
        return builder.setFailCount(failCount);
    }

    public GenericRecoverableCheckFailoverBuilder<E> setChecker(Predicate<? super E> checker) {
        builder.setChecker(checker);
        return this;
    }

    public GenericRecoverableCheckFailoverBuilder<E> setRecoveryCheckDuration(
            long recoveryCheckDuration, TimeUnit unit) {
        builder.setRecoveryCheckDuration(recoveryCheckDuration, unit);
        return this;
    }

    public GenericRecoverableCheckFailoverBuilder<E> setFailDuration(long failDuration,
            TimeUnit unit) {
        builder.setFailDuration(failDuration, unit);
        return this;
    }

    public GenericRecoverableCheckFailoverBuilder<E> setReturnOriginalWhileAllFailed(
            boolean returnOriginalWhileAllFailed) {
        builder.setReturnOriginalWhileAllFailed(returnOriginalWhileAllFailed);
        return this;
    }

    public RecoverableCheckFailover<E> build(List<? extends E> original) {
        return builder.build(original);
    }
}
