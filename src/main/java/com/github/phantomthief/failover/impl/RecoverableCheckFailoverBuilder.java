/**
 * 
 */
package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public final class RecoverableCheckFailoverBuilder<T> {

    private int failCount;
    private long failDuration;
    private long recoveryCheckDuration;
    private boolean returnOriginalWhileAllFailed;
    private Predicate<T> checker;

    public RecoverableCheckFailoverBuilder<T> setFailCount(int failCount) {
        this.failCount = failCount;
        return this;
    }

    @SuppressWarnings("unchecked")
    public <E> RecoverableCheckFailoverBuilder<E> setChecker(Predicate<? super E> checker) {
        RecoverableCheckFailoverBuilder<E> thisBuilder = (RecoverableCheckFailoverBuilder<E>) this;
        thisBuilder.checker = thisBuilder.catching((Predicate<E>) checker);
        return thisBuilder;
    }

    public RecoverableCheckFailoverBuilder<T> setRecoveryCheckDuration(long recoveryCheckDuration, TimeUnit unit) {
        this.recoveryCheckDuration = unit.toMillis(recoveryCheckDuration);
        return this;
    }

    public RecoverableCheckFailoverBuilder<T> setFailDuration(long failDuration, TimeUnit unit) {
        this.failDuration = unit.toMillis(failDuration);
        return this;
    }

    public RecoverableCheckFailoverBuilder<T> setReturnOriginalWhileAllFailed(boolean returnOriginalWhileAllFailed) {
        this.returnOriginalWhileAllFailed = returnOriginalWhileAllFailed;
        return this;
    }

    @SuppressWarnings("unchecked")
    public <E> RecoverableCheckFailover<E> build(List<? extends E> original) {
        RecoverableCheckFailoverBuilder<E> thisBuilder = (RecoverableCheckFailoverBuilder<E>) this;
        thisBuilder.ensure();
        return new RecoverableCheckFailover<>((List<E>) original, thisBuilder.checker, failCount,
                failDuration, recoveryCheckDuration, returnOriginalWhileAllFailed);
    }

    private void ensure() {
        checkNotNull(checker);

        if (failCount <= 0) {
            failCount = RecoverableCheckFailover.DEFAULT_FAIL_COUNT;
        }
        if (failDuration <= 0) {
            failDuration = RecoverableCheckFailover.DEFAULT_FAIL_DURATION;
        }
        if (recoveryCheckDuration <= 0) {
            recoveryCheckDuration = RecoverableCheckFailover.DEFAULT_RECOVERY_CHECK_DURATION;
        }
    }

    private Predicate<T> catching(Predicate<T> predicate) {
        return t -> {
            try {
                return predicate.test(t);
            } catch (Throwable e) {
                RecoverableCheckFailover.logger.error("Ops. fail to test:{}", t, e);
                return false;
            }
        };
    }
}