/**
 * 
 */
package com.github.phantomthief.failover.util;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.exception.NoAvailableResourceException;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 */
public class FailoverUtils {

    private FailoverUtils() {
        throw new UnsupportedOperationException();
    }

    public static <T, R, X extends Throwable> R supplyWithRetry(int maxRetryTimes,
            long sleepBetweenRetryMs, Failover<T> failover, ThrowableFunction<T, R, X> func,
            Predicate<Throwable> failChecker) throws X {
        Set<T> failed = new HashSet<>();
        Throwable lastError = null;
        for (int i = 0; i < maxRetryTimes; i++) {
            T oneAvailable = failover.getOneAvailableExclude(failed);
            if (oneAvailable != null) {
                try {
                    R result = func.apply(oneAvailable);
                    failover.success(oneAvailable);
                    return result;
                } catch (Throwable e) {
                    if (failChecker == null || failChecker.test(e)) {
                        failover.fail(oneAvailable);
                        failed.add(oneAvailable);
                        if (sleepBetweenRetryMs > 0) {
                            sleepUninterruptibly(sleepBetweenRetryMs, MILLISECONDS);
                        }
                        lastError = e;
                        continue;
                    } else {
                        failover.success(oneAvailable);
                    }
                    throw e;
                }
            } else {
                throw new NoAvailableResourceException();
            }
        }
        throw propagate(lastError);
    }

    public static <T, R, X extends Throwable> R supply(Failover<T> failover,
            ThrowableFunction<T, R, X> func, Predicate<Throwable> failChecker) throws X {
        T oneAvailable = failover.getOneAvailable();
        if (oneAvailable != null) {
            try {
                R result = func.apply(oneAvailable);
                failover.success(oneAvailable);
                return result;
            } catch (Throwable e) {
                if (failChecker == null || failChecker.test(e)) {
                    failover.fail(oneAvailable);
                } else {
                    failover.success(oneAvailable);
                }
                throw e;
            }
        } else {
            throw new NoAvailableResourceException();
        }
    }

    public static <T, X extends Throwable> void runWithRetry(int maxRetryTimes,
            long sleepBetweenRetryMs, Failover<T> failover, ThrowableConsumer<T, X> func,
            Predicate<Throwable> failChecker) throws X {
        supplyWithRetry(maxRetryTimes, sleepBetweenRetryMs, failover, t -> {
            func.accept(t);
            return null;
        }, failChecker);
    }

    public static <T, X extends Throwable> void run(Failover<T> failover,
            ThrowableConsumer<T, X> func, Predicate<Throwable> failChecker) throws X {
        supply(failover, t -> {
            func.accept(t);
            return null;
        }, failChecker);
    }

    public static <T> Failover<T> singleton(Failover<T> parent, T o) {
        return new Failover<T>() {

            @Override
            public List<T> getAll() {
                return singletonList(o);
            }

            @Override
            public void fail(T object) {
                parent.fail(object);
            }

            @Override
            public List<T> getAvailable() {
                List<T> parentAvailable = parent.getAvailable();
                if (parentAvailable.contains(o)) {
                    return singletonList(o);
                } else {
                    return emptyList();
                }
            }

            @Override
            public Set<T> getFailed() {
                Set<T> parentFailed = parent.getFailed();
                if (parentFailed.contains(o)) {
                    return Collections.singleton(o);
                } else {
                    return emptySet();
                }
            }
        };
    }
}
