/**
 * 
 */
package com.github.phantomthief.failover.util;

import java.util.function.Predicate;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.exception.NoAvailableResourceException;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;
import com.google.common.base.Throwables;
import com.google.common.reflect.Reflection;

/**
 * @author w.vela
 */
public class FailoverUtils {

    private FailoverUtils() {
        throw new UnsupportedOperationException();
    }

    public static <T, R, X extends Throwable> R run(Failover<T> failover,
            ThrowableFunction<T, R, X> func, Predicate<Throwable> failChecker) {
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
                throw Throwables.propagate(e);
            }
        } else {
            throw new NoAvailableResourceException();
        }
    }

    public static <T, X extends Throwable> void call(Failover<T> failover,
            ThrowableConsumer<T, X> func, Predicate<Throwable> failChecker) {
        T oneAvailable = failover.getOneAvailable();
        if (oneAvailable != null) {
            try {
                func.accept(oneAvailable);
                failover.success(oneAvailable);
            } catch (Throwable e) {
                if (failChecker == null || failChecker.test(e)) {
                    failover.fail(oneAvailable);
                } else {
                    failover.success(oneAvailable);
                }
                throw Throwables.propagate(e);
            }
        } else {
            throw new NoAvailableResourceException();
        }
    }

    public static <T, E> T proxy(Class<T> iface, T toProxyObject, Failover<E> failover,
            Predicate<Throwable> failChecker) {
        return Reflection.newProxy(iface, (proxy, method, args) -> run(failover,
                res -> method.invoke(res, args), failChecker));
    }
}
