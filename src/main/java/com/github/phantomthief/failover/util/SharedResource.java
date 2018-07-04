package com.github.phantomthief.failover.util;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableConsumer;

/**
 * 多处使用的资源, 需要在所有使用者都注销之后, 再进行清理, 类似于引用计数.
 *
 * TODO: 其实和netty的io.netty.util.ReferenceCounted很相似
 * 回头可以考虑使用和netty一样的方式，使用无锁的方式……
 *
 * TODO: 回头挪到common-util包中
 *
 * @author w.vela
 * Created on 16/2/19.
 */
public class SharedResource<K, V> {

    private static final Logger logger = getLogger(SharedResource.class);
    private final ConcurrentMap<K, V> resources = new ConcurrentHashMap<>();

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ConcurrentMap<K, AtomicInteger> counters = new ConcurrentHashMap<>();

    @Nonnull
    public V register(@Nonnull K key, @Nonnull Function<K, V> factory) {
        synchronized (lock) {
            V v = resources.computeIfAbsent(key, factory);
            counters.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
            return v;
        }
    }

    @Nullable
    public V get(@Nonnull K key) {
        return resources.get(key);
    }

    /**
     * @throws UnregisterFailedException if specified cleanup function failed to run.
     * @throws IllegalStateException if there is a illegal state found.(e.g. non paired call unregister)
     */
    @Nonnull
    public V unregister(@Nonnull K key, @Nonnull ThrowableConsumer<V, Throwable> cleanup) {
        synchronized (lock) {
            AtomicInteger counter = counters.get(key);
            if (counter == null) {
                throw new IllegalStateException("non paired unregister call for key:" + key);
            }
            int count = counter.decrementAndGet();

            if (count < 0) { // impossible run into here
                throw new AssertionError("INVALID INTERNAL STATE:" + key);
            } else if (count > 0) { // wait others to unregister
                return resources.get(key);
            } else { // count == 0
                V removed = resources.remove(key);
                counters.remove(key);
                try {
                    cleanup.accept(removed);
                    logger.info("cleanup resource:{}->{}", key, removed);
                } catch (Throwable e) {
                    throw new UnregisterFailedException(e, removed);
                }
                return removed;
            }
        }
    }

    public static class UnregisterFailedException extends RuntimeException {

        private final Object removed;

        private UnregisterFailedException(Throwable cause, Object removed) {
            super(cause);
            this.removed = removed;
        }

        @SuppressWarnings("unchecked")
        public <T> T getRemoved() {
            return (T) removed;
        }
    }
}
