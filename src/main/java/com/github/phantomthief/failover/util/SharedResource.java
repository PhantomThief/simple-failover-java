package com.github.phantomthief.failover.util;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

    public V register(K key, Function<K, V> factory) {
        synchronized (lock) {
            V v = resources.computeIfAbsent(key, factory);
            counters.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
            return v;
        }
    }

    public V get(K key) {
        return resources.get(key);
    }

    public V unregister(K key, ThrowableConsumer<V, Throwable> cleanup) {
        synchronized (lock) {
            AtomicInteger counter = counters.get(key);
            if (counter == null) {
                throw new IllegalStateException("non paired unregister call for key:" + key);
            }
            int count = counter.decrementAndGet();

            if (count < 0) { // impossible run into here
                throw new IllegalStateException("INVALID INTERNAL STATE:" + key);
            } else if (count > 0) { // wait others to unregister
                return resources.get(key);
            } else { // count == 0
                V removed = resources.remove(key);
                counters.remove(key);
                try {
                    cleanup.accept(removed);
                    logger.info("cleanup resource:{}->{}", key, removed);
                } catch (Throwable e) {
                    throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }
                return removed;
            }
        }
    }
}
