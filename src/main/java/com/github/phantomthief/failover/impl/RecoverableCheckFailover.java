package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.failover.util.SharedCheckExecutorHolder.getInstance;
import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.collect.EvictingQueue.create;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;

import org.slf4j.Logger;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.EvictingQueue;

/**
 * 一个简易的failover/failback策略类
 * failover条件是一段时间内出错次数超过一个阈值
 * failback策略是定期检查可用
 * 
 * @author w.vela
 */
public class RecoverableCheckFailover<T> implements Failover<T>, Closeable {

    private static Logger logger = getLogger(RecoverableCheckFailover.class);
    private final List<T> original;
    private final long failDuration;
    private final Set<T> failedList = new CopyOnWriteArraySet<>();
    private final LoadingCache<T, EvictingQueue<Long>> failCountMap;
    private final boolean returnOriginalWhileAllFailed;
    private final CloseableSupplier<ScheduledFuture<?>> recoveryFuture;

    private volatile boolean closed;

    RecoverableCheckFailover(List<T> original, Predicate<T> checker, int failCount,
            long failDuration, long recoveryCheckDuration, boolean returnOriginalWhileAllFailed) {
        this.returnOriginalWhileAllFailed = returnOriginalWhileAllFailed;
        this.original = original;
        this.failDuration = failDuration;
        this.failCountMap = CacheBuilder.newBuilder().weakKeys()
                .build(new CacheLoader<T, EvictingQueue<Long>>() {

                    @Override
                    public EvictingQueue<Long> load(T key) throws Exception {
                        return create(failCount);
                    }
                });
        recoveryFuture = lazy(() -> getInstance().scheduleWithFixedDelay(() -> {
            if (closed) {
                tryCloseScheduler();
                return;
            }
            if (failedList == null || failedList.isEmpty()) {
                return;
            }
            try {
                // 考虑到COWArraySet不支持iterator.remove，所以这里使用搜集->统一清理的策略
                List<T> covered = failedList.stream() //
                        .filter(checker) //
                        .peek(obj -> logger.info("obj:{} is recovered during test.", obj)) //
                        .collect(toList());
                failedList.removeAll(covered);
            } catch (Throwable e) {
                logger.error("Ops.", e);
            }
        }, recoveryCheckDuration, recoveryCheckDuration, MILLISECONDS));
    }

    public static RecoverableCheckFailoverBuilder<Object> newBuilder() {
        return new RecoverableCheckFailoverBuilder<>();
    }

    public static <E> GenericRecoverableCheckFailoverBuilder<E> newGenericBuilder() {
        return new GenericRecoverableCheckFailoverBuilder<>(newBuilder());
    }

    @Override
    public void fail(T object) {
        if (!getAll().contains(object)) {
            logger.warn("invalid fail obj:{}, it's not in original list.", object);
            return;
        }
        logger.warn("server {} failed.", object);
        boolean addToFail = false;
        EvictingQueue<Long> evictingQueue = failCountMap.getUnchecked(object);
        synchronized (evictingQueue) {
            evictingQueue.add(currentTimeMillis());
            if (evictingQueue.remainingCapacity() == 0
                    && evictingQueue.element() >= currentTimeMillis() - failDuration) {
                addToFail = true;
            }
        }
        if (addToFail) {
            failedList.add(object);
        }
        recoveryFuture.get();
    }

    @Override
    public void down(T object) {
        if (!getAll().contains(object)) {
            logger.warn("invalid fail obj:{}, it's not in original list.", object);
            return;
        }
        logger.warn("server {} down.", object);
        failedList.add(object);
        recoveryFuture.get();
    }

    @Override
    public List<T> getAvailable() {
        return getAvailableExclude(emptySet());
    }

    @Override
    public List<T> getAvailableExclude(Collection<T> exclusions) {
        List<T> availables = original.stream()
                .filter(obj -> !getFailed().contains(obj))
                .filter(obj -> !exclusions.contains(obj))
                .collect(toList());
        if (availables.isEmpty() && returnOriginalWhileAllFailed) {
            return original;
        } else {
            return availables;
        }
    }

    @Override
    public Set<T> getFailed() {
        return failedList;
    }

    @Override
    public List<T> getAll() {
        return original;
    }

    public synchronized void close() {
        closed = true;
        tryCloseScheduler();
    }

    private void tryCloseScheduler() {
        recoveryFuture.ifPresent(future -> {
            if (!future.isCancelled()) {
                if (!future.cancel(true)) {
                    logger.warn("fail to close failover:{}", this);
                }
            }
        });
    }

    @Override
    public String toString() {
        return "RecoverableCheckFailover [" + original + "]";
    }
}
