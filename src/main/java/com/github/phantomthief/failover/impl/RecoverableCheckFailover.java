/**
 * 
 */
package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;
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

    private static org.slf4j.Logger logger = getLogger(RecoverableCheckFailover.class);
    private static final int DEFAULT_FAIL_COUNT = 10;
    private static final long DEFAULT_FAIL_DURATION = MINUTES.toMillis(1);
    private static final long DEFAULT_RECOVERY_CHECK_DURATION = SECONDS.toMillis(5);

    private final List<T> original;
    private final long failDuration;
    private final Set<T> failedList = new CopyOnWriteArraySet<>();
    private final LoadingCache<T, EvictingQueue<Long>> failCountMap;
    private final boolean returnOriginalWhileAllFailed;
    private final ScheduledFuture<?> recoveryFuture;

    private RecoverableCheckFailover(List<T> original, Predicate<T> checker, int failCount,
            long failDuration, long recoveryCheckDuration, boolean returnOriginalWhileAllFailed) {
        this.returnOriginalWhileAllFailed = returnOriginalWhileAllFailed;
        this.original = original;
        this.failDuration = failDuration;
        this.failCountMap = CacheBuilder.newBuilder().weakKeys()
                .build(new CacheLoader<T, EvictingQueue<Long>>() {

                    @Override
                    public EvictingQueue<Long> load(T key) throws Exception {
                        return EvictingQueue.create(failCount);
                    }
                });
        recoveryFuture = SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(() -> {
            if (failedList == null || failedList.isEmpty()) {
                return;
            }
            try {
                // 考虑到COWArraySet不支持iterator.remove，所以这里使用搜集->统一清理的策略
                List<T> covered = failedList.stream() //
                        .filter(checker::test) //
                        .peek(obj -> logger.info("obj:{} is recovered during test.", obj)) //
                        .collect(toList());
                failedList.removeAll(covered);
            } catch (Throwable e) {
                logger.error("Ops.", e);
            }
        } , recoveryCheckDuration, recoveryCheckDuration, MILLISECONDS);
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#fail(java.lang.Object)
     */
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
            evictingQueue.add(System.currentTimeMillis());
            if (evictingQueue.remainingCapacity() == 0
                    && evictingQueue.element() >= System.currentTimeMillis() - failDuration) {
                addToFail = true;
            }
        }
        if (addToFail) {
            failedList.add(object);
            if (logger.isTraceEnabled()) {
                logger.trace("server {} failed. add to fail list.", object);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getAvailable()
     */
    @Override
    public List<T> getAvailable() {
        List<T> availables = original.stream().filter(obj -> !getFailed().contains(obj))
                .collect(toList());
        if ((availables == null || availables.isEmpty()) && returnOriginalWhileAllFailed) {
            return original;
        } else {
            return availables;
        }
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getFailed()
     */
    @Override
    public Set<T> getFailed() {
        return failedList;
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getAll()
     */
    @Override
    public List<T> getAll() {
        return original;
    }

    public synchronized void close() {
        if (!recoveryFuture.isCancelled()) {
            if (!recoveryFuture.cancel(true)) {
                logger.warn("fail to close failover:{}", this);
            }
        }
    }

    @Override
    public String toString() {
        return "RecoverableCheckFailover [" + original + "]";
    }

    public static final class Builder<T> {

        private int failCount;
        private long failDuration;
        private long recoveryCheckDuration;
        private boolean returnOriginalWhileAllFailed;
        private Predicate<T> checker;

        public Builder<T> setFailCount(int failCount) {
            this.failCount = failCount;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <E> Builder<E> setChecker(Predicate<E> checker) {
            Builder<E> thisBuilder = (Builder<E>) this;
            thisBuilder.checker = thisBuilder.catching(checker);
            return thisBuilder;
        }

        public Builder<T> setRecoveryCheckDuration(long recoveryCheckDuration, TimeUnit unit) {
            this.recoveryCheckDuration = unit.toMillis(recoveryCheckDuration);
            return this;
        }

        public Builder<T> setFailDuration(long failDuration, TimeUnit unit) {
            this.failDuration = unit.toMillis(failDuration);
            return this;
        }

        public Builder<T> setReturnOriginalWhileAllFailed(boolean returnOriginalWhileAllFailed) {
            this.returnOriginalWhileAllFailed = returnOriginalWhileAllFailed;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <E> RecoverableCheckFailover<E> build(List<E> original) {
            Builder<E> thisBuilder = (Builder<E>) this;
            thisBuilder.ensure();
            return new RecoverableCheckFailover<>(original, thisBuilder.checker, failCount,
                    failDuration, recoveryCheckDuration, returnOriginalWhileAllFailed);
        }

        private void ensure() {
            checkNotNull(checker);

            if (failCount <= 0) {
                failCount = DEFAULT_FAIL_COUNT;
            }
            if (failDuration <= 0) {
                failDuration = DEFAULT_FAIL_DURATION;
            }
            if (recoveryCheckDuration <= 0) {
                recoveryCheckDuration = DEFAULT_RECOVERY_CHECK_DURATION;
            }
        }

        private Predicate<T> catching(Predicate<T> predicate) {
            return t -> {
                try {
                    return predicate.test(t);
                } catch (Throwable e) {
                    logger.error("Ops. fail to test:{}", t, e);
                    return false;
                }
            };
        }
    }

    public static Builder<Object> newBuilder() {
        return new Builder<>();
    }
}
