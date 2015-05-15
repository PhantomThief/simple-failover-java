/**
 * 
 */
package com.github.phantomthief.failover.impl;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.phantomthief.failover.Failover;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.EvictingQueue;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 一个简易的failover/failback策略类
 * failover条件是一段时间内出错次数超过一个阈值
 * failback策略是定期检查可用
 * 
 * @author w.vela
 */
public class RecoverableCheckFailover<T> implements Failover<T>, Closeable {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private static final int DEFAULT_FAIL_COUNT = 10;
    private static final long DEFAULT_FAIL_DURATION = TimeUnit.MINUTES.toMillis(1);
    private static final long DEFAULT_RECOVERY_CHECK_DURATION = TimeUnit.SECONDS.toMillis(5);

    private final List<T> original;
    private final long failDuration;
    private final Set<T> failedList = new CopyOnWriteArraySet<>();
    private final LoadingCache<T, EvictingQueue<Long>> failCountMap;
    private final boolean returnOriginalWhileAllFailed;
    private final ScheduledExecutorService scheduledExecutorService;

    private RecoverableCheckFailover(List<T> original, Predicate<T> checker, int failCount,
            long failDuration, long recoveryCheckDuration, boolean returnOriginalWhileAllFailed,
            ScheduledExecutorService scheduledExecutorService) {
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
        this.scheduledExecutorService = scheduledExecutorService;
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            if (failedList == null || failedList.isEmpty()) {
                return;
            }
            try {
                // 考虑到COWArraySet不支持iterator.remove，所以这里使用搜集->统一清理的策略
                List<T> covered = failedList.stream().filter(checker::test).peek(obj -> {
                    logger.info("obj:{} is recoveried during test.", obj);
                }).collect(Collectors.toList());
                failedList.removeAll(covered);
            } catch (Throwable e) {
                logger.error("Ops.", e);
            }
        } , recoveryCheckDuration, recoveryCheckDuration, TimeUnit.MILLISECONDS);
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
        try {
            EvictingQueue<Long> evictingQueue = failCountMap.get(object);
            synchronized (evictingQueue) {
                evictingQueue.add(System.currentTimeMillis());
                if (evictingQueue.remainingCapacity() == 0
                        && evictingQueue.element() >= System.currentTimeMillis() - failDuration) {
                    addToFail = true;
                }
            }
        } catch (ExecutionException e) {
            logger.error("Ops.", e);
        }
        if (addToFail) {
            failedList.add(object);
            logger.trace("server {} failed. add to fail list.", object);
        }
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getAvailable()
     */
    @Override
    public List<T> getAvailable() {
        List<T> availables = original.stream().filter(obj -> !getFailed().contains(obj))
                .collect(Collectors.toList());
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

    public void close() {
        synchronized (this) {
            if (scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
                MoreExecutors.shutdownAndAwaitTermination(scheduledExecutorService, 1,
                        TimeUnit.MINUTES);
            }
        }
    }

    @Override
    public String toString() {
        return "RecoverableCheckFailover [logger=" + logger + ", original=" + original
                + ", failDuration=" + failDuration + ", failedList=" + failedList
                + ", failCountMap=" + failCountMap + ", returnOriginalWhileAllFailed="
                + returnOriginalWhileAllFailed + ", scheduledExecutorService="
                + scheduledExecutorService + "]";
    }

    public static final class Builder<T> {

        private List<T> original;
        private int failCount;
        private long failDuration;
        private long recoveryCheckDuration;
        private boolean returnOriginalWhileAllFailed;
        private Predicate<T> checker;
        private ScheduledExecutorService scheduledExecutorService;

        public Builder<T> setFailCount(int failCount) {
            this.failCount = failCount;
            return this;
        }

        public Builder<T> setChecker(Predicate<T> checker) {
            this.checker = checker;
            return this;
        }

        public Builder<T> setRecoveryCheckDuration(long recoveryCheckDuration, TimeUnit unit) {
            this.recoveryCheckDuration = unit.toMillis(recoveryCheckDuration);
            return this;
        }

        public Builder<T> setOriginal(List<T> original) {
            this.original = original;
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

        public Builder<T> setScheduledExecutorService(
                ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        public RecoverableCheckFailover<T> build() {
            ensure();
            return new RecoverableCheckFailover<>(original, checker, failCount, failDuration,
                    recoveryCheckDuration, returnOriginalWhileAllFailed, scheduledExecutorService);
        }

        private void ensure() {
            if (original == null || original.isEmpty()) {
                throw new IllegalArgumentException("original list is empty.");
            }
            if (checker == null) {
                throw new NullPointerException("no checker found.");
            }
            if (failCount <= 0) {
                failCount = DEFAULT_FAIL_COUNT;
            }
            if (failDuration <= 0) {
                failDuration = DEFAULT_FAIL_DURATION;
            }
            if (recoveryCheckDuration <= 0) {
                recoveryCheckDuration = DEFAULT_RECOVERY_CHECK_DURATION;
            }
            if (scheduledExecutorService == null) {
                scheduledExecutorService = Executors.newScheduledThreadPool(1, r -> {
                    Thread thread = new Thread(r);
                    thread.setName("failover-check-thread-id-" + thread.getId());
                    return thread;
                });
            }
        }
    }

    public static final <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

}
