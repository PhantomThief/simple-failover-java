/**
 * 
 */
package com.github.phantomthief.failover.impl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.github.phantomthief.failover.Failover;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.EvictingQueue;

/**
 * 一个简易的failover/failback策略类
 * failover条件是一段时间内出错次数超过一个阈值
 * failback策略是直接一段时间后就认为恢复
 * 
 * 由于实际没啥卵用，所以废弃掉
 * 
 * @author w.vela
 */
@Deprecated
public class SimpleFailover<T> implements Failover<T> {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private static final int DEFAULT_FAIL_COUNT = 10;
    private static final long DEFAULT_FAIL_DURATION = TimeUnit.MINUTES.toMillis(1);
    private static final long DEFAULT_RECOVERY_DURATION = TimeUnit.MINUTES.toMillis(3);

    private final List<T> original;
    private final long failDuration;
    private final Cache<T, Boolean> failedList;
    private final LoadingCache<T, EvictingQueue<Long>> failCountMap;

    /**
     * @param original
     * @param failDuration
     * @param failedList
     * @param failCountMap
     */
    private SimpleFailover(List<T> original, int failCount, long failDuration,
            long recoveryDuration) {
        this.original = original;
        this.failDuration = failDuration;
        this.failedList = CacheBuilder.newBuilder().weakKeys()
                .expireAfterWrite(recoveryDuration, TimeUnit.MILLISECONDS).build();
        this.failCountMap = CacheBuilder.newBuilder().weakKeys()
                .build(new CacheLoader<T, EvictingQueue<Long>>() {

                    @Override
                    public EvictingQueue<Long> load(T key) throws Exception {
                        return EvictingQueue.create(failCount);
                    }
                });
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
        logger.trace("server {} failed.", object);
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
            failedList.put(object, Boolean.TRUE);
            logger.trace("server {} failed. add to fail list.", object);
        }
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getAvailable()
     */
    @Override
    public List<T> getAvailable() {
        return original.stream().filter(obj -> !getFailed().contains(obj))
                .collect(Collectors.toList());
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getFailed()
     */
    @Override
    public Set<T> getFailed() {
        return failedList.asMap().keySet();
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.failover.FailoverStrategy#getAll()
     */
    @Override
    public List<T> getAll() {
        return original;
    }

    public static final class Builder<T> {

        private long failDuration;
        private long recoveryDuration;
        private int failCount;

        public Builder<T> setFailDuration(long failDuration, TimeUnit unit) {
            this.failDuration = unit.toMillis(failDuration);
            return this;
        }

        public Builder<T> setRecoveryDuration(long recoveryDuration, TimeUnit unit) {
            this.recoveryDuration = unit.toMillis(recoveryDuration);
            return this;
        }

        public Builder<T> setFailCount(int failCount) {
            this.failCount = failCount;
            return this;
        }

        public SimpleFailover<T> build(List<T> original) {
            ensure();
            return new SimpleFailover<>(original, failCount, failDuration, recoveryDuration);
        }

        private void ensure() {
            if (failCount <= 0) {
                failCount = DEFAULT_FAIL_COUNT;
            }
            if (failDuration <= 0) {
                failDuration = DEFAULT_FAIL_DURATION;
            }
            if (recoveryDuration <= 0) {
                recoveryDuration = DEFAULT_RECOVERY_DURATION;
            }
        }

    }

    public static final <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

}
