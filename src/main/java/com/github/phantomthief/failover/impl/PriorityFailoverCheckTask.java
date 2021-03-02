package com.github.phantomthief.failover.impl;

import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.failover.impl.PriorityFailover.GroupInfo;
import com.github.phantomthief.failover.impl.PriorityFailover.ResInfo;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.PriorityFailoverConfig;

/**
 * @author huangli
 * Created on 2020-01-20
 */
class PriorityFailoverCheckTask<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PriorityFailoverCheckTask.class);

    private final PriorityFailoverConfig<T> config;

    private final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

    private final HashMap<T, ResInfo<T>> resourcesMap;
    private final GroupInfo<T>[] groups;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 有时在 ResInfo 持有的资源对象中，会持有 failover 实例，以便调用 failover 的 success/fail 等方法
     * 一旦在注册到 GcUtil 的 CloseRunnable 中持有了 PriorityFailoverCheckTask 的引用，将导致 failover
     * 始终存在引用，无法被关闭和清理。所以这里单独构造一个 static class，避免隐式持有 this.resourcesMap。
     * 导致即使主动关闭了 failover，failover 也无法被 gc 掉
     */
    private static class CloseRunnable implements Runnable {
        private final AtomicBoolean closed;
        private final AtomicReference<ScheduledFuture<?>> futureRef;

        CloseRunnable(AtomicBoolean closed,
                AtomicReference<ScheduledFuture<?>> futureRef) {
            this.closed = closed;
            this.futureRef = futureRef;
        }

        @Override
        public void run() {
            // 这里代码和 close 一样，由于此时 failover 已经被 gc 掉了，所以不需要再加锁了
            closed.set(true);
            ScheduledFuture<?> scheduledFuture = futureRef.get();
            if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
                scheduledFuture.cancel(true);
            }
        }
    }

    PriorityFailoverCheckTask(PriorityFailoverConfig<T> config, PriorityFailover<T> failover) {
        this.config = config;
        this.resourcesMap = failover.getResourcesMap();
        this.groups = failover.getGroups();
        if (config.getChecker() != null) {
            if (config.isStartCheckTaskImmediately()) {
                ensureStart();
            }
            GcUtil.register(failover, new CloseRunnable(closed, futureRef));
            GcUtil.doClean();
        }
    }

    public void ensureStart() {
        if (futureRef.get() == null) {
            synchronized (this) {
                if (futureRef.get() == null && config.getChecker() != null) {
                    futureRef.set(config.getCheckExecutor().scheduleWithFixedDelay(
                            this, config.getCheckDuration().toMillis(),
                            config.getCheckDuration().toMillis(), TimeUnit.MILLISECONDS));
                }
            }
        }
    }

    @Override
    public void run() {
        // TODO run in multi threads mode?

        Thread currentThread = Thread.currentThread();
        String origName = currentThread.getName();
        if (config.getName() != null) {
            currentThread.setName(origName + "-[" + config.getName() + "]");
        }
        try {
            for (ResInfo<T> r : resourcesMap.values()) {
                try {
                    if (closed.get()) {
                        return;
                    }
                    if (config.getWeightFunction().needCheck(r.maxWeight,
                            r.minWeight, r.priority, r.currentWeight, r.resource)) {
                        boolean ok = config.getChecker().test(r.resource);
                        if (closed.get()) {
                            return;
                        }
                        PriorityFailover.updateWeight(ok, r, config, groups);
                    }
                } catch (Throwable e) {
                    // the test may fail, the user's onSuccess/onFail callback may fail
                    if (config.getName() == null) {
                        logger.error("failover check/updateWeight fail: {}", e.toString());
                    } else {
                        logger.error("failover({}) check/updateWeight fail: {}", config.getName(), e.toString());
                    }
                }
            }
        } finally {
            currentThread.setName(origName);
        }
    }

    public synchronized void close() {
        closed.set(true);
        ScheduledFuture<?> scheduledFuture = futureRef.get();
        if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
            scheduledFuture.cancel(true);
        }
    }

    boolean isClosed() {
        return closed.get();
    }
}
