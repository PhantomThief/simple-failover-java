package com.github.phantomthief.failover.impl;

import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.github.phantomthief.failover.impl.PriorityFailover.GroupInfo;
import com.github.phantomthief.failover.impl.PriorityFailover.ResInfo;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.PriorityFailoverConfig;

/**
 * @author huangli
 * Created on 2020-01-20
 */
class PriorityFailoverCheckTask<T> implements Runnable {

    private final PriorityFailoverConfig<T> config;

    private volatile ScheduledFuture<?> future;

    private final HashMap<T, ResInfo<T>> resourcesMap;
    private final GroupInfo<T>[] groups;

    private volatile boolean closed;

    PriorityFailoverCheckTask(PriorityFailoverConfig<T> config, PriorityFailover<T> failover) {
        this.config = config;
        this.resourcesMap = failover.getResourcesMap();
        this.groups = failover.getGroups();
        if (config.getChecker() != null) {
            if (config.isStartCheckTaskImmediately()) {
                ensureStart();
            }
            GcUtil.register(failover, this::close);
            GcUtil.doClean();
        } else {
            future = null;
        }
    }

    public void ensureStart() {
        if (future == null) {
            synchronized (this) {
                if (future == null && config.getChecker() != null) {
                    future =  config.getCheckExecutor().scheduleWithFixedDelay(
                            this, config.getCheckDuration().toMillis(),
                            config.getCheckDuration().toMillis(), TimeUnit.MILLISECONDS);
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
                    if (closed) {
                        return;
                    }
                    if (config.getWeightFunction().needCheck(r.maxWeight,
                            r.minWeight, r.priority, r.currentWeight, r.resource)) {
                        boolean ok = config.getChecker().test(r.resource);
                        if (closed) {
                            return;
                        }
                        PriorityFailover.updateWeight(ok, r, config, groups);
                    }
                } catch (Throwable ignore) {
                    // the test may fail, the user's onSuccess/onFail callback may fail
                }
            }
        } finally {
            currentThread.setName(origName);
        }
    }

    public synchronized void close() {
        closed = true;
        if (future != null && !future.isCancelled()) {
            future.cancel(true);
        }
    }

    boolean isClosed() {
        return closed;
    }
}
