package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.primitives.Ints.constrainToRange;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;

/**
 *
 * @author w.vela
 * @author huangli
 */
class WeightFailoverCheckTask<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WeightFailoverCheckTask.class);

    private final String failoverName;
    private final WeightFailoverBuilder<T> builder;
    private final AtomicBoolean closed;
    private final ConcurrentMap<T, Integer> initWeightMap;
    private final ConcurrentMap<T, Integer> currentWeightMap;
    private final AtomicInteger allAvailableVersion;

    private final CloseableSupplier<ScheduledFuture<?>> recoveryFuture;

    private final MyPhantomReference phantomReference;

    private static final ReferenceQueue REF_QUEUE = new ReferenceQueue();

    private static class MyPhantomReference extends PhantomReference {
        private WeightFailoverCheckTask task;
        public MyPhantomReference(Object referent, ReferenceQueue q, WeightFailoverCheckTask task) {
            super(referent, q);
            this.task = task;
        }
    }

    WeightFailoverCheckTask(WeightFailover<T> failover, WeightFailoverBuilder<T> builder, AtomicBoolean closed,
            ConcurrentMap<T, Integer> initWeightMap, ConcurrentMap<T, Integer> currentWeightMap,
            AtomicInteger allAvailableVersion) {
        this.failoverName = failover.toString();
        this.builder = builder;
        this.closed = closed;
        this.initWeightMap = initWeightMap;
        this.currentWeightMap = currentWeightMap;
        this.allAvailableVersion = allAvailableVersion;
        this.recoveryFuture = lazy(() -> SharedCheckExecutorHolder.getInstance().scheduleWithFixedDelay(
                this, builder.checkDuration, builder.checkDuration, MILLISECONDS));

        phantomReference = new MyPhantomReference(failover, REF_QUEUE, this);

        // do clean in constructor, so we don't need a thread
        doClean();
    }

    private static void doClean() {
        MyPhantomReference ref = (MyPhantomReference) REF_QUEUE.poll();
        while (ref != null) {
            ref.task.close();
            ref = (MyPhantomReference) REF_QUEUE.poll();
        }
    }

    public CloseableSupplier<ScheduledFuture<?>> lazyFuture() {
        return recoveryFuture;
    }

    public void close() {
        if (!closed.get()) {
            logger.warn("failover not released manually: {}", failoverName);
            closed.set(true);
            WeightFailover.tryCloseRecoveryScheduler(recoveryFuture, failoverName);
        }
    }

    @Override
    public void run() {
        if (closed.get()) {
            return;
        }
        Thread currentThread = Thread.currentThread();
        String origName = currentThread.getName();
        if (builder.name != null) {
            currentThread.setName(origName + "-[" + builder.name + "]");
        }
        try {
            Map<T, Double> recoveredObjects = new HashMap<>();
            this.currentWeightMap.forEach((obj, weight) -> {
                if (weight == 0) {
                    double recoverRate = builder.checker.applyAsDouble(obj);
                    if (recoverRate > 0) {
                        recoveredObjects.put(obj, recoverRate);
                    }
                }
            });
            if (!recoveredObjects.isEmpty()) {
                logger.info("found recovered objects:{}", recoveredObjects);
            }
            recoveredObjects.forEach((recovered, rate) -> {
                Integer initWeight = initWeightMap.get(recovered);
                if (initWeight == null) {
                    throw new IllegalStateException("obj:" + recovered);
                }
                int recoveredWeight = constrainToRange((int) (initWeight * rate), 1,
                        initWeight);
                currentWeightMap.put(recovered, recoveredWeight);
                allAvailableVersion.incrementAndGet();
                if (builder.onRecovered != null) {
                    builder.onRecovered.accept(recovered);
                }
            });
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            currentThread.setName(origName);
        }
    }
}
