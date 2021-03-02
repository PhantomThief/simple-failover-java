package com.github.phantomthief.failover.impl;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


/**
 * @author huangli
 * Created on 2019-12-30
 */
final class GcUtil {

    private static final Logger logger = LoggerFactory.getLogger(GcUtil.class);

    private static final ConcurrentHashMap<Reference<Object>, Runnable> refMap = new ConcurrentHashMap<>();
    private static final ReferenceQueue<Object> REF_QUEUE = new ReferenceQueue<>();

    @VisibleForTesting
    static ConcurrentHashMap<Reference<Object>, Runnable> getRefMap() {
        return refMap;
    }

    public static void register(Object resource, Runnable cleaner) {
        if (resource != null && cleaner != null) {
            PhantomReference<Object> ref = new PhantomReference<>(resource, REF_QUEUE);
            refMap.put(ref, cleaner);
        }
    }

    public static void doClean() {
        Reference<?> ref = REF_QUEUE.poll();
        while (ref != null) {
            Runnable cleaner = refMap.remove(ref);
            try {
                cleaner.run();
            } catch (Throwable t) {
                logger.warn("Failover GC doClean failed", t);
            }
            ref = REF_QUEUE.poll();
        }
    }
}
