package com.github.phantomthief.failover.impl;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author huangli
 * Created on 2019-12-30
 */
final class GcUtil {

    private static ConcurrentHashMap<Reference<Object>, Runnable> refMap = new ConcurrentHashMap<>();
    private static final ReferenceQueue<Object> REF_QUEUE = new ReferenceQueue<>();

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
            cleaner.run();
            ref = REF_QUEUE.poll();
        }
    }
}
