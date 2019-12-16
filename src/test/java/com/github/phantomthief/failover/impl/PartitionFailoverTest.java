package com.github.phantomthief.failover.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.Failover;
import com.google.common.collect.ImmutableMap;

/**
 * @author huangli
 * Created on 2019-12-12
 */
class PartitionFailoverTest<T> {

    private Res r0;
    private Res r1;
    private Res r2;
    private Res r3;
    private Res r4;

    @BeforeEach
    public void setup() {
        r0 = new Res(0);
        r1 = new Res(1);
        r2 = new Res(2);
        r3 = new Res(3);
        r4 = new Res(4);
    }

    private static class Res {
        private final int index;
        private AtomicLong invokeCount = new AtomicLong();
        private boolean invokeDownWhenFail = false;
        private volatile long executeTime = 0;
        private volatile double failRate = 0;

        public Res(int index) {
            this.index = index;
        }

        public boolean execute(Failover<Res> failover, long sleepTime) {
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                }
            }
            boolean fail = ThreadLocalRandom.current().nextDouble() < failRate;
            if (fail) {
                if (invokeDownWhenFail) {
                    failover.down(this);
                } else {
                    failover.fail(this);
                }
            } else {
                failover.success(this);
            }
            invokeCount.incrementAndGet();
            return !fail;
        }

        public boolean execute(Failover<Res> failover) {
            return execute(failover, executeTime);
        }
    }

    @Test
    public void testPoolWithPartition() throws Exception {
        testPool1(3);
        setup();
        testPool2(3);
        setup();
        testPool3(3);
        setup();
        testPool4(3);

        setup();
        testPool1(1);
        setup();
        testPool2(1);
        setup();
        testPool3(1);
        setup();
        testPool4(1);
    }

    @Test
    public void testPoolWithoutPartition() throws Exception {
        testPool1(5);
        setup();
        testPool2(5);
        setup();
        testPool3(5);
        setup();
        testPool4(5);
    }

    private void testPool1(int coreSize) {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(coreSize)
                .reuseRecentResource(50)
                .build(Arrays.asList(r0, r1, r2, r3, r4));
        Res res = failover.getOneAvailable();
        res.execute(failover);
        int lastIndex = res.index;
        for (int i = 0; i < 100; i++) {
            res = failover.getOneAvailable();
            assertEquals(lastIndex, res.index);
            res.execute(failover);
        }
        failover.close();
    }

    private void testPool2(int coreSize) throws Exception {
        PartitionFailover<Res> failover = PartitionFailover.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(coreSize)
                .reuseRecentResource(5000)
                .build(Arrays.asList(r0, r1, r2, r3, r4), 50);
        final Res res = failover.getOneAvailable();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            countDownLatch.countDown();
            res.execute(failover, 1000);
        });
        t.start();
        countDownLatch.await();
        HashSet<Res> selectResSet = new HashSet<>();
        final int loopCount = 5000;
        for (int i = 0; i < loopCount; i++) {
            Res r = failover.getOneAvailable();
            r.execute(failover);
            selectResSet.add(r);
        }
        assertEquals(coreSize, selectResSet.size());
        for (Res r : selectResSet) {
            long totalWeight = 100 * coreSize - 50;
            if (r == res) {
                double expectCount = 50.0 / totalWeight * loopCount;
                assertEquals(expectCount, r.invokeCount.get(), expectCount * 0.1);
            } else {
                double expectCount = 100.0 / totalWeight * loopCount;
                assertEquals(expectCount, r.invokeCount.get(), expectCount * 0.1);
            }
        }
        t.interrupt();
        failover.close();
    }

    private void testPool3(int coreSize) {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(coreSize)
                .reuseRecentResource(1)
                .build(ImmutableMap.of(r0, 100, r1, 100, r2, 100, r3, 100, r4, 100));
        HashSet<Res> selectResSet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
            selectResSet.add(res);
        }
        assertEquals(coreSize, selectResSet.size());
        failover.close();
    }

    private void testPool4(int coreSize) {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(coreSize)
                .reuseRecentResource(0)
                .build(Arrays.asList(r0, r1, r2, r3, r4));
        HashSet<Res> selectResSet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            selectResSet.add(res);
        }
        assertEquals(coreSize, selectResSet.size());
        failover.close();
    }

    @Test
    public void testFailAndDown() {
        testFailWithPartition();

        setup();
        r0.invokeDownWhenFail = true;
        r1.invokeDownWhenFail = true;
        r2.invokeDownWhenFail = true;
        r3.invokeDownWhenFail = true;
        r4.invokeDownWhenFail = true;
        testFailWithPartition();

        setup();
        testFailWithoutPartition();

        setup();
        r0.invokeDownWhenFail = true;
        r1.invokeDownWhenFail = true;
        r2.invokeDownWhenFail = true;
        r3.invokeDownWhenFail = true;
        r4.invokeDownWhenFail = true;
        testFailWithoutPartition();
    }

    private void testFailWithPartition() {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(3)
                .failReduceRate(0.5)
                .reuseRecentResource(0)
                .build(Arrays.asList(r0, r1, r2, r3, r4));

        assertEquals(3, failover.getAvailable().size());
        assertEquals(0, failover.getFailed().size());
        {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            res.failRate = 1;
        }
        HashSet<Res> selectResSet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            selectResSet.add(res);
        }
        //replaceDownResource
        assertEquals(4, selectResSet.size());
        assertEquals(3, failover.getAvailable().size());
        assertEquals(1, failover.getFailed().size());

        {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            res.failRate = 1;
        }
        selectResSet.clear();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            selectResSet.add(res);
        }
        //replaceDownResource
        assertEquals(4, selectResSet.size());
        assertEquals(3, failover.getAvailable().size());
        assertEquals(2, failover.getFailed().size());

        {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            res.failRate = 1;
        }
        selectResSet.clear();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            selectResSet.add(res);
        }
        // no more backup available resource
        assertEquals(3, selectResSet.size());
        assertEquals(2, failover.getAvailable().size());
        assertEquals(3, failover.getFailed().size());

        selectResSet.forEach(r -> r.failRate = 1);
        selectResSet.clear();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            if (res != null) {
                res.execute(failover);
                selectResSet.add(res);
            }
        }
        assertEquals(2, selectResSet.size());
        assertEquals(0, failover.getAvailable().size());
        assertEquals(5, failover.getFailed().size());

        failover.close();
    }

    private void testFailWithoutPartition() {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(5)
                .failReduceRate(0.5)
                .reuseRecentResource(0)
                .build(Arrays.asList(r0, r1, r2, r3, r4));

        assertEquals(5, failover.getAvailable().size());
        assertEquals(0, failover.getFailed().size());
        {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            res.failRate = 1;
        }
        HashSet<Res> selectResSet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            res.execute(failover);
            selectResSet.add(res);
        }
        //replaceDownResource
        assertEquals(5, selectResSet.size());
        assertEquals(4, failover.getAvailable().size());
        assertEquals(1, failover.getFailed().size());

        selectResSet.forEach(r -> r.failRate = 1);
        selectResSet.clear();
        for (int i = 0; i < 100; i++) {
            Res res = failover.getOneAvailable();
            if (res != null) {
                res.execute(failover);
                selectResSet.add(res);
            }
        }
        assertEquals(4, selectResSet.size());
        assertEquals(0, failover.getAvailable().size());
        assertEquals(5, failover.getFailed().size());

        failover.close();
    }

    @Test
    public void testBadConcurrencyWithPartition() {
        for (int i = 0; i < 10; i++) {
            testBadConcurrencyWithoutPartition(3);
            testBadConcurrencyWithoutPartition(1);
        }
    }

    @Test
    public void testBadConcurrencyWithoutPartition() {
        for (int i = 0; i < 10; i++) {
            testBadConcurrencyWithoutPartition(5);
        }
    }

    private void testBadConcurrencyWithoutPartition(int coreSize) {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(coreSize)
                .reuseRecentResource(50)
                .build(Arrays.asList(r0, r1, r2, r3, r4));
        failover.down(r0);
        if (coreSize == failover.getAll().size()) {
            assertEquals(coreSize - 1, failover.getAvailable().size());
        } else {
            assertEquals(coreSize, failover.getAvailable().size());
        }

        failover.down(r1);
        failover.down(r2);
        failover.down(r3);
        failover.down(r4);

        assertEquals(0, failover.getAvailable().size());
        assertEquals(5, failover.getFailed().size());

        failover.close();
    }

    @Test
    public void testGetOneAvailableExclude() {
        testGetOneAvailableExclude(1);
        testGetOneAvailableExclude(3);
        testGetOneAvailableExclude(5);
    }

    private void testGetOneAvailableExclude(int coreSize) {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(coreSize)
                .reuseRecentResource(50)
                .build(Arrays.asList(r0, r1, r2, r3, r4));
        List<Res> list = new ArrayList<>();
        for (int i = 0; i < coreSize + 1; i++) {
            Res res = failover.getOneAvailableExclude(list);
            if (i == coreSize) {
                assertNull(res);
            } else {
                assertNotNull(res);
                list.add(res);
            }
        }
        failover.close();
    }

    @Test
    public void testNoSupportedMethod() {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .corePartitionSize(3)
                .reuseRecentResource(50)
                .build(Arrays.asList(r0, r1, r2, r3, r4));
        assertThrows(UnsupportedOperationException.class, () -> failover.getAvailable(2));
        assertThrows(UnsupportedOperationException.class, () -> failover.getAvailableExclude(emptyList()));
    }

    @Test
    public void testConcurrency() throws Exception {
        testConcurrency(3);
        testConcurrency(5);
    }

    private void testConcurrency(int coreSize) throws Exception {
        PartitionFailover<Res> failover = PartitionFailoverBuilder.<Res> newBuilder()
                .checker(r -> 1.0)
                .checkDuration(5, TimeUnit.MILLISECONDS)
                .corePartitionSize(coreSize)
                .reuseRecentResource(50)
                .build(Arrays.asList(r0, r1, r2, r3, r4));
        Res res = failover.getOneAvailable();
        res.execute(failover);

        Consumer<Res> fun = r -> r.failRate = r == res ? 0 : 0.05;
        fun.accept(r0);
        fun.accept(r1);
        fun.accept(r2);
        fun.accept(r3);
        fun.accept(r4);
        int threadCount = 50;
        Thread[] thread = new Thread[threadCount];
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean fail = new AtomicBoolean(false);
        for (int i = 0; i < threadCount; i++) {
            thread[i] = new Thread(() -> {
                while (!stop.get()) {
                    List<Res> failed = new ArrayList<>();
                    Res r = failover.getOneAvailableExclude(failed);
                    while (r != null) {
                        boolean ok = r.execute(failover);
                        if (!ok) {
                            failed.add(r);
                            r = failover.getOneAvailableExclude(failed);
                            if (r == null) {
                                fail.set(true);
                            }
                        } else {
                            break;
                        }
                    }
                }
            });
            thread[i].start();
        }
        Thread.sleep(1000);
        stop.set(true);
        for (int i = 0; i < threadCount; i++) {
            thread[i].join();
        }
        assertFalse(fail.get());
    }

}
