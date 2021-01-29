package com.github.phantomthief.failover.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.impl.PriorityFailover.ResStatus;

/**
 * @author huangli
 * Created on 2020-01-20
 */
class PriorityFailoverTest {
    private Object o0 = "o0";
    private Object o1 = "o1";
    private Object o2 = "o2";

    @Test
    public void testEmpty() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder().build();
        assertNull(failover.getOneAvailable());
    }

    @Test
    public void testSimple() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .addResource(o2, 100, 0, 0, 80)
                .weightFunction(new SimpleWeightFunction<>(0.05, 0.01))
                .build();
        for (int i = 0; i < 20; i++) {
            ArrayList<Object> list = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                Object one = failover.getOneAvailableExclude(list);
                assertNotNull(one);
                if (one == o0) {
                    failover.fail(one);
                } else {
                    failover.success(one);
                }
                list.add(one);
            }
            assertNull(failover.getOneAvailableExclude(list));
        }

        int o1Count = 0, o2Count = 0;
        for (int i = 0; i < 50000; i++) {
            Object one = failover.getOneAvailable();
            if (one == o1) {
                o1Count++;
            } else if (one == o2) {
                o2Count++;
            } else {
                Assertions.fail();
            }
        }

        assertEquals(1.0, 1.0 * o1Count / o2Count, 0.03);
        failover.close();
    }

    @Test
    public void testSimplePriority() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 1, 100)
                .addResource(o2, 100, 0, 2, 100)
                .priorityFactor(2.0)
                .weightFunction(new SimpleWeightFunction<>(0.05, 0.01))
                .build();
        for (int i = 0; i < 10; i++) {
            Object o = failover.getOneAvailable();
            assertSame(o0, o);
            failover.fail(o);
        }
        HashSet<Object> set = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            Object o = failover.getOneAvailable();
            assertNotNull(o);
            failover.fail(o);
            set.add(o);
        }
        assertEquals(3, set.size());
        assertNull(failover.getOneAvailable());
        failover.close();
    }

    @Test
    public void testZeroMaxWeight() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 0, 0, 0)
                .addResource(o1, 0, 0, 0)
                .build();
        assertNull(failover.getOneAvailable());
    }

    @Test
    public void testZeroMaxWeight2() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 0, 0, 0)
                .addResource(o1, 100, 0, 0)
                .build();
        for (int i = 0; i < 100; i++) {
            assertSame(o1, failover.getOneAvailable());
        }
    }

    @Test
    public void testZeroMaxWeight3() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 0, 0, 0)
                .addResource(o1, 0, 0, 0)
                .addResource(o2, 100, 1, 0)
                .build();
        for (int i = 0; i < 100; i++) {
            assertSame(o2, failover.getOneAvailable());
        }
    }

    @Test
    public void testGetResourceStatus(){
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 1, 100)
                .addResource(o2, 100, 0, 2, 100)
                .build();
        Object getO2 = failover.getOneAvailableExclude(Arrays.asList(o0, o1));
        failover.down(getO2);
        ResStatus s = failover.getResourceStatus(getO2);
        assertEquals(100, s.getMaxWeight());
        assertEquals(0, s.getMinWeight());
        assertEquals(2, s.getPriority());
        assertEquals(0, s.getCurrentWeight());
        assertEquals(0, s.getConcurrency());
    }

    @Test
    public void testSelectGroup() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try (PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 1, 100)
                .priorityFactor(2.0)
                .build()) {
            for (int i = 0; i < 10; i++) {
                assertEquals(0, failover.selectGroup(random));
            }
        }
        try (PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 30)
                .addResource(o1, 100, 0, 1, 100)
                .priorityFactor(2.0)
                .build()) {
            int c0 = 0, c1 = 0;
            int totalCount = 10000;
            for (int i = 0; i < totalCount; i++) {
                int x = failover.selectGroup(random);
                if (x == 0) {
                    c0++;
                } else {
                    c1++;
                }
            }
            assertEquals(30 * 2.0 / 100, 1.0 * c0 / totalCount, 0.03);
        }
        try (PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 25)
                .addResource(o1, 100, 0, 1, 20)
                .addResource(o2, 100, 0, 2, 100)
                .priorityFactor(2.0)
                .build()) {
            int c0 = 0, c1 = 0, c2 = 0;
            int totalCount = 50000;
            for (int i = 0; i < totalCount; i++) {
                int x = failover.selectGroup(random);
                if (x == 0) {
                    c0++;
                } else if (x == 1) {
                    c1++;
                } else {
                    c2++;
                }
            }
            double expectCount0Percent = 25 * 2.0 / 100;
            double expectCount1Percent = (1 - expectCount0Percent) * (20 * 2.0 / 100);
            double expectCount2Percent = 1 - expectCount0Percent - expectCount1Percent;
            assertEquals(expectCount0Percent, 1.0 * c0 / totalCount, 0.03);
            assertEquals(expectCount1Percent, 1.0 * c1 / totalCount, 0.03);
            assertEquals(expectCount2Percent, 1.0 * c2 / totalCount, 0.03);
        }
        try (PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 25)
                .addResource(o1, 100, 0, 1, 20)
                .addResource(o2, 100, 0, 2, 0)
                .priorityFactor(2.0)
                .build()) {
            int c0 = 0, c1 = 0, c2 = 0;
            int totalCount = 10000;
            for (int i = 0; i < totalCount; i++) {
                int x = failover.selectGroup(random);
                if (x == 0) {
                    c0++;
                } else if (x == 1) {
                    c1++;
                } else {
                    c2++;
                }
            }
            double expectCount0Percent = 25 * 2.0 / 100;
            double expectCount1Percent = (1 - expectCount0Percent) * (20 * 2.0 / 100);
            expectCount0Percent += 1 - expectCount0Percent - expectCount1Percent;
            assertEquals(expectCount0Percent, 1.0 * c0 / totalCount, 0.03);
            assertEquals(expectCount1Percent, 1.0 * c1 / totalCount, 0.03);
            assertEquals(0, 1.0 * c2 / totalCount, 0.05);
        }
    }

    @Test
    public void testConcurrency() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .concurrencyControl(true)
                .build();
        failover.getOneAvailableExclude(Collections.singleton(o1));
        failover.getOneAvailableExclude(Collections.singleton(o1));
        failover.getOneAvailableExclude(Collections.singleton(o1));
        assertEquals(3, failover.getResourceStatus(o0).getConcurrency());

        int c0 = 0, c1 = 0;
        int totalCount = 10000;
        for (int i = 0; i < totalCount; i++) {
            Object o = failover.getOneAvailable();
            if (o == o0) {
                c0++;
                failover.success(o);
            } else {
                c1++;
                failover.success(o);
            }
        }
        assertEquals(0.2, 1.0 * c0 / totalCount, 0.03);
        assertEquals(0.8, 1.0 * c1 / totalCount, 0.03);

        failover.close();
    }

    @Test
    public void testManualConcurrencyCtrl() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .concurrencyControl(true)
                .manualConcurrencyControl(true)
                .build();

        Object one = failover.getOneAvailable();
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());
        failover.incrConcurrency(one);
        failover.incrConcurrency(one);
        failover.incrConcurrency(one);
        assertEquals(3, failover.getResourceStatus(one).getConcurrency());
        failover.decrConcurrency(one);
        assertEquals(2, failover.getResourceStatus(one).getConcurrency());
        failover.resetConcurrency(one);
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());

        one = failover.getOneAvailable();
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());
        failover.success(one);
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());

        one = failover.getOneAvailable();
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());
        failover.fail(one);
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());

        one = failover.getOneAvailable();
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());
        failover.down(one);
        assertEquals(0, failover.getResourceStatus(one).getConcurrency());
    }

    @Test
    public void testDown() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 1, 100)
                .addResource(o2, 100, 0, 2, 100)
                .build();
        assertEquals(o0, failover.getOneAvailable());
        failover.down(o0);
        assertEquals(o1, failover.getOneAvailable());
        failover.down(o1);
        assertEquals(o2, failover.getOneAvailable());
        failover.close();
    }

    @Test
    public void testListener() {
        ArrayList<String> list = new ArrayList<>();
        WeightListener<Object> listener = new WeightListener<Object>() {
            @Override
            public void onSuccess(double maxWeight, double minWeight, int priority,
                    double currentOldWeight, double currentNewWeight, Object resource) {
                list.add("success " + maxWeight + " " + minWeight + " " + priority
                        + " " + currentOldWeight + " " + currentNewWeight + " " + resource);
            }

            @Override
            public void onFail(double maxWeight, double minWeight, int priority,
                    double currentOldWeight, double currentNewWeight, Object resource) {
                list.add("fail " + maxWeight + " " + minWeight + " " + priority
                        + " " + currentOldWeight + " " + currentNewWeight + " " + resource);
            }
        };
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 1, 100)
                .weightListener(listener)
                .priorityFactor(Double.MAX_VALUE)
                .weightFunction(new SimpleWeightFunction<>(0.5, 0.01))
                .build();
        assertEquals(o0, failover.getOneAvailable());
        failover.success(o0);
        assertEquals(0, list.size());

        assertEquals(o0, failover.getOneAvailable());
        failover.fail(o0);
        assertEquals("fail 100.0 0.0 0 100.0 50.0 o0", list.get(0));

        assertEquals(o0, failover.getOneAvailable());
        failover.success(o0);
        assertEquals("success 100.0 0.0 0 50.0 51.0 o0", list.get(1));

        assertEquals(o0, failover.getOneAvailable());
        failover.fail(o0);
        assertEquals("fail 100.0 0.0 0 51.0 1.0 o0", list.get(2));

        assertEquals(o0, failover.getOneAvailable());
        failover.fail(o0);
        assertEquals("fail 100.0 0.0 0 1.0 0.0 o0", list.get(3));

        assertEquals(o1, failover.getOneAvailable());
        failover.down(o1);
        assertEquals("fail 100.0 0.0 1 100.0 0.0 o1", list.get(4));

        failover.close();
    }

    @Test
    public void testNoRes() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .build();
        assertNull(failover.getOneAvailable());

        failover.fail(o0);
        failover.down(o0);
        failover.success(o0);

        assertNull(failover.getOneAvailable());

        failover.close();
    }

    @Test
    public void testRecover() throws Exception {
        PriorityFailoverBuilder<Object> builder = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 10)
                .addResource(o1, 100, 0, 0, 20)
                .addResource(o2, 100, 0, 0, 100)
                .concurrencyControl(false)
                .weightFunction(new SimpleWeightFunction<Object>() {
                    @Override
                    public boolean needCheck(double maxWeight, double minWeight, int priority,
                            double currentWeight, Object resource) {
                        return currentWeight < 15;
                    }

                    @Override
                    public double success(double maxWeight, double minWeight, int priority,
                            double currentOldWeight, Object resource) {
                        return 100;
                    }
                })
                .checkDuration(Duration.ofMillis(1))
                .checker(o -> true);

        PriorityFailover<Object> failover = builder.startCheckTaskImmediately(true).build();
        testDist(failover, 100.0, 20.0, 100.0);
        failover.close();

        failover = builder.startCheckTaskImmediately(false).build();
        testDist(failover, 10.0, 20.0, 100.0);

        failover.fail(o0);
        testDist(failover, 100.0, 20.0, 100.0);
        failover.close();
    }

    private void testDist(PriorityFailover<Object> failover, double w0, double w1, double w2)
            throws Exception {
        testDist(failover, w0, w1, w2, false);
    }

    private void testDist(PriorityFailover<Object> failover, double w0, double w1, double w2, boolean returnRes)
            throws Exception {
        Thread.sleep(5);
        int c0 = 0, c1 = 0, c2 = 0;
        int totalCount = 10000;
        for (int i = 0; i < totalCount; i++) {
            Object o = failover.getOneAvailable();
            if (o == o0) {
                c0++;
            } else if (o == o1) {
                c1++;
            } else {
                c2++;
            }
            if (returnRes) {
                failover.success(o);
            }
        }
        assertEquals(w0 / (w0 + w1 + w2), 1.0 * c0 / totalCount, 0.03);
        assertEquals(w1 / (w0 + w1 + w2), 1.0 * c1 / totalCount, 0.03);
        assertEquals(w2 / (w0 + w1 + w2), 1.0 * c2 / totalCount, 0.03);
    }



    @Test
    public void testDistForRoundRobin() throws Exception {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 1.0)
                .addResource(o1, 1.0)
                .addResource(o2, 1.0)
                .build();
        testDist(failover, 1.0, 1.0, 1.0);
        failover.close();
    }

    @Test
    public void testDistForAliasMethod() throws Exception {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 1.0)
                .addResource(o1, 0.9)
                .addResource(o2, 0.8)
                .build();
        testDist(failover, 1.0, 0.9, 0.8);
        failover.close();

        failover = PriorityFailover.newBuilder()
                .addResource(o0, 1.0)
                .addResource(o1, 0.9)
                .addResource(o2, 0.8)
                .aliasMethodThreshold(2)
                .build();
        testDist(failover, 1.0, 0.9, 0.8);
        failover.close();
    }

    @Test
    public void testDistNotHealthy() throws Exception {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .addResource(o2, 100, 0, 0, 50)
                .build();
        testDist(failover, 100, 100, 50);
        failover.close();
    }

    @Test
    public void testDistWithConCtrl() throws Exception {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .addResource(o2, 100, 0, 0, 100)
                .concurrencyControl(true)
                .build();
        failover.getOneAvailableExclude(Arrays.asList(o1, o2));
        testDist(failover, 50, 100, 100, true);
        failover.close();
    }

    @Test
    public void testGc() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 0)
                .checkDuration(Duration.ofMillis(1))
                .checker(o -> false)
                .build();
        PriorityFailoverCheckTask<Object> task = failover.getCheckTask();
        failover = null;

        for (int i = 0; i < 5000; i++) {
            byte[] bs = new byte[1 * 1024 * 1024];
        }

        PriorityFailover.newBuilder().checker(o -> false).build().close();
        assertTrue(task.isClosed());
    }

}
