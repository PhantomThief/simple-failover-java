package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.failover.impl.PriorityGroupManager.buildResMap;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author huangli
 * Created on 2020-02-06
 */
class PriorityGroupManagerTest {

    @Test
    public void testBuildResMap() {
        List<String> cores = asList("0", "1", "2");
        List<String> rests = asList("3", "4", "5");

        HashMap<String, int[]> map = buildResMap(cores, rests, new int[] {1, 2});
        assertArrayEquals(new int[] {0, 0}, map.get("0"));
        assertArrayEquals(new int[] {1, 1}, map.get("1"));
        assertArrayEquals(new int[] {1, 2}, map.get("2"));
        assertArrayEquals(new int[] {2, 3}, map.get("3"));
        assertArrayEquals(new int[] {2, 4}, map.get("4"));
        assertArrayEquals(new int[] {2, 5}, map.get("5"));

        map = buildResMap(cores, rests, new int[] {1, 0, 2});
        assertArrayEquals(new int[] {0, 0}, map.get("0"));
        assertArrayEquals(new int[] {2, 1}, map.get("1"));
        assertArrayEquals(new int[] {2, 2}, map.get("2"));
        assertArrayEquals(new int[] {3, 3}, map.get("3"));
        assertArrayEquals(new int[] {3, 4}, map.get("4"));
        assertArrayEquals(new int[] {3, 5}, map.get("5"));

        map = buildResMap(cores, emptyList(), new int[] {1, 4});
        assertArrayEquals(new int[] {0, 0}, map.get("0"));
        assertArrayEquals(new int[] {1, 1}, map.get("1"));
        assertArrayEquals(new int[] {1, 2}, map.get("2"));

        buildResMap(emptyList(), emptyList(), new int[] {1, 4});

        map = buildResMap(emptyList(), rests, new int[] {});
        assertArrayEquals(new int[] {0, 0}, map.get("3"));
        assertArrayEquals(new int[] {0, 1}, map.get("4"));
        assertArrayEquals(new int[] {0, 2}, map.get("5"));
    }

    static int[] countOfEachGroup(int groupCount, PriorityGroupManager<?> manager) {
        Map<?, Integer> map = manager.getPriorityMap();
        int[] result = new int[groupCount];
        for (Map.Entry<?, Integer> en : map.entrySet()) {
            result[en.getValue()]++;
        }
        return result;
    }

    @Test
    public void testConstructor() {
        Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4", "5"));
        PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 1, 2);
        int[] resCountOfGroup = countOfEachGroup(3, manager);
        assertEquals(1, resCountOfGroup[0]);
        assertEquals(2, resCountOfGroup[1]);
        assertEquals(3, resCountOfGroup[2]);

        int p = manager.getPriority("0");
        manager.getPriorityMap().put("0", 10000);
        assertEquals(p, manager.getPriority("0"));

        int p0 = 0, p1 = 0, p2 = 0, count = 20000;
        for (int i = 0; i < count; i++) {
            manager = new PriorityGroupManager<>(resources, 1, 2);
            switch (manager.getPriority("0")) {
                case 0:
                    p0++;
                    break;
                case 1:
                    p1++;
                    break;
                case 2:
                    p2++;
                    break;
            }
        }
        assertEquals(1 / 6.0, 1.0 * p0 / count, 0.02);
        assertEquals(2 / 6.0, 1.0 * p1 / count, 0.02);
        assertEquals(3 / 6.0, 1.0 * p2 / count, 0.02);
    }


    @Test
    public void testConstructorEx() {
        Set<String> resources = new HashSet<>(asList("0", "1", "2"));
        PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources);
        assertEquals(0, manager.getPriority("0"));
        assertEquals(0, manager.getPriority("1"));
        assertEquals(0, manager.getPriority("2"));

        manager = new PriorityGroupManager<>(resources, 10);
        assertEquals(0, manager.getPriority("0"));
        assertEquals(0, manager.getPriority("1"));
        assertEquals(0, manager.getPriority("2"));

        assertThrows(IllegalArgumentException.class,
                () -> new PriorityGroupManager<>(resources, -1));
    }

    @Test
    public void testUpdate() {
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 2);
            manager.update(singleton("5"), singleton("0"));
            int[] resCountOfGroup = countOfEachGroup(2, manager);
            assertEquals(2, resCountOfGroup[0]);
            assertEquals(3, resCountOfGroup[1]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 2);
            manager.update(new HashSet<>(asList("5", "6")), new HashSet<>(asList("0", "1")));
            int[] resCountOfGroup = countOfEachGroup(2, manager);
            assertEquals(2, resCountOfGroup[0]);
            assertEquals(3, resCountOfGroup[1]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 2);
            manager.update(emptySet(), emptySet());
            int[] resCountOfGroup = countOfEachGroup(2, manager);
            assertEquals(2, resCountOfGroup[0]);
            assertEquals(3, resCountOfGroup[1]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 2);
            manager.update(new HashSet<>(asList("5", "1")), singleton("0"));
            int[] resCountOfGroup = countOfEachGroup(2, manager);
            assertEquals(2, resCountOfGroup[0]);
            assertEquals(3, resCountOfGroup[1]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 2);
            manager.update(singleton("5"), null);
            int[] resCountOfGroup = countOfEachGroup(2, manager);
            assertEquals(2, resCountOfGroup[0]);
            assertEquals(4, resCountOfGroup[1]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 2);
            manager.update(null, singleton("0"));
            int[] resCountOfGroup = countOfEachGroup(2, manager);
            assertEquals(2, resCountOfGroup[0]);
            assertEquals(2, resCountOfGroup[1]);
        }
    }

    @Test
    public void testUpdateDist() {
        HashSet<Integer> set1 = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            set1.add(i);
        }
        HashSet<Integer> set2 = new HashSet<>();
        for (int i = 10; i < 20; i++) {
            set2.add(i);
        }

        int[] p0count = new int[20];

        int loopCount = 50000;
        for (int i = 0; i < loopCount; i++) {
            PriorityGroupManager<Integer> manager = new PriorityGroupManager<>(set1, 4);
            manager.update(set2, null);
            Map<Integer, Integer> priorityMap = manager.getPriorityMap();
            priorityMap.forEach((res, p) -> {
                if (p == 0) {
                    p0count[res]++;
                }
            });
        }

        for (int i = 0; i < p0count.length; i++) {
            Assertions.assertEquals(0.2, 1.0 * p0count[i] / loopCount, 0.015, "index " + i);
        }
    }

    @Test
    public void testUpdateAll() {
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4", "5"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 1, 2);
            manager.updateAll(new HashSet<>(asList("1", "2", "3", "4", "5", "6")));
            int[] resCountOfGroup = countOfEachGroup(3, manager);
            assertEquals(1, resCountOfGroup[0]);
            assertEquals(2, resCountOfGroup[1]);
            assertEquals(3, resCountOfGroup[2]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4", "5"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 1, 2);
            manager.updateAll(new HashSet<>(asList("2", "3", "4", "5", "6", "7")));
            int[] resCountOfGroup = countOfEachGroup(3, manager);
            assertEquals(1, resCountOfGroup[0]);
            assertEquals(2, resCountOfGroup[1]);
            assertEquals(3, resCountOfGroup[2]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4", "5"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 1, 2);
            manager.updateAll(new HashSet<>(asList("0", "1", "2", "3", "4", "5", "6", "7")));
            int[] resCountOfGroup = countOfEachGroup(3, manager);
            assertEquals(1, resCountOfGroup[0]);
            assertEquals(2, resCountOfGroup[1]);
            assertEquals(5, resCountOfGroup[2]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4", "5"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 1, 2);
            manager.updateAll(new HashSet<>(asList("0", "1", "2", "3")));
            int[] resCountOfGroup = countOfEachGroup(3, manager);
            assertEquals(1, resCountOfGroup[0]);
            assertEquals(2, resCountOfGroup[1]);
            assertEquals(1, resCountOfGroup[2]);
        }
        {
            Set<String> resources = new HashSet<>(asList("0", "1", "2", "3", "4", "5"));
            PriorityGroupManager<String> manager = new PriorityGroupManager<>(resources, 1, 2);
            manager.updateAll(new HashSet<>(asList("0", "1")));
            int[] resCountOfGroup = countOfEachGroup(3, manager);
            assertEquals(1, resCountOfGroup[0]);
            assertEquals(1, resCountOfGroup[1]);
            assertEquals(0, resCountOfGroup[2]);
        }
    }

    private int remove(Set<Integer> set, Random r) {
        int index = r.nextInt(set.size());
        int loopIndex = 0;
        for (int x : set) {
            if (loopIndex++ == index) {
                set.remove(x);
                return x;
            }
        }
        throw new IllegalStateException();
    }

    @Test
    public void testUpdateAllDist() {
        HashSet<Integer> set1 = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            set1.add(i);
        }
        HashSet<Integer> set2 = new HashSet<>();
        for (int i = 10; i < 20; i++) {
            set2.add(i);
        }

        int[] p0count = new int[20];
        PriorityGroupManager<Integer> manager = new PriorityGroupManager<>(set1, 4);
        int loopCount = 50000;
        Random r = new Random();
        for (int i = 0; i < loopCount; i++) {
            int x1 = remove(set1, r);
            int x2 = remove(set1, r);
            int x3 = remove(set2, r);
            int x4 = remove(set2, r);
            set1.add(x3);
            set1.add(x4);
            set2.add(x1);
            set2.add(x2);
            manager.updateAll(set1);
            Map<Integer, Integer> priorityMap = manager.getPriorityMap();
            priorityMap.forEach((res, p) -> {
                if (p == 0) {
                    p0count[res]++;
                }
            });
        }

        for (int i = 0; i < p0count.length; i++) {
            Assertions.assertEquals(0.2, 1.0 * p0count[i] / loopCount, 0.015, "index " + i);
        }
    }
}
