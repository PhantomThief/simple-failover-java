package com.github.phantomthief.failover.impl;

import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Auto divide resources into several groups, the priority of first group is number 0, the second is 1 ...
 *
 * @author huangli
 * Created on 2020-02-03
 */
@NotThreadSafe
public class PriorityGroupManager<T> {

    private final int sumOfCoreGroupSize;
    private final int[] coreGroupSizes;

    // contains resources from group 0 to group N-2, ordered
    private volatile ArrayList<T> coreResources;

    // contains resources of group N-1 (the last group), has no order
    private volatile ArrayList<T> restResources;

    // res -> [int priority, int logicIndex]
    private volatile HashMap<T, int[]> resMap;

    /**
     * @param initResources the init resources
     * @param coreGroupSizes the size of each group, if you want N groups, coreGroupSizes.length should be N-1
     */
    public PriorityGroupManager(@Nonnull Set<T> initResources, int... coreGroupSizes) {
        Objects.requireNonNull(initResources);
        Objects.requireNonNull(coreGroupSizes);
        this.coreGroupSizes = coreGroupSizes.clone();
        int sum = 0;
        for (int size : coreGroupSizes) {
            if (size < 0) {
                throw new IllegalArgumentException("illegal coreGroupSizes:" + size);
            }
            sum += size;
        }
        this.sumOfCoreGroupSize = sum;
        ArrayList<T> totalResources = new ArrayList<>(initResources);
        Collections.shuffle(totalResources);

        this.coreResources = new ArrayList<>(totalResources.subList(0,
                Math.min(sumOfCoreGroupSize, totalResources.size())));
        if (totalResources.size() > sumOfCoreGroupSize) {
            this.restResources = new ArrayList<>(totalResources.subList(sumOfCoreGroupSize, totalResources.size()));
        } else {
            this.restResources = new ArrayList<>();
        }
        resMap = buildResMap(coreResources, restResources, coreGroupSizes);
    }

    static <T> HashMap<T, int[]> buildResMap(List<T> coreResources, List<T> restResources,
            int[] coreGroupSizes) {
        final int totalGroupCount = coreGroupSizes.length + 1;
        HashMap<T, int[]> map = new HashMap<>();
        int logicIndex = 0;
        int priority = 0;
        int groupIndex = 0;
        for (T res : coreResources) {
            while (groupIndex >= coreGroupSizes[priority]) {
                priority++;
                groupIndex = 0;
            }
            map.put(res, new int[] {priority, logicIndex});
            groupIndex++;
            logicIndex++;
        }
        for (T res : restResources) {
            map.put(res, new int[] {totalGroupCount - 1, logicIndex});
            logicIndex++;
        }
        return map;
    }

    public Map<T, Integer> getPriorityMap() {
        return resMap.entrySet().stream()
                .collect(toMap(Entry::getKey, e -> e.getValue()[0]));
    }

    public int getPriority(T res) {
        return resMap.get(res)[0];
    }

    public void updateAll(@Nonnull Set<T> resources) {
        Objects.requireNonNull(resources);
        Set<T> resNeedToAdd = resources.stream()
                .filter(res -> !resMap.containsKey(res))
                .collect(Collectors.toSet());
        Set<T> resNeedToRemove = resMap.keySet().stream()
                .filter(res -> !resources.contains(res))
                .collect(Collectors.toSet());
        update0(resNeedToAdd, resNeedToRemove);
    }

    public void update(@Nullable Set<T> resNeedToAdd, @Nullable Set<T> resNeedToRemove) {
        if (resNeedToAdd != null) {
            resNeedToAdd = new HashSet<>(resNeedToAdd);
            resNeedToAdd.removeAll(resMap.keySet());
            if (resNeedToRemove != null) {
                resNeedToAdd.removeAll(resNeedToRemove);
            }
        }
        update0(resNeedToAdd, resNeedToRemove);
    }

    @SuppressWarnings("checkstyle:HiddenField")
    private void update0(@Nullable Set<T> resNeedToAdd, @Nullable Set<T> resNeedToRemove) {
        ArrayList<T> coreResources = new ArrayList<>(this.coreResources);
        ArrayList<T> restResources = new ArrayList<>(this.restResources);
        if (resNeedToRemove != null) {
            coreResources.removeAll(resNeedToRemove);
            restResources.removeAll(resNeedToRemove);
        }
        Random r = new Random();
        while (coreResources.size() < sumOfCoreGroupSize && restResources.size() > 0) {
            int index = r.nextInt(restResources.size());
            coreResources.add(restResources.remove(index));
        }
        if (resNeedToAdd != null) {
            for (T res : resNeedToAdd) {
                int currentCount = coreResources.size() + restResources.size();
                int index = r.nextInt(currentCount + 1);
                if (index < sumOfCoreGroupSize) {
                    coreResources.add(index, res);
                } else {
                    restResources.add(res);
                }
                if (coreResources.size() > sumOfCoreGroupSize) {
                    // move last one
                    restResources.add(coreResources.remove(coreResources.size() - 1));
                }
            }
        }
        resMap = buildResMap(coreResources, restResources, coreGroupSizes);
        this.coreResources = coreResources;
        this.restResources = restResources;
    }
}
