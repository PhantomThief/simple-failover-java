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
 * 自动分组管理器，自动将资源分成若干组，第一组的priority是0，第二组是1，依次类推。
 * 同时，提供全量和增量变更管理的功能。
 *
 * <p>
 * 举个例子，假设update后新的资源列表共100个，分两个组，分别有5个和95个资源。
 * <li>每个加入的资源有均等的机会进入第一组，几率为5%，这样可以避免新增的资源（服务器）没有流量，并且新增的资源和已有的资源流量应该是对等的；
 * <li>对于保留的资源来说，如果一个资源A之前进入了前一组，而B没有，那么A仍然优先，不会出现B进了第一组而A没有进的情况
 * （B有可能因为旧资源删除而晋升），这样可以尽量保持主调方的粘性，有利于连接复用和被调用方的缓存等。
 * </p>
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
     * 构造自动分组管理器。
     * @param initResources 初始资源列表
     * @param coreGroupSizes 你对每组资源数量的要求，只填N-1个组，比如想分3组，第一组5个，第二组20个，剩下是第三组，那么这个参数传入[5, 10]
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

    /**
     * 获取资源优先级map。
     * @return map里面的key是资源，value是优先级（0 based）
     */
    public Map<T, Integer> getPriorityMap() {
        return resMap.entrySet().stream()
                .collect(toMap(Entry::getKey, e -> e.getValue()[0]));
    }

    /**
     * 获取某个资源的优先级
     * @param res 资源
     * @return 优先级（0 based）
     */
    public int getPriority(T res) {
        return resMap.get(res)[0];
    }

    /**
     * 全量更新，会自动计算出需要增加的、删除的、保留的资源，以前的分组数据会按几率分布保留。
     *
     * <p>
     * 举个例子，假设update后新的资源列表共100个，分两个组，分别有5个和95个资源。
     * <li>每个加入的资源有均等的机会进入第一组，几率为5%，这样可以避免新增的资源（服务器）没有流量，并且新增的资源和已有的资源流量应该是对等的；
     * <li>对于保留的资源来说，如果一个资源A之前进入了前一组，而B没有，那么A仍然优先，不会出现B进了第一组而A没有进的情况
     * （B有可能因为旧资源删除而晋升），这样可以尽量保持主调方的粘性，有利于连接复用和被调用方的缓存等。
     * </p>
     * @param resources 新的资源列表
     */
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

    /**
     * 增量更新，会自动计算出需要增加的、删除的、保留的资源，以前的分组数据会按几率分布保留。
     *
     * <p>
     * 举个例子，假设update后新的资源列表共100个，分两个组，分别有5个和95个资源。
     * <li>每个加入的资源有均等的机会进入第一组，几率为5%，这样可以避免新增的资源（服务器）没有流量，并且新增的资源和已有的资源流量应该是对等的；
     * <li>对于保留的资源来说，如果一个资源A之前进入了前一组，而B没有，那么A仍然优先，不会出现B进了第一组而A没有进的情况
     * （B有可能因为旧资源删除而晋升），这样可以尽量保持主调方的粘性，有利于连接复用和被调用方的缓存等。
     * </p>
     * @param resNeedToAdd 需要添加的
     * @param resNeedToRemove 需要删除的
     */
    public void update(@Nullable Set<T> resNeedToAdd, @Nullable Set<T> resNeedToRemove) {
        if (resNeedToAdd != null) {
            resNeedToAdd = new HashSet<>(resNeedToAdd);
            resNeedToAdd.removeAll(resMap.keySet());
            if (resNeedToRemove != null) {
                // 以免两个set之间有重叠
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
