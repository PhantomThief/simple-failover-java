package com.github.phantomthief.failover.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.PriorityFailoverConfig;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.ResConfig;

/**
 * 为了提升性能，PriorityFailover是个"不可变"对象，构造以后，资源列表、每个资源的配置（比如最大权重）就不能变了，
 * 如果需要构建后持续变更（比如资源上下线），并且保留以前资源的当前权重等信息，就需要用到这个类。
 *
 * 这个类的update/updateAll方法不是线程安全的，使用的时候，请自行加锁。
 *
 * @author huangli
 * Created on 2020-01-23
 */
@NotThreadSafe
public class PriorityFailoverManager<T> {
    private volatile PriorityFailover<T> failover;

    @Nullable
    private final PriorityGroupManager<T> groupManager;

    /**
     * 手工构造一个PriorityFailoverManager，用户通常不需要调用这个方法来构造，而是使用{@link PriorityFailoverBuilder#buildManager()}。
     * @param failover failover
     * @param groupManager 自动分组管理器，如果不需要自动分组管理，可以为null
     * @see PriorityFailoverBuilder#buildManager()
     */
    public PriorityFailoverManager(PriorityFailover<T> failover, @Nullable PriorityGroupManager<T> groupManager) {
        setFailover(failover);
        this.groupManager = groupManager;
    }


    /**
     * 获取本类管理的failover，每次返回的对象可能是不同的（比如调用update方法更新了）。
     * 虽然本类不是线程安全的，但是本方法多线程调用时不必加锁。
     * @return 受管的failover
     */
    public PriorityFailover<T> getFailover() {
        return failover;
    }

    protected void setFailover(PriorityFailover<T> failover) {
        this.failover = failover;
    }

    /**
     * 增量add/update/remove资源。
     * 如果一个被更新的资源的最大权重被改变，当前权重会按百分比保留（在不溢出的情况下），举例来说，如果一个资源原来的最大权重是100，当前权重90，
     * 更新后最大权重如果设定为50，那么当前权重自动设置为45。
     * @param resNeedToAddOrUpdate 需要添加或者更新的资源，可以为null，如果为null就代表不需要添加和更新资源
     * @param resNeedToRemove 需要删除的资源，可以为null，如果为null就代表不需要删除资源
     */
    public UpdateResult<T> update(@Nullable Map<T, ResConfig> resNeedToAddOrUpdate, @Nullable Set<T> resNeedToRemove) {
        UpdateResult<T> result = new UpdateResult<>();
        PriorityFailoverConfig<T> oldConfigCopy = failover.getConfig().clone();
        if (groupManager != null) {
            groupManager.update(resNeedToAddOrUpdate == null ? null : resNeedToAddOrUpdate.keySet(),
                    resNeedToRemove);
        }
        if (resNeedToAddOrUpdate != null) {
            processAddAndUpdate(resNeedToAddOrUpdate, oldConfigCopy, result);
        }
        if (resNeedToRemove != null) {
            resNeedToRemove.forEach(res -> {
                ResConfig config = oldConfigCopy.getResources().remove(res);
                result.getRemovedResources().put(res, config.clone());
            });
        }
        failover.close();
        setFailover(new PriorityFailover<>(oldConfigCopy));
        return result;
    }

    private void processAddAndUpdate(@Nonnull Map<T, ResConfig> resNeedToAddOrUpdate,
            PriorityFailoverConfig<T> oldConfigCopy, UpdateResult<T> result) {
        HashMap<T, PriorityFailover.ResInfo<T>> currentDataMap = failover.getResourcesMap();
        Map<T, ResConfig> initResConfigCopy = oldConfigCopy.getResources();
        resNeedToAddOrUpdate.forEach((res, newConfig) -> {
            PriorityFailover.ResInfo<T> resInfo = currentDataMap.get(res);
            double initWeight;
            if (resInfo != null) {
                double current = resInfo.currentWeight;
                if (current != resInfo.maxWeight) {
                    initWeight = current / resInfo.maxWeight * newConfig.getMaxWeight();
                } else {
                    initWeight = newConfig.getMaxWeight();
                }
                initWeight = Math.min(initWeight, newConfig.getMaxWeight());
                initWeight = Math.max(initWeight, newConfig.getMinWeight());
            } else {
                initWeight = newConfig.getInitWeight();
            }
            int pri = groupManager == null ? newConfig.getPriority() : groupManager.getPriority(res);
            newConfig = new ResConfig(newConfig.getMaxWeight(),
                    newConfig.getMinWeight(), pri, initWeight);
            PriorityFailoverBuilder.checkResConfig(newConfig);
            if (initResConfigCopy.containsKey(res)) {
                result.getUpdatedResources().put(res, newConfig.clone());
            } else {
                result.getAddedResources().put(res, newConfig.clone());
            }
            initResConfigCopy.put(res, newConfig);
        });
    }

    /**
     * 全量更新资源，已有的资源，如果不在新的列表中的资源会被删除，如果在新的列表中的资源会被更新。
     * 如果一个被更新的资源的最大权重被改变，当前权重会按百分比保留（在不溢出的情况下），举例来说，如果一个资源原来的最大权重是100，当前权重90，
     * 更新后最大权重如果设定为50，那么当前权重自动设置为45。
     * @param newResourceConfigs 新的资源列表
     */
    public UpdateResult<T> updateAll(@Nonnull Map<T, ResConfig> newResourceConfigs) {
        UpdateResult<T> result = new UpdateResult<>();
        PriorityFailoverConfig<T> oldConfigCopy = failover.getConfig().clone();
        if (groupManager != null) {
            groupManager.updateAll(newResourceConfigs.keySet());
        }

        processAddAndUpdate(newResourceConfigs, oldConfigCopy, result);

        Iterator<Entry<T, ResConfig>> iterator = oldConfigCopy.getResources().entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<T, ResConfig> en = iterator.next();
            if (!newResourceConfigs.containsKey(en.getKey())) {
                iterator.remove();
                result.getRemovedResources().put(en.getKey(), en.getValue());
            }
        }

        failover.close();
        setFailover(new PriorityFailover<>(oldConfigCopy));
        return result;
    }

    @Nullable
    PriorityGroupManager<T> getGroupManager() {
        return groupManager;
    }

    /**
     * 更新的结果
     * @param <T> 类型参数
     */
    public static class UpdateResult<T> {
        private Map<T, ResConfig> addedResources = new HashMap<>();
        private Map<T, ResConfig> removedResources = new HashMap<>();
        private Map<T, ResConfig> updatedResources = new HashMap<>();

        /**
         * 本次添加的资源，Map的value是新的配置。
         * @return 更新的资源
         */
        public Map<T, ResConfig> getAddedResources() {
            return addedResources;
        }

        /**
         * 删除的资源，Map的value是旧的配置。
         * @return 更新的资源
         */
        public Map<T, ResConfig> getRemovedResources() {
            return removedResources;
        }

        /**
         * 本次更新的资源，Map的value是新的配置。
         * @return 更新的资源
         */
        public Map<T, ResConfig> getUpdatedResources() {
            return updatedResources;
        }

    }

}
