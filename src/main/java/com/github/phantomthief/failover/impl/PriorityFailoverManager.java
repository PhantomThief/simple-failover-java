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
 * @author huangli
 * Created on 2020-01-23
 */
@NotThreadSafe
public class PriorityFailoverManager<T> {
    private volatile PriorityFailover<T> failover;

    @Nullable
    private final PriorityGroupManager<T> groupManager;

    public PriorityFailoverManager(PriorityFailover<T> failover, @Nullable PriorityGroupManager<T> groupManager) {
        setFailover(failover);
        this.groupManager = groupManager;
    }

    public PriorityFailover<T> getFailover() {
        return failover;
    }

    protected void setFailover(PriorityFailover<T> failover) {
        this.failover = failover;
    }

    /**
     * Incrementally add/update/remove resources, the current weight of updated resources is kept.
     *
     * @param resNeedToAddOrUpdate resources need to add or update
     * @param resNeedToRemove resources need to remove
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
     * Update all resources, the current weight of updated resources is kept.
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

    public static class UpdateResult<T> {
        private Map<T, ResConfig> addedResources = new HashMap<>();
        private Map<T, ResConfig> removedResources = new HashMap<>();
        private Map<T, ResConfig> updatedResources = new HashMap<>();

        public Map<T, ResConfig> getAddedResources() {
            return addedResources;
        }

        public Map<T, ResConfig> getRemovedResources() {
            return removedResources;
        }

        public Map<T, ResConfig> getUpdatedResources() {
            return updatedResources;
        }

    }

}
