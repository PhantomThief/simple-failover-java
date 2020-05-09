package com.github.phantomthief.failover.impl;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.phantomthief.failover.util.SharedCheckExecutorHolder;

/**
 * @author huangli
 * Created on 2020-01-16
 */
public class PriorityFailoverBuilder<T> {

    private static final double DEFAULT_MAX_WEIGHT = 100.0;
    private static final double DEFAULT_MIN_WEIGHT = 0.0;
    private static final int DEFAULT_PRIORITY = 0;

    private PriorityFailoverConfig<T> config = new PriorityFailoverConfig<>();
    private int[] coreGroupSizes;

    PriorityFailoverBuilder() {
    }

    public PriorityFailover<T> build() {
        PriorityFailoverConfig<T> configCopy = config.clone();
        int[] coreGroupSizesCopy = coreGroupSizes == null ? null : coreGroupSizes.clone();
        buildGroup(configCopy, coreGroupSizesCopy);
        return new PriorityFailover<>(configCopy);
    }

    public PriorityFailoverManager<T> buildManager() {
        PriorityFailoverConfig<T> configCopy = config.clone();
        int[] coreGroupSizesCopy = coreGroupSizes == null ? null : coreGroupSizes.clone();
        PriorityGroupManager<T> groupManager = buildGroup(configCopy, coreGroupSizesCopy);
        PriorityFailover<T> priorityFailover = new PriorityFailover<>(configCopy);
        return new PriorityFailoverManager<>(priorityFailover, groupManager);
    }

    private static <T> PriorityGroupManager<T> buildGroup(PriorityFailoverConfig<T> config, int[] coreGroupSizes) {
        if (coreGroupSizes != null && coreGroupSizes.length > 0) {
            Map<T, ResConfig> resources = config.getResources();
            PriorityGroupManager<T> groupManager = new PriorityGroupManager<>(
                    resources.keySet(), coreGroupSizes);
            Map<T, Integer> priorityMap = groupManager.getPriorityMap();
            priorityMap.forEach((res, pri) -> {
                ResConfig old = resources.get(res);
                ResConfig newConfig = new ResConfig(old.getMaxWeight(), old.getMinWeight(), pri, old.getInitWeight());
                resources.put(res, newConfig);
            });
            return groupManager;
        } else {
            return null;
        }
    }

    public PriorityFailoverBuilder<T> addResource(T res) {
        return addResource(res, DEFAULT_MAX_WEIGHT);
    }

    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight) {
        return addResource(res, maxWeight, DEFAULT_MIN_WEIGHT);
    }

    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight, double minWeight) {
        return addResource(res, maxWeight, minWeight, DEFAULT_PRIORITY);
    }

    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight, double minWeight, int priority) {
        return addResource(res, maxWeight, minWeight, priority, maxWeight);
    }

    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight, double minWeight,
            int priority, double initWeight) {
        requireNonNull(res);
        ResConfig resConfig = new ResConfig(maxWeight, minWeight, priority, initWeight);
        checkResConfig(resConfig);
        config.getResources().put(res, resConfig);
        return this;
    }

    public PriorityFailoverBuilder<T> addResources(@Nonnull Collection<? extends T> resources) {
        addResources(resources, DEFAULT_MAX_WEIGHT);
        return this;
    }

    public PriorityFailoverBuilder<T> addResources(@Nonnull Collection<? extends T> resources, double maxWeight) {
        Objects.requireNonNull(resources);
        resources.forEach(res -> addResource(res, maxWeight));
        return this;
    }

    public PriorityFailoverBuilder<T> addResources(@Nonnull Map<? extends T, ? extends Number> resources) {
        Objects.requireNonNull(resources);
        resources.forEach((res, maxWeight) -> addResource(res, maxWeight.doubleValue()));
        return this;
    }

    static void checkResConfig(ResConfig resConfig) {
        if (resConfig.getMaxWeight() < 0) {
            throw new IllegalArgumentException("maxWeight less than zero:" + resConfig.getMaxWeight());
        }
        if (resConfig.getMinWeight() < 0) {
            throw new IllegalArgumentException("minWeight less than zero:" + resConfig.getMinWeight());
        }
        if (resConfig.getMaxWeight() < resConfig.getMinWeight()) {
            throw new IllegalArgumentException(
                    "maxWeight < minWeight:" + resConfig.getMaxWeight() + "," + resConfig.getMinWeight());
        }
        if (resConfig.getInitWeight() < resConfig.getMinWeight() && resConfig.getInitWeight() > resConfig
                .getMaxWeight()) {
            throw new IllegalArgumentException("illegal initWeight:" + resConfig.getInitWeight());
        }
    }

    public PriorityFailoverBuilder<T> name(String name) {
        config.setName(name);
        return this;
    }

    public PriorityFailoverBuilder<T> weightFunction(WeightFunction<T> weightFunction) {
        requireNonNull(weightFunction);
        config.setWeightFunction(weightFunction);
        return this;
    }

    public PriorityFailoverBuilder<T> weightListener(WeightListener<T> weightListener) {
        requireNonNull(weightListener);
        config.setWeightListener(weightListener);
        return this;
    }

    public PriorityFailoverBuilder<T> priorityFactor(double priorityFactor) {
        if (priorityFactor < 0) {
            throw new IllegalArgumentException("priorityFactor less than zero:" + priorityFactor);
        }
        config.setPriorityFactor(priorityFactor);
        return this;
    }

    @SuppressWarnings("checkstyle:HiddenField")
    public PriorityFailoverBuilder<T> enableAutoPriority(int... coreGroupSizes) {
        this.coreGroupSizes = coreGroupSizes;
        return this;
    }

    public PriorityFailoverBuilder<T> checkDuration(Duration checkDuration) {
        requireNonNull(checkDuration);
        config.setCheckDuration(checkDuration);
        return this;
    }

    public PriorityFailoverBuilder<T> checkExecutor(ScheduledExecutorService checkExecutor) {
        requireNonNull(checkExecutor);
        config.setCheckExecutor(checkExecutor);
        return this;
    }

    public PriorityFailoverBuilder<T> checker(Predicate<T> checker) {
        requireNonNull(checker);
        config.setChecker(checker);
        return this;
    }

    public PriorityFailoverBuilder<T> startCheckTaskImmediately(boolean startCheckTaskImmediately) {
        config.setStartCheckTaskImmediately(startCheckTaskImmediately);
        return this;
    }

    public PriorityFailoverBuilder<T> concurrencyControl(boolean concurrencyControl) {
        config.setConcurrencyControl(concurrencyControl);
        return this;
    }

    public PriorityFailoverBuilder<T> aliasMethodThreshold(int aliasMethodThreshold) {
        config.setAliasMethodThreshold(aliasMethodThreshold);
        return this;
    }

    public static final class ResConfig implements Cloneable {
        private final int priority;
        private final double maxWeight;
        private final double minWeight;
        private final double initWeight;

        public ResConfig() {
            this(DEFAULT_MAX_WEIGHT);
        }

        public ResConfig(double maxWeight) {
            this(maxWeight, DEFAULT_MIN_WEIGHT);
        }

        public ResConfig(double maxWeight, double minWeight) {
            this(maxWeight, minWeight, DEFAULT_PRIORITY);
        }

        public ResConfig(double maxWeight, double minWeight, int priority) {
            this(maxWeight, minWeight, priority, maxWeight);
        }

        public ResConfig(double maxWeight, double minWeight, int priority, double initWeight) {
            this.maxWeight = maxWeight;
            this.minWeight = minWeight;
            this.priority = priority;
            this.initWeight = initWeight;
        }

        @Override
        public ResConfig clone() {
            try {
                return (ResConfig) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        public int getPriority() {
            return priority;
        }

        public double getMaxWeight() {
            return maxWeight;
        }

        public double getMinWeight() {
            return minWeight;
        }

        public double getInitWeight() {
            return initWeight;
        }
    }


    static class PriorityFailoverConfig<T> implements Cloneable {
        private Map<T, ResConfig> resources = new HashMap<>();
        private String name;

        private double priorityFactor = 1.4;
        private WeightFunction<T> weightFunction = new RatioWeightFunction<>();
        @Nullable
        private WeightListener<T> weightListener;
        private boolean concurrencyControl = false;
        private Duration checkDuration = Duration.ofSeconds(1);
        private ScheduledExecutorService checkExecutor = SharedCheckExecutorHolder.getInstance();
        @Nullable
        private Predicate<T> checker;
        private boolean startCheckTaskImmediately;

        private int aliasMethodThreshold = 10;

        @Override
        @SuppressWarnings("unchecked")
        protected PriorityFailoverConfig<T> clone() {
            try {
                PriorityFailoverConfig<T> newOne = (PriorityFailoverConfig<T>) super.clone();
                newOne.resources = new HashMap<>(resources);
                return newOne;
            } catch (CloneNotSupportedException e) {
                // assert false
                throw new RuntimeException(e);
            }
        }

        public Map<T, ResConfig> getResources() {
            return resources;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getPriorityFactor() {
            return priorityFactor;
        }

        public void setPriorityFactor(double priorityFactor) {
            this.priorityFactor = priorityFactor;
        }

        public WeightFunction<T> getWeightFunction() {
            return weightFunction;
        }

        public void setWeightFunction(WeightFunction<T> weightFunction) {
            this.weightFunction = weightFunction;
        }

        @Nullable
        public WeightListener<T> getWeightListener() {
            return weightListener;
        }

        public void setWeightListener(@Nullable WeightListener<T> weightListener) {
            this.weightListener = weightListener;
        }

        public boolean isConcurrencyControl() {
            return concurrencyControl;
        }

        public void setConcurrencyControl(boolean concurrencyControl) {
            this.concurrencyControl = concurrencyControl;
        }

        public Duration getCheckDuration() {
            return checkDuration;
        }

        public void setCheckDuration(Duration checkDuration) {
            this.checkDuration = checkDuration;
        }

        public ScheduledExecutorService getCheckExecutor() {
            return checkExecutor;
        }

        public void setCheckExecutor(ScheduledExecutorService checkExecutor) {
            this.checkExecutor = checkExecutor;
        }

        @Nullable
        public Predicate<T> getChecker() {
            return checker;
        }

        public void setChecker(@Nullable Predicate<T> checker) {
            this.checker = checker;
        }

        public boolean isStartCheckTaskImmediately() {
            return startCheckTaskImmediately;
        }

        public void setStartCheckTaskImmediately(boolean startCheckTaskImmediately) {
            this.startCheckTaskImmediately = startCheckTaskImmediately;
        }

        public int getAliasMethodThreshold() {
            return aliasMethodThreshold;
        }

        public void setAliasMethodThreshold(int aliasMethodThreshold) {
            this.aliasMethodThreshold = aliasMethodThreshold;
        }
    }
}
