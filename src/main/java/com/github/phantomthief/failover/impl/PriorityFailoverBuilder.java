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
 * PriorityFailover的builder。
 *
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

    /**
     * 构造一个PriorityFailover，PriorityFailover构造后是不能添加和删除资源的，如果有这方面的需求应该构建一个
     * PriorityFailoverManager。
     * @see #buildManager()
     * @return 一个新的PriorityFailover
     */
    public PriorityFailover<T> build() {
        PriorityFailoverConfig<T> configCopy = config.clone();
        int[] coreGroupSizesCopy = coreGroupSizes == null ? null : coreGroupSizes.clone();
        buildGroup(configCopy, coreGroupSizesCopy);
        return new PriorityFailover<>(configCopy);
    }

    /**
     * 构造一个PriorityFailoverManager，可以管理资源的添加和删除，如果资源列表是固定的，
     * 可用build方法直接构建PriorityFailover。
     * @see #build()
     * @return 一个PriorityFailoverManager
     */
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

    /**
     * 添加资源，使用默认的配置，最大权重100.0，最小权重0，优先级0，初始权重100.0。
     * @param res 要添加的资源
     * @return this
     */
    public PriorityFailoverBuilder<T> addResource(T res) {
        return addResource(res, DEFAULT_MAX_WEIGHT);
    }

    /**
     * 添加资源，最小权重0，优先级0，初始权重等于最大权重。
     * @param res 要添加的资源
     * @param maxWeight 最大权重
     * @return this
     */
    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight) {
        return addResource(res, maxWeight, DEFAULT_MIN_WEIGHT);
    }

    /**
     * 添加资源，优先级0，初始权重等于最大权重。
     * @param res 要添加的资源
     * @param maxWeight 最大权重
     * @param minWeight 最小权重
     * @return this
     */
    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight, double minWeight) {
        return addResource(res, maxWeight, minWeight, DEFAULT_PRIORITY);
    }

    /**
     * 添加资源，初始权重等于最大权重。
     * @param res 要添加的资源
     * @param maxWeight 最大权重
     * @param minWeight 最小权重
     * @param priority 优先级
     * @return this
     */
    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight, double minWeight, int priority) {
        return addResource(res, maxWeight, minWeight, priority, maxWeight);
    }

    /**
     * 添加资源。
     * @param res 要添加的资源
     * @param maxWeight 最大权重
     * @param minWeight 最小权重
     * @param priority 优先级
     * @param initWeight 初始权重
     * @return this
     */
    public PriorityFailoverBuilder<T> addResource(T res, double maxWeight, double minWeight,
            int priority, double initWeight) {
        requireNonNull(res);
        ResConfig resConfig = new ResConfig(maxWeight, minWeight, priority, initWeight);
        checkResConfig(resConfig);
        config.getResources().put(res, resConfig);
        return this;
    }

    /**
     * 一次添加多个资源，使用默认的配置，最大权重100.0，最小权重0，优先级0，初始权重100.0。
     * @param resources 资源列表
     * @return this
     */
    public PriorityFailoverBuilder<T> addResources(@Nonnull Collection<? extends T> resources) {
        addResources(resources, DEFAULT_MAX_WEIGHT);
        return this;
    }

    /**
     * 一次添加多个资源，除了指定了最大权重外，其他使用默认的配置，最小权重0，优先级0，初始权重等于最大权重。
     * @param resources 资源列表
     * @param maxWeight 所有资源的最大权重
     * @return this
     */
    public PriorityFailoverBuilder<T> addResources(@Nonnull Collection<? extends T> resources, double maxWeight) {
        Objects.requireNonNull(resources);
        resources.forEach(res -> addResource(res, maxWeight));
        return this;
    }

    /**
     * 一次添加多个资源，除了指定了最大权重外，其他使用默认的配置，最小权重0，优先级0，初始权重等于最大权重。
     * @param resources 资源列表，map中的value就是对应资源的最大权重
     * @return this
     */
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

    /**
     * 指定failover的name。
     * @param name 这个failover的name
     * @return this
     */
    public PriorityFailoverBuilder<T> name(String name) {
        config.setName(name);
        return this;
    }

    /**
     * 指定权重计算的回调，如果不需要定制可以不指定，用默认的（{@link RatioWeightFunction}）就可以。
     * @param weightFunction 权重回调
     * @return this
     * @see WeightFunction
     */
    public PriorityFailoverBuilder<T> weightFunction(WeightFunction<T> weightFunction) {
        requireNonNull(weightFunction);
        config.setWeightFunction(weightFunction);
        return this;
    }

    /**
     * 注册权重事件回调。
     * @param weightListener 回调器
     * @return this
     * @see WeightListener
     */
    public PriorityFailoverBuilder<T> weightListener(WeightListener<T> weightListener) {
        requireNonNull(weightListener);
        config.setWeightListener(weightListener);
        return this;
    }

    /**
     * 优先级因子，默认1.4与envoy相同。
     * 可参考<a href="https://www.servicemesher.com/envoy/intro/arch_overview/load_balancing.html">envoy负载均衡文档</a>
     * @param priorityFactor 优先级因子
     * @return
     */
    public PriorityFailoverBuilder<T> priorityFactor(double priorityFactor) {
        if (priorityFactor < 0) {
            throw new IllegalArgumentException("priorityFactor less than zero:" + priorityFactor);
        }
        config.setPriorityFactor(priorityFactor);
        return this;
    }

    /**
     * 激活自动优先级管理，假设现在有N个资源，想分3组，第一组5个优先级0，第二组20个优先级1，剩下的归为第三组优先级2，那么调用本方法
     * enableAutoPriority(5, 20)即可
     * @param coreGroupSizes 每组的资源数字，最后一组不用填
     * @return this
     */
    @SuppressWarnings("checkstyle:HiddenField")
    public PriorityFailoverBuilder<T> enableAutoPriority(int... coreGroupSizes) {
        this.coreGroupSizes = coreGroupSizes;
        return this;
    }

    /**
     * 设定健康检查时间间隔，注意后台健康检查器是懒启动的，只有在有资源出现访问失败以后才会启动。
     * @param checkDuration 健康检查时间间隔
     * @return this
     * @see #startCheckTaskImmediately(boolean)
     */
    public PriorityFailoverBuilder<T> checkDuration(Duration checkDuration) {
        requireNonNull(checkDuration);
        config.setCheckDuration(checkDuration);
        return this;
    }

    /**
     * 指定健康检查使用的线程池，如果不指定会用默认的。
     * @param checkExecutor 线程池
     * @return this
     */
    public PriorityFailoverBuilder<T> checkExecutor(ScheduledExecutorService checkExecutor) {
        requireNonNull(checkExecutor);
        config.setCheckExecutor(checkExecutor);
        return this;
    }

    /**
     * 注册健康检查器回调，检查器传入参数为资源，输出资源是否健康。
     * @param checker 检查器
     * @return this
     */
    public PriorityFailoverBuilder<T> checker(Predicate<T> checker) {
        requireNonNull(checker);
        config.setChecker(checker);
        return this;
    }

    /**
     * 设置构造后立即启动后台健康检查任务。
     * @param startCheckTaskImmediately 是否立即启动健康检查任务
     * @return this
     */
    public PriorityFailoverBuilder<T> startCheckTaskImmediately(boolean startCheckTaskImmediately) {
        config.setStartCheckTaskImmediately(startCheckTaskImmediately);
        return this;
    }

    /**
     * 是否激活并发度控制，默认false，激活并发度控制后，getOneAvailable/getOneAvailableExclude和success/fail/down必须匹配，
     * 否则并发度计算会出错。
     * @param concurrencyControl 激活并发度控制
     * @return this
     */
    public PriorityFailoverBuilder<T> concurrencyControl(boolean concurrencyControl) {
        config.setConcurrencyControl(concurrencyControl);
        return this;
    }

    /**
     * 启用AliasMethod算法的资源数量阈值，AliasMethod算法是O(1)，但是如果资源总数少，是没有收益的，默认值是10。
     * @param aliasMethodThreshold 启用AliasMethod算法的资源数量阈值
     * @return this
     */
    public PriorityFailoverBuilder<T> aliasMethodThreshold(int aliasMethodThreshold) {
        config.setAliasMethodThreshold(aliasMethodThreshold);
        return this;
    }

    /**
     * 代表资源的配置maxWeight/minWeight/priority/initWeight，这是个不可变对象。
     */
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
