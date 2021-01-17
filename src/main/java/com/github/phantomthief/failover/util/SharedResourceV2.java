package com.github.phantomthief.failover.util;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;

import com.github.phantomthief.tuple.TwoTuple;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author myco
 * Created on 2020-10-22
 * @see SharedResource
 * 新版 shared resource 实现，行为具体如下：
 */
public class SharedResourceV2<K, V> {

    private static final Logger logger = getLogger(SharedResourceV2.class);

    private final ConcurrentMap<K, ResourceWrapper<K, V>> resourceMap = new ConcurrentHashMap<>();
    private final Function<K, V> factory;
    private final ThrowableConsumer<V, Throwable> cleanup;

    public SharedResourceV2(@Nonnull Function<K, V> factory,
            @Nonnull ThrowableConsumer<V, Throwable> cleanup) {
        this.factory = factory;
        this.cleanup = cleanup;
    }

    /**
     * 不管是否成功，都只get一次，然后缓存起来
     */
    private static class OnceSupplier<T> implements Supplier<T> {

        private final Supplier<TwoTuple<T, Throwable>> delegate;

        public OnceSupplier(Object key, Supplier<T> delegate) {
            this.delegate = lazy(() -> {
                T value = null;
                Throwable throwable = null;
                try {
                    value = delegate.get();
                    logger.info("create shared resource for key: [{}] => [{}]", key, value);
                } catch (Throwable t) {
                    throwable = t;
                }
                return tuple(value, throwable);
            });
        }

        @Override
        public T get() {
            TwoTuple<T, Throwable> tuple = delegate.get();
            Throwable throwable = tuple.getSecond();
            if (throwable != null) {
                throw new OnceBrokenException(throwable);
            } else {
                return tuple.getFirst();
            }
        }
    }

    private static class ResourceWrapper<K, V> {

        private final K key;
        private final OnceSupplier<V> resourceSupplier;

        @GuardedBy("ResourceWrapper::this")
        private volatile int counter = 0;
        @GuardedBy("ResourceWrapper::this")
        private volatile boolean expired = false;

        public ResourceWrapper(K key, @Nonnull Supplier<V> resourceSupplier) {
            this.key = key;
            this.resourceSupplier = new OnceSupplier<>(key, resourceSupplier);
        }

        @Nonnull
        public V get() {
            return resourceSupplier.get();
        }

        public int count() {
            return counter;
        }

        public boolean incr() {
            synchronized (this) {
                if (!expired) {
                    counter++;
                    logger.info("incr success: [{}], refCount: [{}], expired: [{}]", key, counter, expired);
                    return true;
                }
            }
            return false;
        }

        public boolean decr() {
            synchronized (this) {
                if (counter <= 0) {
                    throw new AssertionError("INVALID INTERNAL STATE:" + key);
                }
                if (!expired) {
                    counter--;
                    if (counter <= 0) {
                        expired = true;
                    }
                    logger.info("decr success: [{}], refCount: [{}], expired: [{}]", key, counter, expired);
                    return true;
                }
            }
            return false;
        }
    }

    @Nonnull
    private ResourceWrapper<K, V> ensureWrapperExist(@Nonnull K key) {
        return resourceMap.compute(key, (k, v) -> {
            if (v == null || v.expired) {
                return new ResourceWrapper<>(k,
                        () -> Objects.requireNonNull(factory.apply(k), "factory 不应返回 null, key: " + key));
            } else {
                return v;
            }
        });
    }

    private void removeExpiredWrapper(@Nonnull K key) {
        resourceMap.compute(key, (k, v) -> {
            if (v != null && v.expired) {
                return null;
            } else {
                return v;
            }
        });
    }

    /**
     * 获取之前注册且未被注销的资源。
     * 使用相同 key 并发调用 get 和 register/unregister 时，本方法的返回值是不确定的（可能是null，也可能是之前注册的值）
     * 真实使用场景下，应避免相同的 key 并发 get/register/unregister，虽然本类是线程安全的，但是并发调用这些方法会让人很难理解
     * 真正共享的资源到底是哪一个
     */
    @Nullable
    public V get(@Nonnull K key) {
        ResourceWrapper<K, V> resourceWrapper = resourceMap.get(key);
        if (resourceWrapper != null) {
            return resourceWrapper.get();
        }
        return null;
    }

    /**
     * 这个方法实现的过程中，踩了挺多坑。这里把当前实现的关键思路记录一下，看后续有没有更好的实现方法:
     * 1. 如果之前成功创建过相同 key 的资源，就应当复用之前创建的资源；所以不能一上来就用 factory 创建资源
     * 2. 为了避免 unregister 已经在清理中的资源再次被增加引用计数，肯定需要加锁；为了避免全局锁，首先想到可以对 key 加锁
     * 3. 但是 key 有可能出现不同对象，但是 equals & hashcode 相同的情况，所以直接对 key 加锁会有问题
     * 4. key 不能用于加锁，那能不能对 value resourceWrapper 加锁呢？也有问题，因为为了避免资源泄漏，value 在引用计数减到 0 的时候
     * 需要删掉，这样导致有可能锁没有加在同一个对象上
     * 5. 所以目前采用这样的方式：我们在 resourceWrapper 上记录一个状态，如果某个资源的引用计数降到 0 则认为这个资源已经过期，
     * 不能继续使用/增加计数了；在 register 和 unregister 过程中发现已经过期的资源，采用清理 + 重建的方式来解决
     */
    @Nonnull
    public V register(@Nonnull K key) {
        boolean incr = false;
        ResourceWrapper<K, V> resourceWrapper = null;
        while (!incr) {
            resourceWrapper = ensureWrapperExist(key);
            // 确保引用计数加成功才退出
            incr = resourceWrapper.incr();
        }
        try {
            // 尝试初始化资源
            return resourceWrapper.get();
        } catch (Throwable t) {
            // 初始化资源失败的话，把引用计数恢复一下，同时清理下 resourceMap 里引用计数降到 0 的记录
            unregister(key);
            throw t;
        }
    }

    @Nonnull
    public V unregister(@Nonnull K key) {
        boolean decr = false;
        ResourceWrapper<K, V> resourceWrapper = null;
        while (!decr) {
            resourceWrapper = resourceMap.get(key);
            if (resourceWrapper == null) {
                throw new IllegalStateException("non paired unregister call for key:" + key);
            }
            decr = resourceWrapper.decr();
            removeExpiredWrapper(key);
        }
        V resource = resourceWrapper.get();
        if (resourceWrapper.expired) {
            try {
                cleanup.accept(resource);
                logger.info("cleanup resource: [{}] => [{}]", key, resource);
            } catch (Throwable e) {
                throw new UnregisterFailedException(e, resource);
            }
        }
        return resource;
    }

    public static class UnregisterFailedException extends RuntimeException {

        private final Object removed;

        private UnregisterFailedException(Throwable cause, Object removed) {
            super(cause);
            this.removed = removed;
        }

        @SuppressWarnings("unchecked")
        public <T> T getRemoved() {
            return (T) removed;
        }
    }

    public static class OnceBrokenException extends RuntimeException {

        private OnceBrokenException(Throwable cause) {
            super("resource broken", cause);
        }
    }
}
