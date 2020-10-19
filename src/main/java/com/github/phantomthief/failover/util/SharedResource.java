package com.github.phantomthief.failover.util;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableConsumer;

/**
 * 多处使用的资源, 需要在所有使用者都注销之后, 再进行清理, 类似于引用计数.
 * <p>
 * TODO: 其实和netty的io.netty.util.ReferenceCounted很相似
 * 回头可以考虑使用和netty一样的方式，使用无锁的方式……
 * <p>
 * TODO: 回头挪到common-util包中
 *
 * @author w.vela
 * Created on 16/2/19.
 */
public class SharedResource<K, V> {

    private static final Logger logger = getLogger(SharedResource.class);

    private final ConcurrentMap<K, ResourceWrapper<K, V>> resourceMap = new ConcurrentHashMap<>();

    /**
     * 不管是否成功，都只get一次，然后缓存起来
     */
    private static class OnceSupplier<T> implements Supplier<T> {

        private enum Status {
            CREATED,
            ACTIVATED,
            BROKEN
        }

        private final Supplier<T> delegate;
        private final AtomicReference<OnceSupplier.Status> status = new AtomicReference<>(OnceSupplier.Status.CREATED);

        public OnceSupplier(Supplier<T> delegate) {
            this.delegate = lazy(delegate);
        }

        private synchronized T doInit() {
            if (status.get() == OnceSupplier.Status.ACTIVATED) {
                return delegate.get();
            } else if (status.get() == OnceSupplier.Status.BROKEN) {
                throw new RuntimeException("resource broken");
            }
            try {
                T resource = delegate.get();
                status.set(OnceSupplier.Status.ACTIVATED);
                return resource;
            } catch (Throwable t) {
                status.set(OnceSupplier.Status.BROKEN);
                throw t;
            }
        }

        @Override
        public T get() {
            if (status.get() == OnceSupplier.Status.ACTIVATED) {
                return delegate.get();
            } else if (status.get() == OnceSupplier.Status.BROKEN) {
                throw new RuntimeException("resource broken");
            }
            return doInit();
        }

        public OnceSupplier.Status getStatus() {
            return status.get();
        }
    }

    private static class ResourceWrapper<K, V> {

        private final K key;
        private final OnceSupplier<V> resourceSupplier;

        @javax.annotation.concurrent.GuardedBy("ResourceWrapper::this")
        private volatile int counter = 0;
        @GuardedBy("ResourceWrapper::this")
        private volatile boolean expired = false;

        public ResourceWrapper(K key, @Nonnull Supplier<V> resourceSupplier) {
            this.key = key;
            this.resourceSupplier = new OnceSupplier<>(resourceSupplier);
        }

        @Nonnull
        public V get() {
            return resourceSupplier.get();
        }

        public OnceSupplier.Status getResourceStatus() {
            return resourceSupplier.getStatus();
        }

        public int count() {
            return counter;
        }

        public boolean incr() {
            synchronized (this) {
                if (!expired) {
                    counter++;
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
                    return true;
                }
            }
            return false;
        }
    }

    @Nonnull
    private ResourceWrapper<K, V> ensureWrapperExist(@Nonnull K key, @Nonnull Supplier<V> resource) {
        return resourceMap.compute(key, (k, v) -> {
            if (v == null || v.expired) {
                return new ResourceWrapper<>(k, resource);
            } else {
                return v;
            }
        });
    }

    @Nullable
    private ResourceWrapper<K, V> removeExpiredWrapper(@Nonnull K key) {
        AtomicReference<ResourceWrapper<K, V>> oldResourceWrapper = new AtomicReference<>(null);
        resourceMap.compute(key, (k, v) -> {
            if (v != null && v.expired) {
                oldResourceWrapper.set(v);
                return null;
            } else {
                return v;
            }
        });
        return oldResourceWrapper.get();
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
    public V register(@Nonnull K key, @Nonnull Function<K, V> factory) {
        boolean incr = false;
        ResourceWrapper<K, V> resourceWrapper = null;
        while (!incr) {
            resourceWrapper =
                    ensureWrapperExist(key, () -> Objects.requireNonNull(factory.apply(key), "factory 不应返回 null"));
            // 确保引用计数加成功才退出
            incr = resourceWrapper.incr();
        }
        try {
            // 尝试初始化资源
            return resourceWrapper.get();
        } catch (Throwable t) {
            // 初始化资源失败的话，把引用计数恢复一下，同时清理下 resourceMap 里引用计数降到 0 的记录
            unregister(key, v -> { });
            throw t;
        }
    }

    @Nonnull
    public V unregister(@Nonnull K key, @Nonnull ThrowableConsumer<V, Throwable> cleanup) {
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
                logger.info("cleanup resource:{}->{}", key, resource);
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

}
