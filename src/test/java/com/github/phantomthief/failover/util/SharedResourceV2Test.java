package com.github.phantomthief.failover.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.util.SharedResourceV2.UnregisterFailedException;

/**
 * @author w.vela
 * Created on 16/2/19.
 */
class SharedResourceV2Test {

    private static SharedResourceV2<String, MockResource> resources =
            new SharedResourceV2<>(MockResource::new, MockResource::close);

    @Test
    void test() {
        MockResource mock = resources.register("1");
        assertSame(mock, resources.get("1"));
        assertSame(mock, resources.register("1"));
        resources.register("2");

        MockResource mockResource1 = resources.get("1");
        MockResource mockResource2 = resources.get("1");
        assertNotNull(mockResource1);
        assertSame(mockResource1, mockResource2);
        assertFalse(mockResource1.isShutdown());

        resources.unregister("1");
        mockResource1 = resources.get("1");
        assertNotNull(mockResource1);
        assertFalse(mockResource1.isShutdown());

        resources.unregister("1");
        assertTrue(mockResource1.isShutdown());
        mockResource1 = resources.get("1");
        assertNull(mockResource1);
    }

    @Test
    void testUnpairUnregister() {
        resources.register("3");
        MockResource mock = resources.get("3");
        assertSame(mock, resources.unregister("3"));
        assertThrows(IllegalStateException.class,
                () -> resources.unregister("3"));
    }

    @Test
    void testCleanupFailed() {
        SharedResourceV2<String, MockResource> resources =
                new SharedResourceV2<>(MockResource::new, it -> {
                    throw new IllegalArgumentException();
                });
        resources.register("4");
        MockResource mock = resources.get("4");
        UnregisterFailedException e = assertThrows(UnregisterFailedException.class,
                () -> resources.unregister("4"));
        assertSame(mock, e.getRemoved());
        assertSame(IllegalArgumentException.class, e.getCause().getClass());
    }

    @Test
    void testRegFail() {
        SharedResourceV2<String, MockResource> resources =
                new SharedResourceV2<>(s -> {
                    throw new RuntimeException();
                }, MockResource::close);
        assertThrows(RuntimeException.class, () -> resources.register("5"));
        MockResource mockResource = resources.get("5");
        assertNull(mockResource);
        assertThrows(IllegalStateException.class, () -> resources.unregister("5"));
    }

    private static class MockResource {

        private String name;
        private boolean shutdown = false;

        MockResource(String name) {
            this.name = name;
        }

        boolean isShutdown() {
            return shutdown;
        }

        void close() {
            if (shutdown) {
                fail("failed");
            }
            shutdown = true;
            System.out.println("shutdown:" + name);
        }
    }
}
