package com.github.phantomthief.failover.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.util.SharedResource.UnregisterFailedException;

/**
 * @author w.vela
 * Created on 16/2/19.
 */
class SharedResourceTest {

    private static SharedResource<String, MockResource> resources = new SharedResource<>();

    @Test
    void test() {
        MockResource mock = resources.register("1", MockResource::new);
        assertSame(mock, resources.get("1"));
        assertSame(mock, resources.register("1", MockResource::new));
        resources.register("2", MockResource::new);

        MockResource mockResource1 = resources.get("1");
        MockResource mockResource2 = resources.get("1");
        assertNotNull(mockResource1);
        assertSame(mockResource1, mockResource2);
        assertFalse(mockResource1.isShutdown());

        resources.unregister("1", MockResource::close);
        mockResource1 = resources.get("1");
        assertNotNull(mockResource1);
        assertFalse(mockResource1.isShutdown());

        resources.unregister("1", MockResource::close);
        assertTrue(mockResource1.isShutdown());
        mockResource1 = resources.get("1");
        assertNull(mockResource1);
    }

    @Test
    void testUnpairUnregister() {
        resources.register("3", MockResource::new);
        MockResource mock = resources.get("3");
        assertSame(mock, resources.unregister("3", MockResource::close));
        assertThrows(IllegalStateException.class,
                () -> resources.unregister("3", MockResource::close));
    }

    @Test
    void testCleanupFailed() {
        resources.register("4", MockResource::new);
        MockResource mock = resources.get("4");
        UnregisterFailedException e = assertThrows(UnregisterFailedException.class,
                () -> resources.unregister("4", it -> {
                    throw new IllegalArgumentException();
                }));
        assertSame(mock, e.getRemoved());
        assertSame(IllegalArgumentException.class, e.getCause().getClass());
    }

    @Test
    void testRegFail() {
        assertThrows(RuntimeException.class, () -> resources.register("5", (s) -> {
            throw new RuntimeException();
        }));
        MockResource mockResource = resources.get("5");
        assertNull(mockResource);
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
