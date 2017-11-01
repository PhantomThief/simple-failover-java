package com.github.phantomthief.failover.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 16/2/19.
 */
class SharedResourceTest {

    private static SharedResource<String, MockResource> resources = new SharedResource<>();

    @Test
    void test() {
        resources.register("1", MockResource::new);
        resources.register("1", MockResource::new);
        resources.register("2", MockResource::new);

        MockResource mockResource1 = resources.get("1");
        MockResource mockResource2 = resources.get("1");
        assertTrue(mockResource1 == mockResource2);
        assertFalse(mockResource1.isShutdown());

        resources.unregister("1", MockResource::close);
        mockResource1 = resources.get("1");
        assertFalse(mockResource1.isShutdown());

        resources.unregister("1", MockResource::close);
        assertTrue(mockResource1.isShutdown());
        mockResource1 = resources.get("1");
        assertTrue(mockResource1 == null);
    }

    @Test
    void testUnpairUnregister() {
        resources.register("3", MockResource::new);
        resources.unregister("3", MockResource::close);
        assertThrows(IllegalStateException.class,
                () -> resources.unregister("3", MockResource::close));
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
