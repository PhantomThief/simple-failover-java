package com.github.phantomthief.failover.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * @author w.vela
 * Created on 16/2/19.
 */
public class SharedResourceTest {

    private static SharedResource<String, MockResource> resources = new SharedResource<>();

    @Test
    public void test() throws Exception {
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
    public void testUnpairUnregister() {
        resources.register("3", MockResource::new);
        resources.unregister("3", MockResource::close);
        try {
            resources.unregister("3", MockResource::close);
            fail("fail.");
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
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
                fail();
            }
            shutdown = true;
            System.out.println("shutdown:" + name);
        }
    }
}
