package com.github.phantomthief.failover.impl;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2017-11-22.
 */
class DummyFailoverTest {

    @Test
    void testSupplier() {
        AtomicReference<String> atomicReference = new AtomicReference<>();
        DummyFailover<String> testSupplier = DummyFailover.ofSingleSupplier(atomicReference::get);
        assertEquals(singletonList(atomicReference.get()), testSupplier.getAvailable());
        atomicReference.set("test");
        assertEquals(singletonList(atomicReference.get()), testSupplier.getAvailable());
        atomicReference.set("test1");
        assertEquals(singletonList(atomicReference.get()), testSupplier.getAvailable());
    }
}