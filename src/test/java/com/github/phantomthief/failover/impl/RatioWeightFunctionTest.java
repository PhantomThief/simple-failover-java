package com.github.phantomthief.failover.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * @author huangli
 * Created on 2020-05-06
 */
public class RatioWeightFunctionTest {

    @Test
    public void testFail1() {
        RatioWeightFunction<String> f = new RatioWeightFunction<>(0.5, 0.01);
        assertEquals(0.5, f.fail(1, 0, 0, 1, "R1"));
        assertEquals(0.25, f.fail(1, 0, 0, 0.5, "R1"));
        assertEquals(0.125, f.fail(1, 0, 0, 0.25, "R1"));
        assertEquals(0, f.fail(1, 0, 0, 0, "R1"));
    }


    @Test
    public void testFail2() {
        RatioWeightFunction<String> f = new RatioWeightFunction<>(0.5, 0.01, 1, 0.2);
        assertEquals(0.5, f.fail(1, 0, 0, 1, "R1"));
        assertEquals(0.25, f.fail(1, 0, 0, 0.5, "R1"));
        assertEquals(0, f.fail(1, 0, 0, 0.25, "R1"));
        assertEquals(0, f.fail(1, 0, 0, 0, "R1"));
    }

    @Test
    public void testSuccess1() {
        RatioWeightFunction<String> f = new RatioWeightFunction<>(0.5, 0.5);
        assertEquals(0.5, f.success(1, 0, 0, 0, "R1"));
        assertEquals(1, f.success(1, 0, 0, 0.5, "R1"));
        assertEquals(1, f.success(1, 0, 0, 1, "R1"));
    }

    @Test
    public void testSuccess2() {
        RatioWeightFunction<String> f = new RatioWeightFunction<>(0.5, 0.5, 2);

        assertEquals(0, f.success(1, 0, 0, 0, "R1"));

        assertEquals(0, f.success(1, 0, 0, 0, "R2"));
        assertEquals(0.5, f.success(1, 0, 0, 0, "R2"));
        assertEquals(1, f.success(1, 0, 0, 0.5, "R2"));
        assertEquals(1, f.success(1, 0, 0, 1, "R2"));

        assertEquals(0, f.fail(1, 0, 0, 0, "R1"));
        assertEquals(0, f.success(1, 0, 0, 0, "R1"));
        assertEquals(0.5, f.success(1, 0, 0, 0, "R1"));
        assertEquals(1, f.success(1, 0, 0, 0.5, "R1"));
        assertEquals(0.5, f.fail(1, 0, 0, 1, "R1"));
        assertEquals(1, f.success(1, 0, 0, 0.5, "R1"));
        assertEquals(1, f.success(1, 0, 0, 1, "R1"));
    }
}
