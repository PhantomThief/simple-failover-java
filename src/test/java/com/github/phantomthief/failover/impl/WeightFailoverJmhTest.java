package com.github.phantomthief.failover.impl;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * @author lijie
 * Created on 2019-02-17
 */
@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 2, time = 2)
@State(Scope.Benchmark)
public class WeightFailoverJmhTest {

    @Param({"5", "20", "100", "200", "1000"})
    public int size;

    private WeightFailover<String> failover;

    @Setup
    public void init() {
        Builder<String, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < size; ++i) {
            builder.put("key" + i, i);
        }
        failover = WeightFailover.<String> newGenericBuilder()
                .checker(it -> true, 1)
                .build(builder.build());
    }

    @Benchmark
    public void getAvailableOpitmized() {
        failover.getAvailable();
    }

    @Benchmark
    public void getAvailable() {
        failover.getAvailable(size);
    }
}
