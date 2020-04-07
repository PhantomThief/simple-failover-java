package com.github.phantomthief.failover.impl.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.github.phantomthief.failover.util.AliasMethod;
import com.github.phantomthief.failover.util.Weight;

/**
 * Benchmark                        (totalSize)   Mode  Cnt          Score            Error  Units
 * WeightBenchmark.testAliasMethod           10  thrpt    3  531373408.041 ± 1310821420.530  ops/s
 * WeightBenchmark.testAliasMethod          100  thrpt    3  524611078.970 ±  567444175.501  ops/s
 * WeightBenchmark.testAliasMethod         1000  thrpt    3  442690581.938 ±  705466764.645  ops/s
 * WeightBenchmark.testWeight                10  thrpt    3  101635713.806 ±   28607384.102  ops/s
 * WeightBenchmark.testWeight               100  thrpt    3   59522839.677 ±   33694178.059  ops/s
 * WeightBenchmark.testWeight              1000  thrpt    3   36993978.805 ±    4766898.860  ops/s
 *
 * @author w.vela
 * Created on 2020-04-07.
 */
@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Threads(10)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 1)
@State(Scope.Benchmark)
public class WeightBenchmark {

    @Param({"10", "100", "1000"})
    private int totalSize;

    private Weight<String> weight;
    private AliasMethod<String> aliasMethod;

    @Setup
    public void init() {
        weight = new Weight<>();
        Map<String, Integer> weightMap = new HashMap<>();
        for (int i = 0; i < totalSize; i++) {
            int weightValue = ThreadLocalRandom.current().nextInt(1, 100);
            String node = "key" + i;
            weight.add(node, weightValue);
            weightMap.put(node, weightValue);
        }
        aliasMethod = new AliasMethod<>(weightMap);
    }


    @Benchmark
    public void testWeight() {
        weight.get();
    }

    @Benchmark
    public void testAliasMethod() {
        aliasMethod.get();
    }
}
