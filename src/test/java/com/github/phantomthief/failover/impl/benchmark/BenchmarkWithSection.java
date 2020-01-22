package com.github.phantomthief.failover.impl.benchmark;

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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.github.phantomthief.failover.SimpleFailover;
import com.github.phantomthief.failover.impl.PartitionFailover;
import com.github.phantomthief.failover.impl.PriorityFailover;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder;
import com.github.phantomthief.failover.impl.SimpleWeightFunction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * @author huangli
 * Created on 2020-01-21
 */
@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Threads(200)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 4)
@State(Scope.Benchmark)
public class BenchmarkWithSection {

    @Param({"1000"})
    private int totalSize;

    @Param({"5", "20"})
    private int coreSize;

    @Param({"true", "false"})
    private boolean concurrencyCtrl;

    private static final int FAIL_RATE = 999;

    private SimpleFailover<String> priorityFailover;
    private SimpleFailover<String> partitionFailover;

    private long count;

    @Setup
    public void init() {
        {
            PriorityFailoverBuilder<String> builder = PriorityFailover.<String> newBuilder();
            for (int i = 0; i < totalSize; i++) {
                if (i < coreSize) {
                    builder.addResource("key" + i, 100, 0, 0, 100);
                } else {
                    builder.addResource("key" + i, 100, 0, 1, 100);
                }
            }
            builder.concurrencyControl(concurrencyCtrl);
            builder.weightFunction(new SimpleWeightFunction<>(0.01, 1.0));
            priorityFailover = builder.build();
        }

        {
            Builder<String, Integer> builder = ImmutableMap.builder();
            for (int i = 0; i < totalSize; ++i) {
                builder.put("key" + i, 100);
            }
            partitionFailover = PartitionFailover.<String> newBuilder()
                    .checker(it -> true, 1)
                    .corePartitionSize(coreSize)
                    .failReduceRate(0.01)
                    .successIncreaseRate(1.0)
                    .build(builder.build());
        }
    }


    @Benchmark
    public void getOneSuccess_priorityFailover() {
        String one = priorityFailover.getOneAvailable();
        priorityFailover.success(one);
    }

    @Benchmark
    public void getOneSuccess_partitionFailover() {
        String one = partitionFailover.getOneAvailable();
        partitionFailover.success(one);
    }

    @Benchmark
    public void getOneFail_priorityFailover() {
        String one = priorityFailover.getOneAvailable();
        if (count++ % FAIL_RATE == 0) {
            priorityFailover.fail(one);
        } else {
            priorityFailover.success(one);
        }
    }

    @Benchmark
    public void getOneFail_partitionFailover() {
        String one = partitionFailover.getOneAvailable();
        if (count++ % FAIL_RATE == 0) {
            partitionFailover.fail(one);
        } else {
            partitionFailover.success(one);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(BenchmarkWithSection.class.getSimpleName())
                .output(System.getProperty("user.home") + "/BenchmarkWithSection.txt")
                .build();
        new Runner(options).run();
    }
}
