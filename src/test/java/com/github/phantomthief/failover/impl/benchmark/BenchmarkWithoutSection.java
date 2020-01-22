package com.github.phantomthief.failover.impl.benchmark;

import java.util.List;

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

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.SimpleFailover;
import com.github.phantomthief.failover.impl.PriorityFailover;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder;
import com.github.phantomthief.failover.impl.SimpleWeightFunction;
import com.github.phantomthief.failover.impl.WeightFailover;
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
public class BenchmarkWithoutSection {

    @Param({"5", "20", "100", "200", "1000"})
    private int size;

    private static final int FAIL_RATE = 999;

    private SimpleFailover<String> priorityFailover;
    private Failover<String> weightFailover;

    private long count;

    @Setup
    public void init() {
        {
            PriorityFailoverBuilder<String> builder = PriorityFailover.<String> newBuilder();
            for (int i = 0; i < size; i++) {
                builder.addResource("key" + i, 100);
            }
            builder.weightFunction(new SimpleWeightFunction<>(0.01, 1.0));
            priorityFailover = builder.build();
        }

        {
            Builder<String, Integer> builder = ImmutableMap.builder();
            for (int i = 0; i < size; ++i) {
                builder.put("key" + i, 100);
            }
            weightFailover = WeightFailover.<String> newGenericBuilder()
                    .checker(it -> true, 1)
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
    public void getOneSuccess_weightFailover() {
        String one = weightFailover.getOneAvailable();
        weightFailover.success(one);
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
    public void getOneFail_weightFailover() {
        String one = weightFailover.getOneAvailable();
        if (count++ % FAIL_RATE == 0) {
            weightFailover.fail(one);
        } else {
            weightFailover.success(one);
        }
    }

    @Benchmark
    public void getAvailableSuccess_weightFailover() {
        List<String> list = weightFailover.getAvailable();
        weightFailover.success(list.get((int) (count % size)));
    }

    @Benchmark
    public void getAvailableFail_weightFailover() {
        List<String> list = weightFailover.getAvailable();
        if (count++ % FAIL_RATE == 0) {
            weightFailover.fail(list.get((int) (count % size)));
        } else {
            weightFailover.success(list.get((int) (count % size)));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(BenchmarkWithoutSection.class.getSimpleName())
                .output(System.getProperty("user.home") + "/BenchmarkWithoutSection.txt")
                .build();
        new Runner(options).run();
    }
}
