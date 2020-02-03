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

import com.github.phantomthief.failover.impl.PriorityFailover;
import com.github.phantomthief.failover.impl.PriorityFailoverBuilder;
import com.github.phantomthief.failover.impl.SimpleWeightFunction;

/**
 * @author huangli
 * Created on 2020-01-21
 */
@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Threads(200)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 2)
@State(Scope.Benchmark)
public class Group1PriorityFailover {

    @Param({"5", "20", "100", "200", "1000"})
    private int totalSize;

    private static final int FAIL_RATE = 999;

    private PriorityFailover<String> priorityFailover;

    private long count;

    @Setup
    public void init() {
        PriorityFailoverBuilder<String> builder = PriorityFailover.<String> newBuilder();
        for (int i = 0; i < totalSize; i++) {
            builder.addResource("key" + i, 100);
        }
        builder.weightFunction(new SimpleWeightFunction<>(0.01, 1.0));
        priorityFailover = builder.build();
    }


    @Benchmark
    public void getOneSuccess() {
        for (int i = 0; i < 1000; i++) {
            String one = priorityFailover.getOneAvailable();
            priorityFailover.success(one);
        }
    }

    @Benchmark
    public void getOneFail() {
        for (int i = 0; i < 1000; i++) {
            String one = priorityFailover.getOneAvailable();
            if (count++ % FAIL_RATE == 0) {
                priorityFailover.fail(one);
            } else {
                priorityFailover.success(one);
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        boolean useJmh = true;
        if (useJmh) {
            Options options = new OptionsBuilder()
                    .include(Group1PriorityFailover.class.getSimpleName())
                    .output(System.getProperty("user.home") + "/" + Group1PriorityFailover.class.getSimpleName()
                            + ".txt")
                    .build();
            new Runner(options).run();
        } else {
            Group1PriorityFailover obj = new Group1PriorityFailover();
            obj.totalSize = 5;
            obj.init();
            int loopCount = 1_0000_0000;
            for (int i = 0; i < 100000; i++) {
                obj.getOneFail();
            }
            long start = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                obj.getOneFail();
            }
            long end = System.nanoTime();
            double seconds = (end - start) / 1000.0 / 1000.0 / 1000.0;
            System.out.printf("tps:%.2f", 1.0 * loopCount / seconds);
        }
    }
}
