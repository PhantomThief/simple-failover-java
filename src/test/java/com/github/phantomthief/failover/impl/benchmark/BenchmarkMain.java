package com.github.phantomthief.failover.impl.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * @author huangli
 * Created on 2020-01-21
 */
public class BenchmarkMain {
    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(Group1PriorityFailover.class.getSimpleName())
                .include(Group1WeightFailover.class.getSimpleName())
                .include(Group2PriorityFailover.class.getSimpleName())
                .include(Group2PartitionFailover.class.getSimpleName())
                .output(System.getProperty("user.home") + "/benchmark.txt")
                .build();
        new Runner(options).run();
    }
}
