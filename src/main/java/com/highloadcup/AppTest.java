package com.highloadcup;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Warmup(iterations=3)
@Measurement(iterations=10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AppTest
{

    final int accounts = 1300000/64;

    long[] arr1;
    long[] arr2;


    long[] allItr;
    long[] accItr;

    public static void main(String[] args) throws Exception
    {
        Options opt = new OptionsBuilder()
                // Specify which benchmarks to run.
                // You can be more specific if you'd like to run only one benchmark per test.
                .include(AppTest.class.getSimpleName())
                // Set the following options as needed
                .mode (Mode.AverageTime)
                .timeUnit(TimeUnit.NANOSECONDS)
                //.warmupTime(TimeValue.seconds(1))
                .warmupIterations(15_000)
                //.measurementTime(TimeValue.seconds(1))
                .measurementIterations(50_000)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
                //.addProfiler(WinPerfAsmProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Invocation)
    public void init() {
        arr1 = new long[accounts];
        arr2 = new long[accounts];
        Arrays.fill(arr1, 43243275687687123L);
        Arrays.fill(arr2, 8767476573575676546L);

        allItr = new long[5000 * 2];
        accItr = new long[]{43214321L, 4321424123L};

    }

    /*
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations=20_000)
    @Measurement(iterations=50_000)
    @CompilerControl(CompilerControl.Mode.INLINE)
    public void benchArrayIntersect() {
        DirectMemoryAllocator.intersectArrays(arr1, arr2);
    }*/


    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations=20_000)
    @Measurement(iterations=50_000)
    @CompilerControl(CompilerControl.Mode.INLINE)
    public void benchInterestsIntersect() {
        for (int i = 0; i < allItr.length; i += 2) {
            allItr[i] = allItr[i] & accItr[0];
            allItr[i+1] = allItr[i+1] & accItr[1];
        }
    }


}
