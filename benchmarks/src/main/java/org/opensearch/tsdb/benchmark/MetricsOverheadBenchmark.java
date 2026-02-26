/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.benchmark.metrics.BenchmarkCounter;
import org.opensearch.tsdb.benchmark.metrics.BenchmarkHistogram;
import org.opensearch.tsdb.benchmark.metrics.BenchmarkMetricsRegistry;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark comparing BenchmarkMetricsRegistry overhead vs noop.
 *
 * <p>Demonstrates that the benchmark registry performs real work (HashMap allocation,
 * ConcurrentHashMap lookup, atomic operations) on each metrics call, unlike the
 * noop registry which skips all work.</p>
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class MetricsOverheadBenchmark {

    @Param({ "noop", "benchmark" })
    private String registryType;

    private Counter counter;
    private Histogram histogram;
    private Tags tags;

    @Setup
    public void setup() {
        tags = Tags.create().addTag("index", "my-metrics-index").addTag("shard", 0L);

        TSDBMetrics.cleanup();

        if ("benchmark".equals(registryType)) {
            TSDBMetrics.initialize(new BenchmarkMetricsRegistry());
            counter = new BenchmarkCounter("tsdb.engine.samples_ingested");
            histogram = new BenchmarkHistogram("tsdb.engine.index_latency");
        } else {
            // noop: leave TSDBMetrics uninitialized, use noop counter/histogram
            counter = new NoopCounter();
            histogram = new NoopHistogram();
        }
    }

    @TearDown
    public void tearDown() {
        TSDBMetrics.cleanup();
    }

    @Benchmark
    public void counterAdd(Blackhole bh) {
        counter.add(1, tags);
    }

    @Benchmark
    public void histogramRecord(Blackhole bh) {
        histogram.record(42.5, tags);
    }

    // Noop implementations matching what TSDBMetrics does when uninitialized
    private static class NoopCounter implements Counter {
        @Override
        public void add(double value) {}

        @Override
        public void add(double value, Tags tags) {}
    }

    private static class NoopHistogram implements Histogram {
        @Override
        public void record(double value) {}

        @Override
        public void record(double value, Tags tags) {}
    }
}
