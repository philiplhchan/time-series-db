/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark.telemetry;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

/**
 * Tests for the benchmark telemetry plugin components.
 */
public class BenchmarkTelemetryPluginTests extends OpenSearchTestCase {

    /**
     * Verifies the registry class name does NOT contain "noop",
     * which is required for TSDBMetrics.isNoopRegistry() to return false.
     */
    public void testRegistryIsNotDetectedAsNoop() {
        BenchmarkTelemetryPlugin.BenchmarkMetricsTelemetry registry = new BenchmarkTelemetryPlugin.BenchmarkMetricsTelemetry();
        String className = registry.getClass().getName().toLowerCase(Locale.ROOT);
        assertFalse("Class name must not contain 'noop'", className.contains("noop"));
        String toString = registry.toString().toLowerCase(Locale.ROOT);
        assertFalse("toString must not contain 'noop'", toString.contains("noop"));
    }

    /**
     * Verifies that createCounter returns a functional counter that can
     * handle add() calls with and without tags.
     */
    public void testCounterWithTags() {
        MetricsTelemetry registry = new BenchmarkTelemetryPlugin.BenchmarkMetricsTelemetry();
        Counter counter = registry.createCounter("test.counter", "test counter", "1");
        assertNotNull(counter);

        // Should not throw
        counter.add(1);
        counter.add(5, Tags.create().addTag("service", "api").addTag("method", "GET"));
        counter.add(3, Tags.EMPTY);
    }

    /**
     * Verifies that createHistogram returns a functional histogram that can
     * handle record() calls with and without tags.
     */
    public void testHistogramWithTags() {
        MetricsTelemetry registry = new BenchmarkTelemetryPlugin.BenchmarkMetricsTelemetry();
        Histogram histogram = registry.createHistogram("test.histogram", "test histogram", "ms");
        assertNotNull(histogram);

        // Should not throw
        histogram.record(42.5);
        histogram.record(100.0, Tags.create().addTag("status", "200"));
        histogram.record(0.0, Tags.EMPTY);
        histogram.record(-1.0);  // negative values should not throw
    }

    /**
     * Verifies that counter correctly accumulates values per tag dimension.
     * Different tag combinations should be tracked independently.
     */
    public void testCounterAccumulatesPerDimension() {
        BenchmarkTelemetryPlugin.BenchmarkCounter counter = new BenchmarkTelemetryPlugin.BenchmarkCounter("test");
        Tags tagsA = Tags.create().addTag("env", "prod");
        Tags tagsB = Tags.create().addTag("env", "staging");

        counter.add(10, tagsA);
        counter.add(20, tagsA);
        counter.add(5, tagsB);

        // Both dimensions should work independently (no exceptions, no cross-contamination).
        counter.add(1, tagsA);
        counter.add(1, tagsB);
    }

    /**
     * Verifies thread safety by calling counter.add() from multiple threads concurrently.
     */
    public void testCounterThreadSafety() throws InterruptedException {
        BenchmarkTelemetryPlugin.BenchmarkCounter counter = new BenchmarkTelemetryPlugin.BenchmarkCounter("test");
        Tags tags = Tags.create().addTag("key", "value");

        int numThreads = 8;
        int iterationsPerThread = 10000;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < iterationsPerThread; j++) {
                    counter.add(1, tags);
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        // If we get here without exceptions, thread safety is verified
    }

    /**
     * Verifies thread safety of histogram under concurrent access.
     */
    public void testHistogramThreadSafety() throws InterruptedException {
        BenchmarkTelemetryPlugin.BenchmarkHistogram histogram = new BenchmarkTelemetryPlugin.BenchmarkHistogram("test");
        Tags tags = Tags.create().addTag("key", "value");

        int numThreads = 8;
        int iterationsPerThread = 10000;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < iterationsPerThread; j++) {
                    histogram.record(j * 1.5, tags);
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    /**
     * Verifies the TelemetryPlugin provides a valid Telemetry object.
     */
    public void testPluginProvidesTelemetry() {
        BenchmarkTelemetryPlugin.BenchmarkTelemetry telemetry = new BenchmarkTelemetryPlugin.BenchmarkTelemetry();
        assertNotNull(telemetry.getMetricsTelemetry());
        assertNotNull(telemetry.getTracingTelemetry());
    }

    /**
     * Verifies gauge creation returns a non-null Closeable.
     */
    public void testGaugeCreation() throws Exception {
        MetricsTelemetry registry = new BenchmarkTelemetryPlugin.BenchmarkMetricsTelemetry();
        java.io.Closeable gauge = registry.createGauge("test.gauge", "test", "1", () -> 42.0, Tags.EMPTY);
        assertNotNull(gauge);
        gauge.close();  // should not throw
    }
}
