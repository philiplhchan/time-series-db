/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.TaggedMeasurement;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * MetricsRegistry that simulates realistic OTel metrics overhead for benchmarking.
 *
 * <p>Unlike {@code NoopMetricsRegistry} (which discards all metrics silently), this registry
 * creates counters and histograms that perform the same categories of work as real OTel
 * instrumentation: HashMap creation for tag dimension routing, ConcurrentHashMap lookups
 * for metric storage, atomic operations for counter increments, and exponential histogram
 * bucket computation.</p>
 *
 * <p><b>Important:</b> The class name intentionally does NOT contain "noop" so that
 * {@code TSDBMetrics.isNoopRegistry()} returns false, allowing metrics initialization
 * to proceed.</p>
 *
 * <h2>Usage in JMH benchmarks:</h2>
 * <pre>{@code
 * TSDBMetrics.cleanup();
 * TSDBMetrics.initialize(new BenchmarkMetricsRegistry());
 * }</pre>
 *
 * <h2>Usage in live cluster (OSB):</h2>
 * <p>Use the {@code benchmark-telemetry} OpenSearch plugin which provides this
 * registry via the {@code TelemetryPlugin} SPI.</p>
 */
public class BenchmarkMetricsRegistry implements MetricsRegistry {

    @Override
    public Counter createCounter(String name, String description, String unit) {
        return new BenchmarkCounter(name);
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        return new BenchmarkCounter(name);
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        return new BenchmarkHistogram(name);
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        // Gauges are pull-based (polled periodically, not on hot path).
        return () -> {};
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        return () -> {};
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String toString() {
        return "BenchmarkMetricsRegistry";
    }
}
