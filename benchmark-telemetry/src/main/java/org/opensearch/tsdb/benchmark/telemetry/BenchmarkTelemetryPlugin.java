/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark.telemetry;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.metrics.TaggedMeasurement;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.TracingContextPropagator;
import org.opensearch.telemetry.tracing.TracingTelemetry;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * TelemetryPlugin that provides a metrics registry simulating realistic OTel overhead.
 *
 * <p>Install this plugin alongside the TSDB plugin to capture metrics-related performance
 * degradation in local runs, OSB benchmarks, and CPU profiles without requiring external
 * infrastructure (no OTel collector, no Prometheus).</p>
 *
 * <p>Each {@code counter.add()} and {@code histogram.record()} call performs the same
 * categories of work as a real OTel SDK: HashMap allocation for tag conversion,
 * ConcurrentHashMap lookup for metric storage, and atomic operations for recording.</p>
 *
 * <h2>Required settings:</h2>
 * <pre>
 * opensearch.experimental.feature.telemetry.enabled=true
 * telemetry.feature.metrics.enabled=true
 * </pre>
 *
 * <p><b>Important:</b> This plugin is mutually exclusive with {@code telemetry-otel}.
 * OpenSearch allows only one {@code TelemetryPlugin} to register a non-empty
 * {@code Telemetry} instance; installing both will cause the node to fail with
 * {@code "Cannot register more than one telemetry"}.</p>
 *
 * <h2>Usage with {@code ./gradlew run}:</h2>
 * <pre>
 * ./gradlew run -PbenchmarkTelemetry
 * </pre>
 */
public class BenchmarkTelemetryPlugin extends Plugin implements TelemetryPlugin {

    public BenchmarkTelemetryPlugin(Settings settings) {
        // Required by OpenSearch plugin framework
    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings telemetrySettings) {
        return Optional.of(new BenchmarkTelemetry());
    }

    @Override
    public String getName() {
        return "benchmark-telemetry";
    }

    // ---- Telemetry wiring ----

    static class BenchmarkTelemetry implements Telemetry {
        @Override
        public TracingTelemetry getTracingTelemetry() {
            return new NoopTracingTelemetry();
        }

        @Override
        public MetricsTelemetry getMetricsTelemetry() {
            return new BenchmarkMetricsTelemetry();
        }
    }

    // ---- MetricsTelemetry (extends MetricsRegistry) ----

    static class BenchmarkMetricsTelemetry implements MetricsTelemetry {
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
            return () -> {};
        }

        @Override
        public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
            return () -> {};
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return "BenchmarkMetricsTelemetry";
        }
    }

    // ---- Counter with realistic OTel overhead ----

    static class BenchmarkCounter implements Counter {
        private final String name;
        private final ConcurrentHashMap<String, AtomicLong> storage = new ConcurrentHashMap<>();

        BenchmarkCounter(String name) {
            this.name = name;
        }

        @Override
        public void add(double value) {
            storage.computeIfAbsent("__no_tags__", k -> new AtomicLong()).addAndGet((long) value);
        }

        @Override
        public void add(double value, Tags tags) {
            String key = convertTagsToKey(tags);
            storage.computeIfAbsent(key, k -> new AtomicLong()).addAndGet((long) value);
        }

        private String convertTagsToKey(Tags tags) {
            if (tags == null || tags == Tags.EMPTY) {
                return "__no_tags__";
            }
            // Simulate OTel AttributesBuilder: new HashMap allocation + iteration + type checking
            Map<String, Object> converted = new HashMap<>();
            tags.getTagsMap().forEach((k, v) -> {
                if (v instanceof String || v instanceof Long || v instanceof Double || v instanceof Boolean) {
                    converted.put(k, v);
                }
            });
            // Simulate Attributes.hashCode computation for ConcurrentHashMap key
            StringBuilder sb = new StringBuilder(name);
            converted.forEach((k, v) -> sb.append(k).append('=').append(v).append(','));
            return sb.toString();
        }
    }

    // ---- Histogram with realistic OTel overhead ----

    static class BenchmarkHistogram implements Histogram {
        private final String name;
        private final ConcurrentHashMap<String, BucketStorage> storage = new ConcurrentHashMap<>();
        private static final int NUM_BUCKETS = 160;
        private static final int SCALE = 20;

        BenchmarkHistogram(String name) {
            this.name = name;
        }

        @Override
        public void record(double value) {
            BucketStorage buckets = storage.computeIfAbsent("__no_tags__", k -> new BucketStorage());
            buckets.record(value, computeBucketIndex(value));
        }

        @Override
        public void record(double value, Tags tags) {
            String key = convertTagsToKey(tags);
            BucketStorage buckets = storage.computeIfAbsent(key, k -> new BucketStorage());
            buckets.record(value, computeBucketIndex(value));
        }

        private static int computeBucketIndex(double value) {
            if (value <= 0) return 0;
            int exponent = Math.getExponent(value);
            double scaled = Math.scalb(value, -exponent);
            int subBucket = (int) ((scaled - 1.0) * (1 << Math.min(SCALE, 10)));
            int index = exponent * (1 << Math.min(SCALE, 4)) + subBucket;
            return Math.floorMod(index, NUM_BUCKETS);
        }

        private String convertTagsToKey(Tags tags) {
            if (tags == null || tags == Tags.EMPTY) {
                return "__no_tags__";
            }
            Map<String, Object> converted = new HashMap<>();
            tags.getTagsMap().forEach((k, v) -> {
                if (v instanceof String || v instanceof Long || v instanceof Double || v instanceof Boolean) {
                    converted.put(k, v);
                }
            });
            StringBuilder sb = new StringBuilder(name);
            converted.forEach((k, v) -> sb.append(k).append('=').append(v).append(','));
            return sb.toString();
        }

        static class BucketStorage {
            private final LongAdder[] buckets;
            private final LongAdder count = new LongAdder();
            private final AtomicLong sumBits = new AtomicLong(Double.doubleToLongBits(0.0));

            BucketStorage() {
                buckets = new LongAdder[NUM_BUCKETS];
                for (int i = 0; i < NUM_BUCKETS; i++) {
                    buckets[i] = new LongAdder();
                }
            }

            void record(double value, int bucketIndex) {
                buckets[bucketIndex].increment();
                count.increment();
                sumBits.getAndUpdate(prev -> Double.doubleToLongBits(Double.longBitsToDouble(prev) + value));
            }
        }
    }

    // ---- Noop tracing (only metrics overhead is in scope) ----

    static class NoopTracingTelemetry implements TracingTelemetry {
        @Override
        public Span createSpan(SpanCreationContext ctx, Span parent) {
            return null;
        }

        @Override
        public TracingContextPropagator getContextPropagator() {
            return null;
        }

        @Override
        public void close() {}
    }
}
