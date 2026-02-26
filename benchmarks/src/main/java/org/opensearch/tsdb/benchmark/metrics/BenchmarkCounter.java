/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counter that simulates realistic OTel metrics overhead for benchmarking.
 *
 * <p>On each {@code add()} call, this counter performs the same categories of work
 * as a real OTel SDK counter:</p>
 * <ol>
 *   <li>Converts {@link Tags} to a dimension key by iterating the tag map into a new
 *       {@link HashMap} (simulates {@code OTelAttributesConverter.convert()})</li>
 *   <li>Looks up the dimension in a {@link ConcurrentHashMap} (simulates
 *       {@code SynchronousMetricStorage.record()})</li>
 *   <li>Atomically increments the counter value (simulates {@code SumAggregator.addAndGet()})</li>
 * </ol>
 */
public class BenchmarkCounter implements Counter {
    private final String name;
    private final ConcurrentHashMap<String, AtomicLong> storage = new ConcurrentHashMap<>();

    public BenchmarkCounter(String name) {
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

    /**
     * Simulates the overhead of OTel's tag-to-attribute conversion:
     * creates a new HashMap, iterates all tag entries with type checking,
     * and builds a composite string key for dimension routing.
     */
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
}
