/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark.metrics;

import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Histogram that simulates realistic OTel metrics overhead for benchmarking.
 *
 * <p>On each {@code record()} call, this histogram performs the same categories of work
 * as a real OTel SDK exponential histogram:</p>
 * <ol>
 *   <li>Converts {@link Tags} to a dimension key (same as {@link BenchmarkCounter})</li>
 *   <li>Computes the exponential bucket index using {@link Math#getExponent} and
 *       {@link Math#scalb} (same JDK intrinsics as OTel's Base2ExponentialHistogramAggregation)</li>
 *   <li>Atomically increments the bucket count and tracks sum</li>
 * </ol>
 */
public class BenchmarkHistogram implements Histogram {
    private final String name;
    private final ConcurrentHashMap<String, BucketStorage> storage = new ConcurrentHashMap<>();

    // OTel default: scale=20, ~160 buckets
    private static final int NUM_BUCKETS = 160;
    private static final int SCALE = 20;

    public BenchmarkHistogram(String name) {
        this.name = name;
    }

    @Override
    public void record(double value) {
        BucketStorage buckets = storage.computeIfAbsent("__no_tags__", k -> new BucketStorage());
        int bucketIndex = computeBucketIndex(value);
        buckets.record(value, bucketIndex);
    }

    @Override
    public void record(double value, Tags tags) {
        String attributeKey = convertTagsToKey(tags);
        BucketStorage buckets = storage.computeIfAbsent(attributeKey, k -> new BucketStorage());
        int bucketIndex = computeBucketIndex(value);
        buckets.record(value, bucketIndex);
    }

    /**
     * Simulates OTel Base2ExponentialHistogramAggregation bucket computation
     * using the same JDK intrinsics (Math.getExponent, Math.scalb).
     */
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

    /**
     * Per-dimension bucket storage simulating OTel's exponential histogram.
     */
    private static class BucketStorage {
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
            sumBits.getAndUpdate(prev -> {
                double prevSum = Double.longBitsToDouble(prev);
                return Double.doubleToLongBits(prevSum + value);
            });
        }
    }
}
