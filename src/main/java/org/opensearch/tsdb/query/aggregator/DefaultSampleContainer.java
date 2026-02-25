/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Simple list-based implementation of {@link SampleContainer} for storing time series samples.
 *
 * <p>This implementation uses a standard {@link ArrayList} to store samples sequentially.
 * It enforces strict append-only semantics with no gaps allowed between samples.
 *
 * <h2>Key Characteristics:</h2>
 * <ul>
 *   <li><b>Storage:</b> Uses {@code ArrayList<Sample>} for simple sequential storage</li>
 *   <li><b>Append-Only:</b> Only allows appending samples at the next expected timestamp</li>
 *   <li><b>No Gaps:</b> Throws exception if a gap is detected during append</li>
 *   <li><b>Type Safety:</b> Validates that all samples match the container's declared type</li>
 * </ul>
 *
 * <h2>Performance Characteristics:</h2>
 * <ul>
 *   <li><b>Append:</b> O(1) amortized (ArrayList growth)</li>
 *   <li><b>Random access:</b> O(1) via {@link #getSampleFor(long)}</li>
 *   <li><b>Iteration:</b> O(n) via standard list iteration</li>
 *   <li><b>Memory overhead:</b> Standard ArrayList overhead plus Sample object references</li>
 * </ul>
 *
 * <h2>Limitations:</h2>
 * <ul>
 *   <li>Cannot handle gaps - every timestamp from min to max must be present</li>
 *   <li>Cannot handle out-of-order appends</li>
 *   <li>No sparse data support</li>
 *   <li>Higher memory overhead compared to primitive array implementations</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * <p>This class is <b>not thread-safe</b>. External synchronization is required for concurrent access.
 *
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * // Create container for float samples with 1-second intervals
 * DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
 *
 * // Append samples sequentially (no gaps allowed)
 * container.append(1000L, new FloatSample(1000L, 42.0));
 * container.append(2000L, new FloatSample(2000L, 43.0));
 * container.append(3000L, new FloatSample(3000L, 44.0));
 *
 * // Access samples by timestamp
 * Sample sample = container.getSampleFor(2000L);
 *
 * // Iterate over all samples
 * for (Sample s : container) {
 *     System.out.println(s.getValue());
 * }
 * }</pre>
 *
 * @see SampleContainer
 * @see DenseSampleContainer
 * @see Sample
 * @see SampleType
 */
public class DefaultSampleContainer implements SampleContainer {
    /**
     * Fixed time interval between consecutive samples in milliseconds.
     */
    private final long step;

    /**
     * Type of samples stored (determines storage layout).
     */
    private final SampleType sampleType;

    /**
     * Earliest timestamp in the container (inclusive).
     */
    private long minTimestamp;

    /**
     * Latest timestamp in the container (inclusive).
     */
    private long maxTimestamp;

    /**
     * Backing list that stores all samples sequentially.
     */
    private final List<Sample> store;

    /**
     * Creates a new default sample container for the specified sample type and time step.
     *
     * <p>The container is initialized with an empty list and will track the min/max timestamps
     * as samples are appended. The container enforces that all appended samples:
     * <ul>
     *   <li>Match the specified sample type</li>
     *   <li>Are appended at exactly {@code maxTimestamp + step}</li>
     *   <li>Have no gaps between consecutive samples</li>
     * </ul>
     *
     * @param sampleType the type of samples this container will store
     * @param step       the fixed time interval between consecutive samples in milliseconds
     */
    public DefaultSampleContainer(SampleType sampleType, long step) {
        this.step = step;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
        this.sampleType = sampleType;
        this.store = new ArrayList<>();
    }

    /**
     * Creates a new default sample container for the specified sample type and time step.
     *
     * <p>The container is initialized with an empty list and will track the min/max timestamps
     * as samples are appended. The container enforces that all appended samples:
     * <ul>
     *   <li>Match the specified sample type</li>
     *   <li>Are appended at exactly {@code maxTimestamp + step}</li>
     *   <li>Have no gaps between consecutive samples</li>
     * </ul>
     *
     * @param sampleType      the type of samples this container will store
     * @param step            the fixed time interval between consecutive samples in milliseconds
     * @param initialCapacity the initial capacity of the backing list to avoid early reallocations
     */
    public DefaultSampleContainer(SampleType sampleType, long step, int initialCapacity) {
        this.step = step;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
        this.sampleType = sampleType;
        this.store = new ArrayList<>(initialCapacity);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation calculates the array index from the timestamp and step interval,
     * then performs a direct list lookup.
     *
     * @throws IllegalArgumentException if the timestamp is out of bounds or no sample exists at that timestamp
     */
    @Override
    public Sample getSampleFor(long ts) {
        // Check bounds and if value is null
        if (ts < minTimestamp || ts > maxTimestamp || (ts - minTimestamp) % step != 0) {
            throw new IllegalArgumentException("Sample not found for the given timestamp: " + ts);
        }

        int index = (int) ((ts - minTimestamp) / step);
        return store.get(index);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation inserts the sample at the calculated index position in the list.
     *
     * @throws IllegalArgumentException if the sample type does not match the container's type or if ts greater than
     *                                  observed timestamps.
     */
    @Override
    public void updateSampleFor(long ts, Sample sample) {
        if (sampleType != sample.getSampleType()) {
            throw new IllegalArgumentException("Sample type does not match");
        }

        if (ts < minTimestamp || ts > maxTimestamp || (ts - minTimestamp) % step != 0) {
            throw new IllegalArgumentException("Sample not found for the given timestamp: " + ts);
        }

        int index = (int) ((ts - minTimestamp) / step);
        store.set(index, sample);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the underlying list's iterator, which yields all samples
     * in sequential order.
     *
     * @return an iterator over all samples in the container
     */
    @Override
    public Iterator<Sample> iterator() {
        return store.iterator();
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>Strict Append Semantics:</b> This implementation only allows appending samples
     * at the next expected timestamp ({@code maxTimestamp + step}). Any attempt to append
     * with a gap or out-of-order will throw an exception.
     *
     * <p><b>Performance:</b> Appends are O(1) amortized due to ArrayList's growth strategy.
     *
     * @throws IllegalArgumentException if the sample type does not match, or if the timestamp
     *                                  is not exactly {@code maxTimestamp + step}
     */
    @Override
    public void append(long ts, Sample sample) {
        if (sampleType != sample.getSampleType()) {
            throw new IllegalArgumentException("Sample type does not match");
        }

        if (maxTimestamp != Long.MIN_VALUE && ts != maxTimestamp + step) {
            throw new IllegalArgumentException("Sample timestamp jumps more than step:" + step);
        }

        minTimestamp = Math.min(minTimestamp, ts);
        maxTimestamp = Math.max(maxTimestamp, ts);
        store.add(sample);
    }

    /**
     * {@inheritDoc}
     *
     * @return the number of samples in the container
     */
    @Override
    public long size() {
        return store.size();
    }

    /**
     * {@inheritDoc}
     *
     * @return the earliest timestamp in the container, or {@link Long#MAX_VALUE} if empty
     */
    @Override
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * {@inheritDoc}
     *
     * @return the latest timestamp in the container, or {@link Long#MIN_VALUE} if empty
     */
    @Override
    public long getMaxTimestamp() {
        return maxTimestamp;
    }
}
