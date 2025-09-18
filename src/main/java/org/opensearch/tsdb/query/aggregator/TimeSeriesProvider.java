/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import java.util.List;

/**
 * Interface for aggregations that can provide TimeSeries data.
 *
 * <p>This interface allows pipeline stages to work with different aggregation types
 * by providing a common contract for accessing time series data. It enables the
 * creation of flexible and reusable pipeline stages that can operate on any
 * aggregation that implements this interface.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Unified Access:</strong> Provides a common interface for accessing time series data</li>
 *   <li><strong>Pipeline Compatibility:</strong> Enables pipeline stages to work with different aggregation types</li>
 *   <li><strong>Reduction Support:</strong> Supports creating reduced aggregations with new time series data</li>
 * </ul>
 *
 * <h3>Usage Examples:</h3>
 * <pre>{@code
 * // Check if an aggregation provides time series data
 * if (aggregation instanceof TimeSeriesProvider provider) {
 *     List<TimeSeries> timeSeries = provider.getTimeSeries();
 *
 *     // Process the time series data
 *     for (TimeSeries series : timeSeries) {
 *         // Apply transformations or aggregations
 *     }
 * }
 *
 * // Create a reduced aggregation
 * List<TimeSeries> reducedTimeSeries = processTimeSeries(originalTimeSeries);
 * TimeSeriesProvider reducedProvider = originalProvider.createReduced(reducedTimeSeries);
 * }</pre>
 *
 * <h3>Implementation Guidelines:</h3>
 * <ul>
 *   <li>Implementations should ensure that {@code getTimeSeries()} returns a non-null list</li>
 *   <li>The {@code createReduced()} method should create a new instance with the provided time series</li>
 *   <li>Time series data should be immutable or properly synchronized for thread safety</li>
 * </ul>
 *
 */
public interface TimeSeriesProvider {
    /**
     * Get the time series from this aggregation.
     *
     * <p>This method returns all time series data contained within this aggregation.
     * The returned list should not be null, but may be empty if no time series data
     * is available.</p>
     *
     * @return List of time series data, never null but may be empty
     */
    List<TimeSeries> getTimeSeries();

    /**
     * Create a new reduced aggregation with the given time series.
     *
     * <p>This method creates a new instance of the aggregation with the provided
     * time series data. This is typically used during the reduce phase of aggregation
     * processing to combine results from multiple shards or segments.</p>
     *
     * <p>The implementation should preserve all metadata and configuration from the
     * original aggregation while replacing the time series data with the provided list.</p>
     *
     * @param timeSeries The reduced time series data to use in the new aggregation
     * @return A new TimeSeriesProvider instance with the provided time series data
     * @throws IllegalArgumentException if the timeSeries parameter is null
     */
    TimeSeriesProvider createReduced(List<TimeSeries> timeSeries);
}
