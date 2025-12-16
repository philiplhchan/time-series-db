/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

/**
 * Defines the normalization strategy for binary projection operations.
 *
 * <p>Normalization aligns time series to a common grid with the same step size and time range,
 * making them compatible for element-wise operations.
 */
public enum NormalizationStrategy {
    /**
     * No normalization applied. Time series are processed as-is.
     * Suitable for operations that can handle misaligned timestamps.
     */
    NONE,

    /**
     * Pairwise normalization. Each pair of left/right time series is normalized together
     * before processing. The normalized pair shares the same step size and time range.
     * Each pair gets its own optimal time grid.
     * Suitable for operations like divide where each left series is divided by its matching right series.
     * Example: divide([A, B], [Total]) → normalize(A, Total), then normalize(B, Total) separately.
     */
    PAIRWISE,

    /**
     * Batch normalization. After matching left and right series, all matching series are normalized
     * together as a single batch, then processed pairwise. All series share the same time grid.
     * Suitable for operations like asPercent where consistency across all series is needed.
     * Example: asPercent([A, B], [Total]) → normalize([A, B, Total]) together, then process.
     */
    BATCH
}
