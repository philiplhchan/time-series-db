/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import java.util.List;

/**
 * Utility class for percentile calculations.
 *
 * <p>This class provides centralized percentile calculation logic to prevent drift
 * between different implementations. It matches the M3 percentileOfSeries behavior.</p>
 */
public final class PercentileUtils {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private PercentileUtils() {
        // Utility class
    }

    /**
     * Calculate percentile from a sorted list of values.
     * This implementation matches the M3 percentileOfSeries behavior.
     *
     * @param sortedValues Sorted list of values (must be pre-sorted)
     * @param percentile Percentile to calculate (0-100)
     * @param interpolate Whether to interpolate between values
     * @return Calculated percentile value, or Double.NaN if input is empty
     * @throws IllegalArgumentException if percentile is not in valid range [0, 100]
     */
    public static double calculatePercentile(List<Double> sortedValues, double percentile, boolean interpolate) {
        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile must be between 0 and 100 (inclusive), got: " + percentile);
        }

        if (sortedValues == null || sortedValues.isEmpty()) {
            return Double.NaN;
        }

        // Calculate fractional rank: (percentile / 100) * length
        double fractionalRank = (percentile / 100.0) * sortedValues.size();

        // Use ceiling to get the rank (1-based)
        double rank = Math.ceil(fractionalRank);

        // Cast to integer
        int rankAsInt = (int) rank;

        // Edge case: if rank <= 1, return first element
        if (rankAsInt <= 1) {
            return sortedValues.get(0);
        }

        // Convert 1-based rank to 0-based index and get the value
        double percentileResult = sortedValues.get(rankAsInt - 1);

        if (interpolate && rankAsInt >= 2) {
            // Get the previous value for interpolation
            double prevValue = sortedValues.get(rankAsInt - 2);

            // Calculate the fraction for interpolation
            double fraction = fractionalRank - (rank - 1);

            // Linear interpolation
            percentileResult = prevValue + (fraction * (percentileResult - prevValue));
        }

        return percentileResult;
    }

    /**
     * Calculate percentile from a sorted list of values without interpolation.
     * This is a convenience method that calls {@link #calculatePercentile(List, double, boolean)}
     * with interpolate=false.
     *
     * @param sortedValues Sorted list of values (must be pre-sorted)
     * @param percentile Percentile to calculate (0-100)
     * @return Calculated percentile value, or Double.NaN if input is empty
     * @throws IllegalArgumentException if percentile is not in valid range [0, 100]
     */
    public static double calculatePercentile(List<Double> sortedValues, double percentile) {
        return calculatePercentile(sortedValues, percentile, false);
    }

    /**
     * Calculate the 50th percentile (median) from a sorted list of values.
     * This is a convenience method that calls {@link #calculatePercentile(List, double, boolean)}
     * with percentile=50 and interpolate=false.
     *
     * @param sortedValues Sorted list of values (must be pre-sorted)
     * @return The 50th percentile (median) value, or Double.NaN if input is empty
     */
    public static double calculateMedian(List<Double> sortedValues) {
        return calculatePercentile(sortedValues, 50.0, false);
    }
}
