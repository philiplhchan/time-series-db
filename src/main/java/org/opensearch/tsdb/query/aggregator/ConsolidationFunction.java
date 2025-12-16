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
 * Consolidation functions for time series normalization and resampling.
 *
 * <p>These functions determine how multiple data points within a time bucket
 * are aggregated into a single value when resampling time series to a different
 * resolution or aligning multiple series to a common grid.</p>
 *
 * <p>This enum corresponds to M3's ts.ConsolidationFunc and is specifically
 * for time series consolidation operations, separate from query-level aggregation types.</p>
 */
public enum ConsolidationFunction {
    /**
     * Average (mean) of all values in the bucket.
     * This is the default consolidation function for most operations.
     *
     * <p>Example: [10, 20, 30] → 20.0</p>
     */
    AVG {
        @Override
        public double apply(List<Double> values) {
            double sum = 0.0;
            for (double v : values) {
                sum += v;
            }
            return sum / values.size();
        }
    },

    /**
     * Sum of all values in the bucket.
     * Useful for counter-type metrics where totals need to be preserved.
     *
     * <p>Example: [10, 20, 30] → 60.0</p>
     */
    SUM {
        @Override
        public double apply(List<Double> values) {
            double sum = 0.0;
            for (double v : values) {
                sum += v;
            }
            return sum;
        }
    },

    /**
     * Maximum value in the bucket.
     * Useful for peak detection and upper bound tracking.
     *
     * <p>Example: [10, 20, 30] → 30.0</p>
     */
    MAX {
        @Override
        public double apply(List<Double> values) {
            double max = Double.NEGATIVE_INFINITY;
            for (double v : values) {
                if (v > max) {
                    max = v;
                }
            }
            return max;
        }
    },

    /**
     * Minimum value in the bucket.
     * Useful for lower bound tracking and minimum value detection.
     *
     * <p>Example: [10, 20, 30] → 10.0</p>
     */
    MIN {
        @Override
        public double apply(List<Double> values) {
            double min = Double.POSITIVE_INFINITY;
            for (double v : values) {
                if (v < min) {
                    min = v;
                }
            }
            return min;
        }
    },

    /**
     * Last (most recent) value in the bucket.
     * Useful for gauges where the latest value is most representative.
     *
     * <p>Example: [10, 20, 30] → 30.0</p>
     */
    LAST {
        @Override
        public double apply(List<Double> values) {
            return values.getLast();
        }
    };

    /**
     * Apply the consolidation function to a list of values.
     *
     * @param values List of values to consolidate (must not be empty)
     * @return The consolidated value
     * @throws IllegalArgumentException if values is empty
     */
    public abstract double apply(List<Double> values);

    /**
     * Get the default consolidation function (AVG).
     *
     * @return The default consolidation function
     */
    public static ConsolidationFunction getDefault() {
        return AVG;
    }
}
