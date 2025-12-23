/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for normalizing time series to common normalized time buckets.
 *
 * <p>This class provides functionality to align multiple time series that have misaligned start
 * times and step sizes to common normalized time buckets. After normalization, all time series
 * will have the same bucket boundaries, which is essential for performing arithmetic operations
 * (like divide, subtract, ratio) between time series.</p>
 *
 * <h2>Normalization Process:</h2>
 * <ol>
 *   <li>Calculate LCM (Least Common Multiple) and MAX of all step sizes</li>
 *   <li>Select common step size based on strategy (LCM or MAX)</li>
 *   <li>Find the union of all time ranges (min start, max end)</li>
 *   <li>Adjust end time to align with common step boundaries</li>
 *   <li>Resample each series to the normalized time buckets using configurable consolidation strategy</li>
 * </ol>
 *
 * <h2>Consolidation Strategy:</h2>
 * <ul>
 *   <li><b>AVG, SUM, MAX, MIN, LAST:</b> Always uses the specified consolidation function for all series, regardless of type</li>
 *   <li><b>TYPE_AWARE:</b> Automatically uses SUM for counter-type series (type="counter" label),
 *       otherwise uses AVG</li>
 * </ul>
 *
 * <h2>Step Size Selection Strategy:</h2>
 * <ul>
 *   <li><b>LCM:</b> Uses the Least Common Multiple of all step sizes. Provides maximum precision by ensuring
 *       all original step boundaries align with the normalized grid.</li>
 *   <li><b>MAX:</b> Uses the maximum step size among all series. Provides coarser resolution but may be
 *       more efficient and preserve original data points better for some use cases.</li>
 * </ul>
 *
 * <h2>Example:</h2>
 * <pre>
 * Series A: 10-second steps [100, 120, 140] at [0s, 10s, 20s]
 * Series B: 15-second steps [50, 75] at [0s, 15s]
 *
 * After normalize with AVG and LCM strategy:
 * LCM(10s, 15s) = 30s
 * Series A: [120] at [0s] (avg of 100, 120, 140)
 * Series B: [62.5] at [0s] (avg of 50, 75)
 *
 * After normalize with AVG and MAX strategy:
 * MAX(10s, 15s) = 15s
 * Series A: [110] at [0s] (avg of 100, 120), [140] at [15s]
 * Series B: [50] at [0s], [75] at [15s]
 *
 * Now they can be operated on: divide(A, B) = 120 / 62.5 = 1.92 (LCM) or 110/50 = 2.2, 140/75 = 1.87 (MAX)
 * </pre>
 */
public final class TimeSeriesNormalizer {

    /**
     * Strategy for selecting the common step size when normalizing multiple time series.
     */
    public enum StepSizeStrategy {
        /**
         * Use the Least Common Multiple (LCM) of all step sizes.
         * This provides maximum precision by ensuring all original step boundaries align.
         */
        LCM,

        /**
         * Use the maximum step size among all series.
         * This provides coarser resolution but may be more efficient and preserve original data points better.
         */
        MAX
    }

    /**
     * Consolidation strategy for normalizing time series.
     * Can be either a specific consolidation function (AVG, SUM, MAX, MIN, LAST) or TYPE_AWARE.
     */
    public enum ConsolidationStrategy {
        /**
         * Use average (mean) of all values in each bucket.
         */
        AVG {
            @Override
            public ConsolidationFunction getFunction(TimeSeries series) {
                return ConsolidationFunction.AVG;
            }
        },

        /**
         * Use sum of all values in each bucket.
         */
        SUM {
            @Override
            public ConsolidationFunction getFunction(TimeSeries series) {
                return ConsolidationFunction.SUM;
            }
        },

        /**
         * Use maximum value in each bucket.
         */
        MAX {
            @Override
            public ConsolidationFunction getFunction(TimeSeries series) {
                return ConsolidationFunction.MAX;
            }
        },

        /**
         * Use minimum value in each bucket.
         */
        MIN {
            @Override
            public ConsolidationFunction getFunction(TimeSeries series) {
                return ConsolidationFunction.MIN;
            }
        },

        /**
         * Use last (most recent) value in each bucket.
         */
        LAST {
            @Override
            public ConsolidationFunction getFunction(TimeSeries series) {
                return ConsolidationFunction.LAST;
            }
        },

        /**
         * Use type-aware consolidation: automatically use SUM for counter-type series
         * (series with type="counter" label), and use AVG for others.
         * This is useful when mixing counter and gauge metrics in the same normalization operation.
         */
        TYPE_AWARE {
            /**
             * <ul>
             *   <li>If the series has a "type" label with value "counter" or "counts" (case-insensitive), uses SUM</li>
             *   <li>Otherwise uses AVG</li>
             * </ul>
             *
             * @param series The time series to check
             * @return SUM if series is counter/counts type, otherwise the function from the strategy
             */
            @Override
            public ConsolidationFunction getFunction(TimeSeries series) {
                if (series.getLabels() != null && series.getLabels().has("type")) {
                    String typeValue = series.getLabels().get("type");
                    // Check for both "counter" and "counts" (case-insensitive)
                    if ("counter".equalsIgnoreCase(typeValue) || "counts".equalsIgnoreCase(typeValue)) {
                        return ConsolidationFunction.SUM;
                    }
                }
                // For TYPE_AWARE, use AVG for non-counter types
                return ConsolidationFunction.AVG;
            }
        };

        /**
         * Get the consolidation function for this strategy.
         * For TYPE_AWARE, this returns the default function used for non-counter types.
         *
         * @return The consolidation function
         */
        public abstract ConsolidationFunction getFunction(TimeSeries series);
    }

    private TimeSeriesNormalizer() {
        // Utility class - no instantiation
    }

    /**
     * Normalizes multiple time series to common normalized time buckets with aligned start time, step size, and end time.
     *
     * <p>This method aligns time series with potentially different step sizes and time ranges by:
     * <ol>
     *   <li>Calculating the LCM (Least Common Multiple) and MAX of all step sizes</li>
     *   <li>Selecting the common step size based on the specified strategy (LCM or MAX)</li>
     *   <li>Finding the union of all time ranges (min start, max end)</li>
     *   <li>Adjusting the end time to be divisible by the common step size</li>
     *   <li>Resampling each series to the normalized time buckets using type-aware consolidation</li>
     * </ol>
     *
     * <p>The consolidation strategy determines how values are aggregated when resampling:
     * <ul>
     *   <li>AVG, SUM, MAX, MIN, LAST: Always uses the specified consolidation function for all series</li>
     *   <li>TYPE_AWARE: Uses SUM for counter-type series (series with type="counter" label),
     *       otherwise uses AVG</li>
     * </ul>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * List<TimeSeries> series = Arrays.asList(seriesA, seriesB);
     * List<TimeSeries> normalized = TimeSeriesNormalizer.normalize(
     *     series,
     *     StepSizeStrategy.LCM,
     *     ConsolidationStrategy.TYPE_AWARE
     * );
     *
     * // Or use a fixed function:
     * List<TimeSeries> normalized2 = TimeSeriesNormalizer.normalize(
     *     series,
     *     StepSizeStrategy.LCM,
     *     ConsolidationStrategy.AVG
     * );
     *
     * // Now all series have the same step size and time range
     * TimeSeries normalizedA = normalized.get(0);
     * TimeSeries normalizedB = normalized.get(1);
     * }</pre>
     *
     * @param seriesList List of time series to normalize (empty list returns empty list)
     * @param stepSizeStrategy The strategy for selecting the common step size (LCM or MAX)
     * @param consolidationStrategy The consolidation strategy (AVG, SUM, MAX, MIN, LAST, or TYPE_AWARE)
     * @return List of normalized time series with common bucket alignment
     * @throws IllegalArgumentException if seriesList is null or contains null elements
     */
    public static List<TimeSeries> normalize(
        List<TimeSeries> seriesList,
        StepSizeStrategy stepSizeStrategy,
        ConsolidationStrategy consolidationStrategy
    ) {
        if (seriesList == null) {
            throw new IllegalArgumentException("Cannot normalize null series list");
        }

        // Empty list - return empty list
        if (seriesList.isEmpty()) {
            return List.of();
        }

        // Single series - no normalization needed
        if (seriesList.size() == 1) {
            return seriesList;
        }

        // Calculate LCM/MAX of all step sizes, and find time range union
        long commonStep = seriesList.get(0).getStep();
        long minStart = seriesList.get(0).getMinTimestamp();
        long maxEnd = seriesList.get(0).getMaxTimestamp();

        for (int i = 1; i < seriesList.size(); i++) {
            TimeSeries series = seriesList.get(i);
            long step = series.getStep();
            // Select common step size based on strategy
            if (StepSizeStrategy.MAX == stepSizeStrategy) {
                if (step > commonStep) {
                    commonStep = step;
                }
            } else {
                commonStep = lcm(commonStep, step);
            }

            if (series.getMinTimestamp() < minStart) {
                minStart = series.getMinTimestamp();
            }
            if (series.getMaxTimestamp() > maxEnd) {
                maxEnd = series.getMaxTimestamp();
            }
        }

        // Adjust end time to be divisible by common step
        long range = maxEnd - minStart;
        long remainder = range % commonStep;
        if (remainder != 0) {
            maxEnd = maxEnd - remainder;
        }

        // Resample each series to the normalized time buckets
        List<TimeSeries> normalizedSeries = new ArrayList<>(seriesList.size());
        for (TimeSeries series : seriesList) {
            // Check if already aligned
            if (series.getMinTimestamp() == minStart && series.getMaxTimestamp() == maxEnd && series.getStep() == commonStep) {
                normalizedSeries.add(series);
                continue;
            }

            // Determine consolidation function based on strategy
            ConsolidationFunction seriesConsolidationFunc = consolidationStrategy.getFunction(series);

            // Resample to normalized time buckets
            TimeSeries resampled = resampleSeries(series, minStart, maxEnd, commonStep, seriesConsolidationFunc);
            normalizedSeries.add(resampled);
        }

        return normalizedSeries;
    }

    /**
     * Normalizes multiple time series using LCM step size strategy and TYPE_AWARE consolidation strategy.
     *
     * @param seriesList List of time series to normalize
     * @param consolidationStrategy The consolidation strategy (AVG, SUM, MAX, MIN, LAST, or TYPE_AWARE)
     * @return List of normalized time series with common bucket alignment
     */
    public static List<TimeSeries> normalize(List<TimeSeries> seriesList, ConsolidationStrategy consolidationStrategy) {
        return normalize(seriesList, StepSizeStrategy.LCM, consolidationStrategy);
    }

    /**
     * Convenience method to normalize with LCM step size strategy and TYPE_AWARE consolidation strategy.
     *
     * @param seriesList List of time series to normalize
     * @return List of normalized time series with common bucket alignment
     */
    public static List<TimeSeries> normalize(List<TimeSeries> seriesList) {
        return normalize(seriesList, StepSizeStrategy.LCM, ConsolidationStrategy.TYPE_AWARE);
    }

    /**
     * Resamples a time series to new time buckets with specified start, end, and step.
     * Uses the specified consolidation function to aggregate values that fall into each bucket.
     *
     * @param series The time series to resample
     * @param newStart The start timestamp of the new time buckets
     * @param newEnd The end timestamp of the new time buckets
     * @param newStep The step size of the new time buckets
     * @param consolidationFunc The consolidation function to use
     * @return A new time series resampled to the specified time buckets
     */
    private static TimeSeries resampleSeries(
        TimeSeries series,
        long newStart,
        long newEnd,
        long newStep,
        ConsolidationFunction consolidationFunc
    ) {
        int numSteps = (int) ((newEnd - newStart) / newStep) + 1;
        List<Sample> newSamples = new ArrayList<>(numSteps);
        // Reusable buffer for current bucket values (timestamps are sorted, so we process buckets sequentially)
        // Preallocate based on maximum possible samples per bucket: ceil(newStep / originalStep) + 1
        long originalStep = series.getStep();
        int maxSamplesPerBucket = (int) ((newStep + originalStep - 1) / originalStep) + 1;
        List<Double> currentBucketValues = new ArrayList<>(maxSamplesPerBucket);
        int currentBucketIndex = -1;

        // O(m) pass: iterate through samples once, consolidating buckets as we move forward
        for (Sample sample : series.getSamples()) {
            long sampleTimestamp = sample.getTimestamp();
            // Calculate which bucket this sample belongs to
            int bucketIndex;
            bucketIndex = (int) ((sampleTimestamp - newStart) / newStep);

            // If we've moved to a new bucket, consolidate the previous bucket
            if (currentBucketIndex >= 0 && bucketIndex > currentBucketIndex) {
                // Consolidate the previous bucket before moving to the new one
                if (!currentBucketValues.isEmpty()) {
                    long timestamp = newStart + (long) currentBucketIndex * newStep;
                    double consolidatedValue = consolidationFunc.apply(currentBucketValues);
                    if (!Double.isNaN(consolidatedValue)) {
                        newSamples.add(new FloatSample(timestamp, consolidatedValue));
                    }
                    currentBucketValues.clear();
                }
                // Empty buckets between currentBucketIndex and bucketIndex are skipped
                // (no samples created, framework handles missing samples)
            }

            // Add sample value to current bucket
            double value = sample.getValue();
            if (!Double.isNaN(value)) {
                currentBucketValues.add(value);
            }
            currentBucketIndex = bucketIndex;
        }

        // Consolidate the last bucket if it has values
        if (currentBucketIndex >= 0 && !currentBucketValues.isEmpty()) {
            long timestamp = newStart + (long) currentBucketIndex * newStep;
            double consolidatedValue = consolidationFunc.apply(currentBucketValues);
            if (!Double.isNaN(consolidatedValue)) {
                newSamples.add(new FloatSample(timestamp, consolidatedValue));
            }
        }

        return new TimeSeries(newSamples, series.getLabels(), newStart, newEnd, newStep, series.getAlias());
    }

    /**
     * Calculates the Least Common Multiple of two positive integers.
     * LCM is used to find the finest resolution that evenly divides all input step sizes.
     *
     * <p>Note: While Apache Commons Math provides ArithmeticUtils.lcm(), we implement it here to avoid adding a dependency
     * @param a First positive integer
     * @param b Second positive integer
     * @return The LCM of a and b
     * @throws IllegalArgumentException if either value is zero or if the calculation would overflow
     */
    private static long lcm(long a, long b) {
        if (a <= 0 || b <= 0) {
            throw new IllegalArgumentException("LCM requires positive values, got: a=" + a + ", b=" + b);
        }
        long gcdValue = gcd(a, b);
        // Divide first to reduce the value before multiplication, avoiding overflow
        // This matches the approach used in Apache Commons Math ArithmeticUtils.lcm()
        long aDivGcd = a / gcdValue;
        // Use Math.multiplyExact to detect overflow automatically
        try {
            return Math.multiplyExact(aDivGcd, b);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("LCM calculation would overflow: a=" + a + ", b=" + b + ". Step sizes are too large.", e);
        }
    }

    /**
     * Calculates the Greatest Common Divisor using Euclidean algorithm.
     *
     * <p>This is the standard iterative Euclidean algorithm for computing GCD.
     * While Apache Commons Math provides ArithmeticUtils.gcd(), we implement it
     * here to avoid adding a dependency for this single mathematical operation.</p>
     *
     * @param a First positive integer
     * @param b Second positive integer
     * @return The GCD of a and b
     * @throws IllegalArgumentException if either value is non-positive
     */
    private static long gcd(long a, long b) {
        if (a <= 0 || b <= 0) {
            throw new IllegalArgumentException("GCD requires positive values, got: a=" + a + ", b=" + b);
        }
        // Since both values are positive, no need for Math.abs
        while (b != 0) {
            long temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }
}
