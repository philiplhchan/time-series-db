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
 *   <li>Calculate LCM (Least Common Multiple) of all step sizes for maximum precision</li>
 *   <li>Find the union of all time ranges (min start, max end)</li>
 *   <li>Adjust end time to align with LCM step boundaries</li>
 *   <li>Resample each series to the normalized time buckets using consolidation</li>
 * </ol>
 *
 * <h2>Example:</h2>
 * <pre>
 * Series A: 10-second steps [100, 120, 140] at [0s, 10s, 20s]
 * Series B: 15-second steps [50, 75] at [0s, 15s]
 *
 * After normalize with AVG:
 * LCM(10s, 15s) = 30s
 * Series A: [120] at [0s] (avg of 100, 120, 140)
 * Series B: [62.5] at [0s] (avg of 50, 75)
 *
 * Now they can be operated on: divide(A, B) = 120 / 62.5 = 1.92
 * </pre>
 */
public final class TimeSeriesNormalizer {

    private TimeSeriesNormalizer() {
        // Utility class - no instantiation
    }

    /**
     * Normalizes multiple time series to common normalized time buckets with aligned start time, step size, and end time.
     *
     * <p>This method aligns time series with potentially different step sizes and time ranges by:
     * <ol>
     *   <li>Calculating the LCM (Least Common Multiple) of all step sizes for maximum precision</li>
     *   <li>Finding the union of all time ranges (min start, max end)</li>
     *   <li>Adjusting the end time to be divisible by the LCM step size</li>
     *   <li>Resampling each series to the normalized time buckets using the specified consolidation function</li>
     * </ol>
     *
     * <p>The consolidation function determines how values are aggregated when resampling:
     * <ul>
     *   <li>AVG (default): Average of values in each bucket</li>
     *   <li>SUM: Sum of values in each bucket</li>
     *   <li>MAX: Maximum value in each bucket</li>
     *   <li>MIN: Minimum value in each bucket</li>
     *   <li>LAST: Last (most recent) value in each bucket</li>
     * </ul>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * List<TimeSeries> series = Arrays.asList(seriesA, seriesB);
     * List<TimeSeries> normalized = TimeSeriesNormalizer.normalize(series, ConsolidationFunction.AVG);
     *
     * // Now all series have the same step size and time range
     * TimeSeries normalizedA = normalized.get(0);
     * TimeSeries normalizedB = normalized.get(1);
     * }</pre>
     *
     * @param seriesList List of time series to normalize (empty list returns empty list)
     * @param consolidationFunc The consolidation function to use for resampling (default: AVG)
     * @return List of normalized time series with common bucket alignment
     * @throws IllegalArgumentException if seriesList is null or contains null elements
     */
    public static List<TimeSeries> normalize(List<TimeSeries> seriesList, ConsolidationFunction consolidationFunc) {
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

        // Calculate LCM of all step sizes and find time range union
        long lcmStep = seriesList.get(0).getStep();
        long minStart = seriesList.get(0).getMinTimestamp();
        long maxEnd = seriesList.get(0).getMaxTimestamp();

        for (int i = 1; i < seriesList.size(); i++) {
            TimeSeries series = seriesList.get(i);
            lcmStep = lcm(lcmStep, series.getStep());

            if (series.getMinTimestamp() < minStart) {
                minStart = series.getMinTimestamp();
            }
            if (series.getMaxTimestamp() > maxEnd) {
                maxEnd = series.getMaxTimestamp();
            }
        }

        // Adjust end time to be divisible by LCM step
        long range = maxEnd - minStart;
        long remainder = range % lcmStep;
        if (remainder != 0) {
            maxEnd = maxEnd - remainder;
        }

        // Resample each series to the normalized time buckets
        List<TimeSeries> normalizedSeries = new ArrayList<>(seriesList.size());
        for (TimeSeries series : seriesList) {
            // Check if already aligned
            if (series.getMinTimestamp() == minStart && series.getMaxTimestamp() == maxEnd && series.getStep() == lcmStep) {
                normalizedSeries.add(series);
                continue;
            }

            // Resample to normalized time buckets
            TimeSeries resampled = resampleSeries(series, minStart, maxEnd, lcmStep, consolidationFunc);
            normalizedSeries.add(resampled);
        }

        return normalizedSeries;
    }

    /**
     * Convenience method to normalize with default consolidation function.
     *
     * @param seriesList List of time series to normalize
     * @return List of normalized time series with common bucket alignment
     */
    public static List<TimeSeries> normalize(List<TimeSeries> seriesList) {
        return normalize(seriesList, ConsolidationFunction.getDefault());
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
