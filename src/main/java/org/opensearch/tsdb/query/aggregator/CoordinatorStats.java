/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import java.util.List;

final class CoordinatorStats {
    private static final Tags TAGS_STATUS_EMPTY = Tags.create()
        .addTag(TSDBMetricsConstants.TAG_STATUS, TSDBMetricsConstants.TAG_STATUS_EMPTY);
    private static final Tags TAGS_STATUS_HITS = Tags.create()
        .addTag(TSDBMetricsConstants.TAG_STATUS, TSDBMetricsConstants.TAG_STATUS_HITS);

    long inputSeriesCount;
    long inputSampleCount;
    long outputSeriesCount;
    long outputSampleCount;

    void addInput(List<TimeSeries> timeSeries) {
        Counts counts = countTimeSeries(timeSeries);
        inputSeriesCount += counts.seriesCount();
        inputSampleCount += counts.sampleCount();
    }

    void addOutput(List<TimeSeries> timeSeries) {
        Counts counts = countTimeSeries(timeSeries);
        outputSeriesCount += counts.seriesCount();
        outputSampleCount += counts.sampleCount();
    }

    void recordMetrics() {
        if (!TSDBMetrics.isInitialized()) {
            return;
        }

        try {
            if (inputSeriesCount > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.coordInputSeries, inputSeriesCount);
            }
            if (inputSampleCount > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.coordInputSamples, inputSampleCount);
            }
            if (outputSeriesCount > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.coordOutputSeries, outputSeriesCount);
            }
            if (outputSampleCount > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.coordOutputSamples, outputSampleCount);
            }

            if (outputSeriesCount > 0) {
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.coordResultsTotal, 1, TAGS_STATUS_HITS);
            } else {
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.coordResultsTotal, 1, TAGS_STATUS_EMPTY);
            }
        } catch (Exception e) {}
    }

    private Counts countTimeSeries(List<TimeSeries> timeSeries) {
        if (timeSeries == null || timeSeries.isEmpty()) {
            return Counts.EMPTY;
        }

        long sampleCount = 0;
        for (TimeSeries series : timeSeries) {
            if (series != null && series.getSamples() != null) {
                sampleCount += series.getSamples().size();
            }
        }
        return new Counts(timeSeries.size(), sampleCount);
    }

    private record Counts(long seriesCount, long sampleCount) {
        private static final Counts EMPTY = new Counts(0, 0);
    }
}
