/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder class for creating test data for TSDB tests.
 *
 * <p>Provides convenient methods to create:
 * <ul>
 *   <li>Time series with various configurations (labels, aliases, samples)</li>
 *   <li>Search responses with aggregations</li>
 *   <li>Mock data for edge cases (null values, empty collections)</li>
 * </ul>
 *
 * <p>This class is designed to be used across all TSDB tests, not just REST tests.
 */
public final class TestDataBuilder {

    private static final Map<String, Object> EMPTY_METADATA = Collections.emptyMap();

    private TestDataBuilder() {
        // Utility class - no instantiation
    }

    // ========== Time Series Creation Methods ==========

    /**
     * Creates a simple time series with labels.
     *
     * @return list containing one time series with labels
     */
    public static List<TimeSeries> createTimeSeriesWithLabels() {
        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 10.0));

        Map<String, String> labelMap = new HashMap<>();
        labelMap.put("region", "us-east");
        labelMap.put("service", "api");
        Labels labels = ByteLabels.fromMap(labelMap);

        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);
        return List.of(timeSeries);
    }

    /**
     * Creates a time series with an alias.
     *
     * @return list containing one time series with an alias
     */
    public static List<TimeSeries> createTimeSeriesWithAlias() {
        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 10.0));

        Labels labels = ByteLabels.fromMap(Map.of("region", "us-west"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, "my_metric");
        return List.of(timeSeries);
    }

    /**
     * Creates a time series with multiple samples.
     *
     * @param count number of samples to create
     * @return list containing one time series with multiple samples
     */
    public static List<TimeSeries> createTimeSeriesWithMultipleSamples(int count) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            samples.add(new FloatSample(1000L * (i + 1), 10.0 * (i + 1)));
        }

        Labels labels = ByteLabels.emptyLabels();
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L * count, 1000L, "multi-sample");
        return List.of(timeSeries);
    }

    /**
     * Creates a time series with no samples (empty values array).
     *
     * @return list containing one time series with empty samples
     */
    public static List<TimeSeries> createTimeSeriesWithEmptySamples() {
        List<Sample> samples = Collections.emptyList();
        Labels labels = ByteLabels.emptyLabels();
        TimeSeries timeSeries = new TimeSeries(samples, labels, 0L, 0L, 1000L, "empty");
        return List.of(timeSeries);
    }

    /**
     * Creates a time series with null labels.
     *
     * @return list containing one time series with null labels
     */
    public static List<TimeSeries> createTimeSeriesWithNullLabels() {
        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 10.0));

        TimeSeries timeSeries = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);
        return List.of(timeSeries);
    }

    /**
     * Creates a time series with null alias.
     *
     * @return list containing one time series with null alias
     */
    public static List<TimeSeries> createTimeSeriesWithNullAlias() {
        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 10.0));

        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);
        return List.of(timeSeries);
    }

    /**
     * Creates a time series with custom samples.
     *
     * @param samples the samples to include
     * @param labels the labels to include
     * @param alias the alias (can be null)
     * @return list containing one time series
     */
    public static List<TimeSeries> createTimeSeriesWith(List<Sample> samples, Labels labels, String alias) {
        long minTs = samples.isEmpty() ? 0 : samples.get(0).getTimestamp();
        long maxTs = samples.isEmpty() ? 0 : samples.get(samples.size() - 1).getTimestamp();
        TimeSeries timeSeries = new TimeSeries(samples, labels, minTs, maxTs, 1000L, alias);
        return List.of(timeSeries);
    }

    // ========== SearchResponse Creation Methods ==========

    /**
     * Creates a search response with a single aggregation.
     *
     * @param aggName the aggregation name
     * @param timeSeriesList the list of time series to include
     * @return a mock SearchResponse
     */
    public static SearchResponse createSearchResponse(String aggName, List<TimeSeries> timeSeriesList) {
        InternalTimeSeries timeSeries = new InternalTimeSeries(aggName, timeSeriesList, EMPTY_METADATA);
        List<InternalTimeSeries> aggsList = List.of(timeSeries);
        Aggregations aggregations = new Aggregations(aggsList);
        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), aggregations, null, false, null, null, 0);
        return new SearchResponse(sections, null, 1, 1, 0, 10, null, null);
    }

    /**
     * Creates a search response with null aggregations.
     *
     * @return a mock SearchResponse with no aggregations
     */
    public static SearchResponse createSearchResponseWithNullAggregations() {
        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), null, null, false, null, null, 0);
        return new SearchResponse(sections, null, 1, 1, 0, 10, null, null);
    }

    /**
     * Creates a search response with multiple aggregations (useful for filter tests).
     *
     * <p>Creates 3 aggregations named "agg1", "agg2", and "agg3" each containing
     * one time series with metrics "metric-from-agg1", "metric-from-agg2", and
     * "metric-from-agg3" respectively.</p>
     *
     * @return a mock SearchResponse with 3 different aggregations
     */
    public static SearchResponse createSearchResponseWithMultipleAggregations() {
        // Create 3 different aggregations with different names
        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        TimeSeries ts1 = new TimeSeries(samples1, ByteLabels.emptyLabels(), 1000L, 1000L, 1000L, "metric-from-agg1");
        InternalTimeSeries agg1 = new InternalTimeSeries("agg1", List.of(ts1), EMPTY_METADATA);

        List<Sample> samples2 = List.of(new FloatSample(2000L, 20.0));
        TimeSeries ts2 = new TimeSeries(samples2, ByteLabels.emptyLabels(), 2000L, 2000L, 1000L, "metric-from-agg2");
        InternalTimeSeries agg2 = new InternalTimeSeries("agg2", List.of(ts2), EMPTY_METADATA);

        List<Sample> samples3 = List.of(new FloatSample(3000L, 30.0));
        TimeSeries ts3 = new TimeSeries(samples3, ByteLabels.emptyLabels(), 3000L, 3000L, 1000L, "metric-from-agg3");
        InternalTimeSeries agg3 = new InternalTimeSeries("agg3", List.of(ts3), EMPTY_METADATA);

        // Create aggregations with all 3
        Aggregations aggregations = new Aggregations(List.of(agg1, agg2, agg3));
        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), aggregations, null, false, null, null, 0);
        return new SearchResponse(sections, null, 1, 1, 0, 10, null, null);
    }

    /**
     * Creates a search response with custom aggregations.
     *
     * @param aggregations the aggregations to include
     * @return a mock SearchResponse
     */
    public static SearchResponse createSearchResponseWithAggregations(Aggregations aggregations) {
        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), aggregations, null, false, null, null, 0);
        return new SearchResponse(sections, null, 1, 1, 0, 10, null, null);
    }

    // ========== Mock Classes ==========

    /**
     * SearchResponse that throws exceptions when aggregations are accessed.
     * Useful for testing error handling.
     */
    public static class FailingSearchResponse extends SearchResponse {
        public FailingSearchResponse() {
            super(new SearchResponseSections(SearchHits.empty(), null, null, false, null, null, 0), null, 1, 1, 0, 10, null, null);
        }

        @Override
        public Aggregations getAggregations() {
            throw new RuntimeException("Simulated aggregation failure");
        }
    }
}
