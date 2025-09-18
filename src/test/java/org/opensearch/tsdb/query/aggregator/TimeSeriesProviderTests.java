/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TimeSeriesProviderTests extends OpenSearchTestCase {

    /**
     * Mock implementation of TimeSeriesProvider for testing purposes.
     */
    private static class MockTimeSeriesProvider implements TimeSeriesProvider {
        private final List<TimeSeries> timeSeries;

        public MockTimeSeriesProvider(List<TimeSeries> timeSeries) {
            this.timeSeries = timeSeries != null ? timeSeries : List.of();
        }

        @Override
        public List<TimeSeries> getTimeSeries() {
            return timeSeries;
        }

        @Override
        public TimeSeriesProvider createReduced(List<TimeSeries> timeSeries) {
            return new MockTimeSeriesProvider(timeSeries);
        }
    }

    public void testGetTimeSeries() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);
        List<TimeSeries> expectedTimeSeries = Arrays.asList(timeSeries);
        TimeSeriesProvider provider = new MockTimeSeriesProvider(expectedTimeSeries);

        // Act
        List<TimeSeries> result = provider.getTimeSeries();

        // Assert
        assertEquals(expectedTimeSeries, result);
    }

    public void testCreateReduced() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);
        TimeSeriesProvider provider = new MockTimeSeriesProvider(Arrays.asList(timeSeries));

        // Act
        TimeSeriesProvider reducedProvider = provider.createReduced(Arrays.asList(timeSeries));

        // Assert
        assertNotNull(reducedProvider);
        assertNotEquals(provider, reducedProvider);
        assertEquals(Arrays.asList(timeSeries), reducedProvider.getTimeSeries());
    }
}
