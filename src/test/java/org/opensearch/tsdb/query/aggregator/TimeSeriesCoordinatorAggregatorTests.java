/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TimeSeriesCoordinatorAggregator}.
 */
public class TimeSeriesCoordinatorAggregatorTests extends OpenSearchTestCase {

    /**
     * doReduce with no stages and empty aggregations yields empty result.
     * This covers the circuit breaker code path that checks for empty results
     * (result != null && !result.isEmpty()).

     */
    public void testDoReduceWithEmptyResultReleasesConsumerAndReturnsEmptyTimeSeries() {
        // No stages and no references - yields empty result
        List<PipelineStage> stages = Collections.emptyList();
        Map<String, String> references = Collections.emptyMap();
        String inputReference = null;

        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_empty_result",
            new String[0],
            stages,
            new LinkedHashMap<>(),
            references,
            inputReference,
            Map.of()
        );

        Aggregations aggregations = new Aggregations(Collections.emptyList());
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries internal = (InternalTimeSeries) result;
        assertTrue(internal.getTimeSeries().isEmpty());
    }

    /**
     * doReduce with a referenced aggregation that yields non-empty time series;
     * covers the branch that tracks result size (result != null && !result.isEmpty()).
     * Empty stages so the result is exactly the referenced aggregation's list.
     */
    public void testDoReduceWithNonEmptyResultTracksResultBytes() {
        TimeSeries oneSeries = new TimeSeries(
            List.of(new FloatSample(1000L, 10.0)),
            ByteLabels.fromMap(Map.of("x", "y")),
            1000L,
            1000L,
            1000L,
            "s1"
        );
        InternalTimeSeries unfoldAgg = new InternalTimeSeries("unfold_a", List.of(oneSeries), Map.of());

        Map<String, String> references = Map.of("a", "unfold_a");
        String inputReference = "a";
        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_non_empty",
            new String[0],
            Collections.emptyList(),
            new LinkedHashMap<>(),
            references,
            inputReference,
            Map.of()
        );

        Aggregations aggregations = new Aggregations(List.of(unfoldAgg));
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries internal = (InternalTimeSeries) result;
        assertFalse(internal.getTimeSeries().isEmpty());
        assertEquals(1, internal.getTimeSeries().size());
    }

    public void testDoReduceEmitsCoordinatorMetricsAndDeduplicatesReferencedPaths() {
        Histogram coordInputSeries = mock(Histogram.class);
        Histogram coordInputSamples = mock(Histogram.class);
        Histogram coordOutputSeries = mock(Histogram.class);
        Histogram coordOutputSamples = mock(Histogram.class);
        Counter coordResultsTotal = mock(Counter.class);
        initializeCoordinatorMetrics(coordInputSeries, coordInputSamples, coordOutputSeries, coordOutputSamples, coordResultsTotal);

        try {
            TimeSeries oneSeries = new TimeSeries(
                List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0)),
                ByteLabels.fromMap(Map.of("x", "y")),
                1000L,
                2000L,
                1000L,
                "s1"
            );
            InternalTimeSeries unfoldAgg = new InternalTimeSeries("unfold_a", List.of(oneSeries), Map.of());

            Map<String, String> references = new LinkedHashMap<>();
            references.put("a", "unfold_a");
            references.put("b", "unfold_a");

            TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
                "test_dedup_metrics",
                new String[0],
                Collections.emptyList(),
                new LinkedHashMap<>(),
                references,
                "a",
                Map.of()
            );

            InternalAggregation result = aggregator.doReduce(
                new Aggregations(List.of(unfoldAgg)),
                createFinalReduceContext(Collections.emptyList())
            );

            assertTrue(result instanceof InternalTimeSeries);
            verify(coordInputSeries).record(eq(1.0), eq(Tags.EMPTY));
            verify(coordInputSamples).record(eq(2.0), eq(Tags.EMPTY));
            verify(coordOutputSeries).record(eq(1.0), eq(Tags.EMPTY));
            verify(coordOutputSamples).record(eq(2.0), eq(Tags.EMPTY));
            verify(coordResultsTotal).add(
                eq(1.0),
                assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("status", "hits"))); })
            );
        } finally {
            TSDBMetrics.cleanup();
        }
    }

    public void testDoReduceEmitsEmptyCoordinatorResultMetric() {
        Histogram coordInputSeries = mock(Histogram.class);
        Histogram coordInputSamples = mock(Histogram.class);
        Histogram coordOutputSeries = mock(Histogram.class);
        Histogram coordOutputSamples = mock(Histogram.class);
        Counter coordResultsTotal = mock(Counter.class);
        initializeCoordinatorMetrics(coordInputSeries, coordInputSamples, coordOutputSeries, coordOutputSamples, coordResultsTotal);

        try {
            TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
                "test_empty_metrics",
                new String[0],
                Collections.emptyList(),
                new LinkedHashMap<>(),
                Collections.emptyMap(),
                null,
                Map.of()
            );

            InternalAggregation result = aggregator.doReduce(
                new Aggregations(Collections.emptyList()),
                createFinalReduceContext(Collections.emptyList())
            );

            assertTrue(result instanceof InternalTimeSeries);
            verify(coordInputSeries, never()).record(anyDouble(), any(Tags.class));
            verify(coordInputSamples, never()).record(anyDouble(), any(Tags.class));
            verify(coordOutputSeries, never()).record(anyDouble(), any(Tags.class));
            verify(coordOutputSamples, never()).record(anyDouble(), any(Tags.class));
            verify(coordResultsTotal).add(
                eq(1.0),
                assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("status", "empty"))); })
            );
        } finally {
            TSDBMetrics.cleanup();
        }
    }

    private void initializeCoordinatorMetrics(
        Histogram coordInputSeries,
        Histogram coordInputSamples,
        Histogram coordOutputSeries,
        Histogram coordOutputSamples,
        Counter coordResultsTotal
    ) {
        MetricsRegistry registry = mock(MetricsRegistry.class);
        when(registry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(registry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));

        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(registry);
        TSDBMetrics.AGGREGATION.coordInputSeries = coordInputSeries;
        TSDBMetrics.AGGREGATION.coordInputSamples = coordInputSamples;
        TSDBMetrics.AGGREGATION.coordOutputSeries = coordOutputSeries;
        TSDBMetrics.AGGREGATION.coordOutputSamples = coordOutputSamples;
        TSDBMetrics.AGGREGATION.coordResultsTotal = coordResultsTotal;
    }

    private InternalAggregation.ReduceContext createFinalReduceContext(List<PipelineAggregator> rootAggregators) {
        PipelineAggregator.PipelineTree rootTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), rootAggregators);
        return InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, rootTree);
    }
}
