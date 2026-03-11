/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBAggregationMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;
    private TSDBAggregationMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBAggregationMetrics();
    }

    @Override
    public void tearDown() throws Exception {
        metrics.cleanup();
        super.tearDown();
    }

    public void testInitialize() {
        // Create mock histograms
        Histogram collectLatency = mock(Histogram.class);
        Histogram postCollectLatency = mock(Histogram.class);
        Histogram docsTotal = mock(Histogram.class);
        Histogram docsLive = mock(Histogram.class);
        Histogram docsClosed = mock(Histogram.class);
        Histogram chunksTotal = mock(Histogram.class);
        Histogram chunksLive = mock(Histogram.class);
        Histogram chunksClosed = mock(Histogram.class);
        Histogram samplesTotal = mock(Histogram.class);
        Histogram samplesLive = mock(Histogram.class);
        Histogram samplesClosed = mock(Histogram.class);
        Counter chunksForDocErrors = mock(Counter.class);
        Counter resultsTotal = mock(Counter.class);
        Histogram seriesTotal = mock(Histogram.class);
        Histogram coordInputSeries = mock(Histogram.class);
        Histogram coordInputSamples = mock(Histogram.class);
        Histogram coordOutputSeries = mock(Histogram.class);
        Histogram coordOutputSamples = mock(Histogram.class);
        Counter coordResultsTotal = mock(Counter.class);

        // Setup mocks
        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY),
                eq(TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(collectLatency);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_POST_COLLECT_LATENCY),
                eq(TSDBMetricsConstants.AGGREGATION_POST_COLLECT_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(postCollectLatency);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(docsTotal);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_LIVE),
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_LIVE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(docsLive);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_CLOSED),
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_CLOSED_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(docsClosed);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(chunksTotal);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_LIVE),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_LIVE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(chunksLive);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_CLOSED),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_CLOSED_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(chunksClosed);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(samplesTotal);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_LIVE),
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_LIVE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(samplesLive);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_CLOSED),
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_CLOSED_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(samplesClosed);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(chunksForDocErrors);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.AGGREGATION_RESULTS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_RESULTS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(resultsTotal);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SERIES_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_SERIES_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(seriesTotal);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SERIES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SERIES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(coordInputSeries);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SAMPLES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SAMPLES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(coordInputSamples);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SERIES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SERIES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(coordOutputSeries);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SAMPLES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SAMPLES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(coordOutputSamples);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_RESULTS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_RESULTS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(coordResultsTotal);

        metrics.initialize(registry);

        // Verify all metrics were created
        assertSame(collectLatency, metrics.collectLatency);
        assertSame(postCollectLatency, metrics.postCollectLatency);
        assertSame(docsTotal, metrics.docsTotal);
        assertSame(docsLive, metrics.docsLive);
        assertSame(docsClosed, metrics.docsClosed);
        assertSame(chunksTotal, metrics.chunksTotal);
        assertSame(chunksLive, metrics.chunksLive);
        assertSame(chunksClosed, metrics.chunksClosed);
        assertSame(samplesTotal, metrics.samplesTotal);
        assertSame(samplesLive, metrics.samplesLive);
        assertSame(samplesClosed, metrics.samplesClosed);
        assertSame(chunksForDocErrors, metrics.chunksForDocErrors);
        assertSame(resultsTotal, metrics.resultsTotal);
        assertSame(seriesTotal, metrics.seriesTotal);
        assertSame(coordInputSeries, metrics.coordInputSeries);
        assertSame(coordInputSamples, metrics.coordInputSamples);
        assertSame(coordOutputSeries, metrics.coordOutputSeries);
        assertSame(coordOutputSamples, metrics.coordOutputSamples);
        assertSame(coordResultsTotal, metrics.coordResultsTotal);

        // Verify registry interactions
        verify(registry).createHistogram(
            TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY,
            TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
        verify(registry).createCounter(
            TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL,
            TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
    }

    public void testCleanup() {
        // Setup mocks for initialization
        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY),
                eq(TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_POST_COLLECT_LATENCY),
                eq(TSDBMetricsConstants.AGGREGATION_POST_COLLECT_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_LIVE),
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_LIVE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_CLOSED),
                eq(TSDBMetricsConstants.AGGREGATION_DOCS_CLOSED_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_LIVE),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_LIVE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_CLOSED),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_CLOSED_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_LIVE),
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_LIVE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_CLOSED),
                eq(TSDBMetricsConstants.AGGREGATION_SAMPLES_CLOSED_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.AGGREGATION_RESULTS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_RESULTS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_SERIES_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_SERIES_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SERIES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SERIES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SAMPLES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_INPUT_SAMPLES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SERIES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SERIES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SAMPLES),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_OUTPUT_SAMPLES_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.AGGREGATION_COORD_RESULTS_TOTAL),
                eq(TSDBMetricsConstants.AGGREGATION_COORD_RESULTS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        metrics.initialize(registry);
        assertNotNull(metrics.collectLatency);
        assertNotNull(metrics.chunksForDocErrors);

        metrics.cleanup();

        assertNull(metrics.collectLatency);
        assertNull(metrics.postCollectLatency);
        assertNull(metrics.docsTotal);
        assertNull(metrics.docsLive);
        assertNull(metrics.docsClosed);
        assertNull(metrics.chunksTotal);
        assertNull(metrics.chunksLive);
        assertNull(metrics.chunksClosed);
        assertNull(metrics.samplesTotal);
        assertNull(metrics.samplesLive);
        assertNull(metrics.samplesClosed);
        assertNull(metrics.chunksForDocErrors);
        assertNull(metrics.resultsTotal);
        assertNull(metrics.seriesTotal);
        assertNull(metrics.coordInputSeries);
        assertNull(metrics.coordInputSamples);
        assertNull(metrics.coordOutputSeries);
        assertNull(metrics.coordOutputSamples);
        assertNull(metrics.coordResultsTotal);
    }

    public void testCleanupBeforeInitialization() {
        // Should not throw
        metrics.cleanup();

        assertNull(metrics.collectLatency);
        assertNull(metrics.chunksForDocErrors);
    }
}
