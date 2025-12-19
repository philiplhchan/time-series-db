/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for TSDB tests.
 */
public final class TSDBTestUtils {

    // Private constructor to prevent instantiation
    private TSDBTestUtils() {}

    /**
     * Create a TSDB document JSON string from a Labels object.
     *
     * @param labels The Labels object
     * @param timestamp Timestamp in milliseconds since epoch
     * @param val The numeric value of the sample
     * @return JSON string in TSDB engine format
     * @throws IOException If JSON building fails
     */
    public static String createSampleJson(Labels labels, long timestamp, double val) throws IOException {
        return createSampleJson(formatLabelsAsSpaceSeparated(labels.toMapView()), timestamp, val);
    }

    /**
     * Create a TSDB document JSON string from a TimeSeriesSample.
     *
     * @param sample The time series sample to convert
     * @return JSON string in TSDB engine format
     * @throws IOException If JSON building fails
     */
    public static String createTSDBDocumentJson(TimeSeriesSample sample) throws IOException {
        return createSampleJson(formatLabelsAsSpaceSeparated(sample.labels()), sample.timestamp().toEpochMilli(), sample.value());
    }

    /**
     * Create a TSDB document JSON string from labels string, timestamp, and value.
     *
     * @param labelsString Space-separated label key-value pairs (e.g., "name http_requests method GET")
     * @param timestamp Timestamp in milliseconds since epoch
     * @param val The numeric value of the sample
     * @return JSON string in TSDB engine format
     * @throws IOException If JSON building fails
     */
    public static String createSampleJson(String labelsString, long timestamp, double val) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, val);
            builder.endObject();
            return builder.toString();
        }
    }

    /**
     * Format labels as space-separated key-value pairs.
     * Example: {"name": "http_requests", "method": "GET"} -> "name http_requests method GET"
     *
     * @param labels Map of label key-value pairs
     * @return Space-separated string of key-value pairs
     */
    private static String formatLabelsAsSpaceSeparated(Map<String, String> labels) {
        if (labels == null || labels.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            if (!first) {
                sb.append(" ");
            }
            first = false;
            // Format as "key value" pairs separated by spaces
            sb.append(String.format(Locale.ROOT, "%s %s", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    /**
     * Count all samples in a TSDBEngine from both live (in-memory) and closed (on-disk) chunks.
     * Uses TSDBEngine#getReferenceManager which returns a reader aggregating both live and closed leaves,
     * and TSDBLeafReader#chunksForDoc to uniformly access chunks from both types of leaves.
     *
     * @param engine The TSDBEngine to count samples from
     * @return Total number of samples across all chunks
     */
    public static long countSamples(TSDBEngine engine) throws Exception {
        long totalSamples = 0;

        // Uses getReferenceManager to get a reader that includes both live and closed leaves
        try (Engine.Searcher searcher = engine.acquireSearcher("count_samples")) {
            IndexReader reader = searcher.getDirectoryReader();

            for (LeafReaderContext context : reader.leaves()) {
                LeafReader leafReader = context.reader();

                TSDBLeafReader tsdbLeafReader = TSDBLeafReader.unwrapLeafReader(leafReader);
                if (tsdbLeafReader == null) {
                    continue;
                }

                TSDBDocValues tsdbDocValues = tsdbLeafReader.getTSDBDocValues();

                // Iterate through all docs in this leaf
                for (int doc = 0; doc < leafReader.maxDoc(); doc++) {
                    List<ChunkIterator> chunks = tsdbLeafReader.chunksForDoc(doc, tsdbDocValues);

                    // Count samples in each chunk
                    for (ChunkIterator chunkIterator : chunks) {
                        while (chunkIterator.next() != ChunkIterator.ValueType.NONE) {
                            totalSamples++;
                        }
                    }
                }
            }
        }

        return totalSamples;
    }

    /**
     * Get sample count from an index using TimeSeriesUnfold aggregation.
     * This is useful for integration tests where you need to verify sample counts via the search API.
     *
     * @param client The OpenSearch client
     * @param indexName The name of the index to query
     * @param startTime Start of the time range (inclusive)
     * @param endTime End of the time range (inclusive)
     * @param interval Interval for the aggregation
     * @return Total number of samples across all time series in the time range
     */
    public static int getSampleCountViaAggregation(Client client, String indexName, long startTime, long endTime, long interval) {
        AggregationBuilder aggregation = new TimeSeriesUnfoldAggregationBuilder("raw_unfold", List.of(), startTime, endTime, interval);
        SearchResponse aggResponse = client.prepareSearch(indexName).setSize(0).addAggregation(aggregation).get();

        InternalTimeSeries unfoldResult = aggResponse.getAggregations().get("raw_unfold");
        if (unfoldResult == null || unfoldResult.getTimeSeries().isEmpty()) {
            return 0;
        }

        // Sum up samples from all time series
        int totalSamples = 0;
        for (TimeSeries series : unfoldResult.getTimeSeries()) {
            totalSamples += series.getSamples().size();
        }
        return totalSamples;
    }
}
