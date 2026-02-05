/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.ArrowBulkDecoder;
import org.opensearch.index.engine.SmileDirectParser;
import org.opensearch.index.engine.TSDBDocument;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.tsdb.core.mapping.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing three document parsing/decoding approaches:
 * <ol>
 *   <li><b>JSON + XContentParser</b> - Current baseline approach</li>
 *   <li><b>SMILE + SmileDirectParser</b> - Direct SMILE byte parsing</li>
 *   <li><b>Native Arrow Flight</b> - Direct Arrow vector access (no parsing)</li>
 * </ol>
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :benchmarks:jmh -Pjmh.includes="ArrowDecodingBenchmark" -Pjmh.profilers="gc"
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class ArrowDecodingBenchmark {

    @Param({ "10000" })
    private int batchSize;

    @Param({ "10", "15", "20" })
    private int labelsPerDocument;

    // JSON payloads and parsed documents (current baseline)
    private List<ParsedDocument> jsonParsedDocuments;

    // SMILE payloads for direct parsing
    private List<BytesReference> smilePayloads;

    // Arrow Flight data (pre-populated arrays simulating Arrow Flight receive)
    // Labels are pre-split into key-value pairs on the client side
    private String[][] arrowLabelKVPairs;
    private long[] arrowTimestamps;
    private double[] arrowValues;

    // Arrow decoder (reusable)
    private ArrowBulkDecoder arrowDecoder;

    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setup() throws IOException {
        jsonParsedDocuments = new ArrayList<>(batchSize);
        smilePayloads = new ArrayList<>(batchSize);

        // Arrow Flight data arrays (pre-split labels)
        arrowLabelKVPairs = new String[batchSize][];
        arrowTimestamps = new long[batchSize];
        arrowValues = new double[batchSize];

        long baseTimestamp = System.currentTimeMillis();

        for (int i = 0; i < batchSize; i++) {
            // Generate pre-split label key-value pairs: ["key0", "value0", "key1", "value1", ...]
            String[] labelKVPairs = generateLabelKVPairs(i, labelsPerDocument);
            // Also generate the space-delimited string for JSON/SMILE
            String labelsString = String.join(" ", labelKVPairs);

            long timestamp = baseTimestamp; // all samples within a batch usually has the same timestamp
            double value = random.nextDouble() * 1000;
            // series_ref is always null for this benchmark
            Long seriesRef = null;

            // JSON payload and ParsedDocument
            BytesReference jsonSource = generateJsonDocument(labelsString, timestamp, value, seriesRef);
            jsonParsedDocuments.add(createParsedDocument(jsonSource, XContentType.JSON, i));

            // SMILE payload
            BytesReference smileSource = generateSmileDocument(labelsString, timestamp, value, seriesRef);
            smilePayloads.add(smileSource);

            // Arrow Flight data (pre-split labels - no serialization needed)
            arrowLabelKVPairs[i] = labelKVPairs;
            arrowTimestamps[i] = timestamp;
            arrowValues[i] = value;
        }

        arrowDecoder = new ArrowBulkDecoder(batchSize);
        // Simulate Arrow Flight receive - populate vectors with pre-split labels
        arrowDecoder.populateFromArrowFlight(batchSize, arrowLabelKVPairs, arrowTimestamps, arrowValues);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (arrowDecoder != null) {
            arrowDecoder.close();
        }
    }

    /**
     * Baseline: JSON + XContentParser (current approach).
     * Creates XContentParser for each document.
     */
    @Benchmark
    public void baselineJsonParsing(Blackhole bh) {
        for (ParsedDocument doc : jsonParsedDocuments) {
            TSDBDocument result = TSDBDocument.fromParsedDocument(doc);
        }
    }

    /**
     * SMILE + SmileDirectParser.
     * Direct SMILE byte parsing without XContentParser allocation.
     */
    @Benchmark
    public void smileDirectParsing(Blackhole bh) {
        for (BytesReference source : smilePayloads) {
            TSDBDocument result = SmileDirectParser.parse(source);
        }
    }

    /**
     * Native Arrow Flight.
     * Direct Arrow vector access - no parsing, labels pre-split on client side.
     * Simulates receiving data via Arrow Flight protocol.
     */
    @Benchmark
    public void arrowFlightNative(Blackhole bh) {
        // Read from vectors and create TSDBDocument (same as other approaches)
        for (int i = 0; i < arrowDecoder.getBatchSize(); i++) {
            TSDBDocument result = arrowDecoder.getTSDBDocument(i);
        }

        arrowDecoder.reset();
    }

    /**
     * Generate pre-split label key-value pairs: ["key0", "value0", "key1", "value1", ...]
     */
    private String[] generateLabelKVPairs(int index, int numLabels) {
        String[] pairs = new String[numLabels * 2];
        for (int i = 0; i < numLabels; i++) {
            pairs[i * 2] = "key" + i;
            pairs[i * 2 + 1] = "value" + index + "_" + i;
        }
        return pairs;
    }

    private String generateLabelsString(int index, int numLabels) {
        StringBuilder labels = new StringBuilder();
        for (int i = 0; i < numLabels; i++) {
            if (i > 0) labels.append(" ");
            labels.append("key").append(i).append(" ");
            labels.append("value").append(index).append("_").append(i);
        }
        return labels.toString();
    }

    private BytesReference generateJsonDocument(String labelsString, long timestamp, double value, Long seriesRef)
        throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            if (seriesRef != null) {
                builder.field(Constants.IndexSchema.REFERENCE, seriesRef);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private BytesReference generateSmileDocument(String labelsString, long timestamp, double value, Long seriesRef)
        throws IOException {
        try (XContentBuilder builder = SmileXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            if (seriesRef != null) {
                builder.field(Constants.IndexSchema.REFERENCE, seriesRef);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private ParsedDocument createParsedDocument(BytesReference source, XContentType contentType, int index) {
        return new ParsedDocument(null, null, "doc_" + index, null, null, source, contentType, null);
    }
}
