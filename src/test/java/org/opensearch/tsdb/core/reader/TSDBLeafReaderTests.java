/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexTSDBDocValues;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexTSDBDocValues;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE;

/**
 * Unit tests for TSDBLeafReader implementations.
 */
public class TSDBLeafReaderTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter indexWriter;
    private MemChunkReader memChunkReader;
    private Map<Long, List<MemChunk>> referenceToChunkMap;
    private Map<Long, java.util.Set<MemChunk>> mmappedChunks;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        indexWriter = new IndexWriter(directory, config);

        // Initialize the reference to chunk mapping
        referenceToChunkMap = new HashMap<>();
        setupReferenceToChunkMapping();

        // Create a MemChunkReader that looks up chunks from the map
        memChunkReader = (reference) -> { return referenceToChunkMap.getOrDefault(reference, List.of()); };

        // Setup empty mmapped chunks for testing
        mmappedChunks = new HashMap<>();
    }

    @Override
    public void tearDown() throws Exception {
        if (indexWriter != null) {
            indexWriter.close();
        }
        if (directory != null) {
            directory.close();
        }

        super.tearDown();
    }

    /**
     * Sets up the reference to chunk mapping for testing.
     * This method populates the map with chunks corresponding to the references
     * used in createLiveSeriesTestDocuments().
     */
    private void setupReferenceToChunkMapping() {
        // Reference 100L: cpu_usage{host="server1", region="us-west"}
        MemChunk cpuMemChunk = new MemChunk(1L, 1000L, 4000L, null, Encoding.XOR);
        cpuMemChunk.append(1000L, 75.5, 1L);
        cpuMemChunk.append(2000L, 80.2, 2L);
        cpuMemChunk.append(3000L, 85.1, 3L);
        referenceToChunkMap.put(100L, List.of(cpuMemChunk));

        // Reference 200L: memory_usage{host="server2", region="us-east"}
        MemChunk memoryMemChunk = new MemChunk(4L, 1000L, 4000L, null, Encoding.XOR);
        memoryMemChunk.append(1000L, 2048.0, 4L);
        memoryMemChunk.append(2000L, 2560.0, 5L);
        memoryMemChunk.append(3000L, 3072.0, 6L);
        referenceToChunkMap.put(200L, List.of(memoryMemChunk));
    }

    /**
     * Test ClosedChunkIndexLeafReader functionality
     */
    public void testClosedChunkIndexLeafReader() throws IOException {
        // Create test documents with chunks and labels
        createClosedChunkTestDocuments();

        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create ClosedChunkIndexLeafReader
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY);

            // Test getTSDBDocValues()
            TSDBDocValues tsdbDocValues = metricsReader.getTSDBDocValues();
            assertNotNull("tsdbDocValues should not be null", tsdbDocValues);
            assertTrue("Should be ClosedChunkIndexTSDBDocValues", tsdbDocValues instanceof ClosedChunkIndexTSDBDocValues);

            // Verify that chunk ref doc values throws UnsupportedOperationException
            expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkRefDocValues);

            // Test chunksForDoc() for document 0
            List<ChunkIterator> chunks = metricsReader.chunksForDoc(0, tsdbDocValues);
            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk", 1, chunks.size());

            ChunkIterator chunk = chunks.get(0);
            assertNotNull("Chunk should not be null", chunk);
            assertTrue("Chunk should have data", chunk.next() != ChunkIterator.ValueType.NONE);

            // Test labelsForDoc() for document 0
            Labels labels = metricsReader.labelsForDoc(0, tsdbDocValues);
            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have correct metric name", "http_requests_total", labels.get("__name__"));
            assertEquals("Should have correct method label", "GET", labels.get("method"));
            assertEquals("Should have correct status label", "200", labels.get("status"));
        }
    }

    /**
     * Test LiveSeriesIndexLeafReader functionality
     */
    public void testLiveSeriesIndexLeafReader() throws IOException {
        // Create test documents with references and labels
        createLiveSeriesTestDocuments();

        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create LiveSeriesIndexLeafReader
            LiveSeriesIndexLeafReader metricsReader = new LiveSeriesIndexLeafReader(
                leafReader,
                memChunkReader,
                mmappedChunks,
                LabelStorageType.BINARY
            );

            // Test getTSDBDocValues()
            TSDBDocValues tsdbDocValues = metricsReader.getTSDBDocValues();
            assertNotNull("tsdbDocValues should not be null", tsdbDocValues);
            assertTrue("Should be LiveSeriesIndexTSDBDocValues", tsdbDocValues instanceof LiveSeriesIndexTSDBDocValues);

            // Verify that chunk doc values throws UnsupportedOperationException
            expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);

            // Test chunksForDoc() for document 0 (reference 100L)
            List<ChunkIterator> chunks = metricsReader.chunksForDoc(0, tsdbDocValues);
            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk for reference 100L", 1, chunks.size());

            // Verify the chunk contains expected data
            ChunkIterator chunkIterator = chunks.get(0);
            assertTrue("Chunk should have data", chunkIterator.next() != ChunkIterator.ValueType.NONE);
            ChunkIterator.TimestampValue firstValue = chunkIterator.at();
            assertEquals("First timestamp should be 1000L", 1000L, firstValue.timestamp());
            assertEquals("First value should be 75.5", 75.5, firstValue.value(), 0.001);

            // Test labelsForDoc() for document 0
            Labels labels = metricsReader.labelsForDoc(0, tsdbDocValues);
            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have correct metric name", "cpu_usage", labels.get("__name__"));
            assertEquals("Should have correct host label", "server1", labels.get("host"));
            assertEquals("Should have correct region label", "us-west", labels.get("region"));
        }
    }

    /**
     * Test error handling when chunk field is missing
     */
    public void testClosedChunkIndexLeafReaderMissingChunkField() throws IOException {
        // Create document without chunk field
        Document doc = new Document();

        ByteLabels labels = ByteLabels.fromStrings("__name__", "test_metric");
        BytesRef serializedLabels = new BytesRef(labels.getRawBytes());
        doc.add(new BinaryDocValuesField(LABELS, serializedLabels));

        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY);

            // Should throw IOException when chunk field is missing
            expectThrows(IOException.class, metricsReader::getTSDBDocValues);
        }
    }

    private void createClosedChunkTestDocuments() throws IOException {
        // Create MemChunk instances with XOR chunks and sample data
        MemChunk memChunk1 = new MemChunk(1, 1000L, 3000L, null, Encoding.XOR);
        memChunk1.append(1000L, 42.0, 1L);
        memChunk1.append(2000L, 43.0, 2L);
        memChunk1.append(3000L, 44.0, 3L);

        MemChunk memChunk2 = new MemChunk(2, 4000L, 5000L, null, Encoding.XOR);
        memChunk2.append(4000L, 100.0, 4L);
        memChunk2.append(5000L, 101.0, 5L);

        // Serialize chunks
        BytesRef serializedChunk1 = ClosedChunkIndexIO.serializeChunk(memChunk1.getCompoundChunk().toChunk());
        BytesRef serializedChunk2 = ClosedChunkIndexIO.serializeChunk(memChunk2.getCompoundChunk().toChunk());

        // Document 1: http_requests_total{method="GET", status="200"}
        Document doc1 = new Document();
        doc1.add(new BinaryDocValuesField(CHUNK, serializedChunk1));

        ByteLabels labels1 = ByteLabels.fromStrings("__name__", "http_requests_total", "method", "GET", "status", "200");
        BytesRef serializedLabels1 = new BytesRef(labels1.getRawBytes());
        doc1.add(new BinaryDocValuesField(LABELS, serializedLabels1));

        indexWriter.addDocument(doc1);

        // Document 2: http_requests_total{method="POST", status="404"}
        Document doc2 = new Document();
        doc2.add(new BinaryDocValuesField(CHUNK, serializedChunk2));

        ByteLabels labels2 = ByteLabels.fromStrings("__name__", "http_requests_total", "method", "POST", "status", "404");
        BytesRef serializedLabels2 = new BytesRef(labels2.getRawBytes());
        doc2.add(new BinaryDocValuesField(LABELS, serializedLabels2));

        indexWriter.addDocument(doc2);
    }

    private void createLiveSeriesTestDocuments() throws IOException {
        // Document 1: cpu_usage{host="server1", region="us-west"} with reference 100L
        Document doc1 = new Document();
        doc1.add(new NumericDocValuesField(REFERENCE, 100L));

        ByteLabels labels1 = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1", "region", "us-west");
        BytesRef serializedLabels1 = new BytesRef(labels1.getRawBytes());
        doc1.add(new BinaryDocValuesField(LABELS, serializedLabels1));

        indexWriter.addDocument(doc1);

        // Document 2: memory_usage{host="server2", region="us-east"} with reference 200L
        Document doc2 = new Document();
        doc2.add(new NumericDocValuesField(REFERENCE, 200L));

        ByteLabels labels2 = ByteLabels.fromStrings("__name__", "memory_usage", "host", "server2", "region", "us-east");
        BytesRef serializedLabels2 = new BytesRef(labels2.getRawBytes());
        doc2.add(new BinaryDocValuesField(LABELS, serializedLabels2));

        indexWriter.addDocument(doc2);
    }

    /**
     * Test that ClosedChunkIndexLeafReader with metadata constructor stores time bounds correctly
     */
    public void testClosedChunkIndexLeafReaderWithMetadata() throws IOException {
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create reader with metadata (minTimestamp=1000, maxTimestamp=5000)
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 1000L, 5000L);

            // Verify metadata is stored correctly
            assertEquals("Min timestamp should be 1000", 1000L, metricsReader.getMinIndexTimestamp());
            assertEquals("Max timestamp should be 5000", 5000L, metricsReader.getMaxIndexTimestamp());
        }
    }

    /**
     * Test that old constructor (without metadata) uses default values
     */
    public void testClosedChunkIndexLeafReaderWithoutMetadata() throws IOException {
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create reader without metadata (old constructor)
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY);

            // Verify default values are used
            assertEquals("Min timestamp should be Long.MIN_VALUE", Long.MIN_VALUE, metricsReader.getMinIndexTimestamp());
            assertEquals("Max timestamp should be Long.MAX_VALUE", Long.MAX_VALUE, metricsReader.getMaxIndexTimestamp());
        }
    }

    /**
     * Test overlapsTimeRange with various scenarios
     */
    public void testOverlapsTimeRange() throws IOException {
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Leaf with time range [1000, 5000]
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 1000L, 5000L);

            // Test case 1: Query completely overlaps leaf
            assertTrue("Query [0, 10000) should overlap [1000, 5000]", metricsReader.overlapsTimeRange(0L, 10000L));

            // Test case 2: Query partially overlaps (before)
            assertTrue("Query [500, 2000) should overlap [1000, 5000]", metricsReader.overlapsTimeRange(500L, 2000L));

            // Test case 3: Query partially overlaps (after)
            assertTrue("Query [3000, 6000) should overlap [1000, 5000]", metricsReader.overlapsTimeRange(3000L, 6000L));

            // Test case 4: Query completely inside leaf
            assertTrue("Query [2000, 3000) should overlap [1000, 5000]", metricsReader.overlapsTimeRange(2000L, 3000L));

            // Test case 5: Query starts at leaf's max (edge case - should overlap)
            assertTrue("Query [5000, 6000) should overlap [1000, 5000]", metricsReader.overlapsTimeRange(5000L, 6000L));

            // Test case 6: Query ends at leaf's min (edge case - should NOT overlap)
            assertFalse("Query [0, 1000) should NOT overlap [1000, 5000]", metricsReader.overlapsTimeRange(0L, 1000L));

            // Test case 7: Query completely before leaf
            assertFalse("Query [0, 500) should NOT overlap [1000, 5000]", metricsReader.overlapsTimeRange(0L, 500L));

            // Test case 8: Query completely after leaf
            assertFalse("Query [6000, 7000) should NOT overlap [1000, 5000]", metricsReader.overlapsTimeRange(6000L, 7000L));
        }
    }

    /**
     * Test that reader without metadata (old constructor) always overlaps
     */
    public void testOverlapsTimeRangeWithoutMetadata() throws IOException {
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Create reader without metadata (uses Long.MIN_VALUE and Long.MAX_VALUE)
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY);

            // Should overlap with any reasonable query range
            assertTrue("Reader without metadata should overlap with [0, 1000)", metricsReader.overlapsTimeRange(0L, 1000L));

            assertTrue("Reader without metadata should overlap with [1000, 2000)", metricsReader.overlapsTimeRange(1000L, 2000L));

            assertTrue(
                "Reader without metadata should overlap with [Long.MAX_VALUE-1000, Long.MAX_VALUE)",
                metricsReader.overlapsTimeRange(Long.MAX_VALUE - 1000L, Long.MAX_VALUE)
            );
        }
    }

    /**
     * Test pruning scenario: Multiple leaves with different time ranges
     */
    public void testPruningMultipleLeaves() throws IOException {
        // Create multiple documents with different time ranges
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Simulate 3 leaves with different time ranges
            // Leaf 1: [0, 1000]
            ClosedChunkIndexLeafReader leaf1 = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 0L, 1000L);

            // Leaf 2: [1000, 2000]
            ClosedChunkIndexLeafReader leaf2 = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 1000L, 2000L);

            // Leaf 3: [2000, 3000]
            ClosedChunkIndexLeafReader leaf3 = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 2000L, 3000L);

            // Query for [1500, 2500) - should only match leaf2 and leaf3
            long queryStart = 1500L;
            long queryEnd = 2500L;

            assertFalse("Leaf 1 [0, 1000] should be pruned for query [1500, 2500)", leaf1.overlapsTimeRange(queryStart, queryEnd));

            assertTrue("Leaf 2 [1000, 2000] should NOT be pruned for query [1500, 2500)", leaf2.overlapsTimeRange(queryStart, queryEnd));

            assertTrue("Leaf 3 [2000, 3000] should NOT be pruned for query [1500, 2500)", leaf3.overlapsTimeRange(queryStart, queryEnd));
        }
    }

    /**
     * Test edge case: Query with equal start and end (empty range)
     */
    public void testOverlapsTimeRangeEmptyQuery() throws IOException {
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 1000L, 5000L);

            // Query with start == end (empty range) should not overlap
            assertFalse("Empty query [2000, 2000) should NOT overlap", metricsReader.overlapsTimeRange(2000L, 2000L));
        }
    }

    /**
     * Test edge case: Leaf with single timestamp (min == max)
     */
    public void testOverlapsTimeRangeSingleTimestampLeaf() throws IOException {
        createClosedChunkTestDocuments();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader leafReader = context.reader();

            // Leaf with single timestamp: [2000, 2000]
            ClosedChunkIndexLeafReader metricsReader = new ClosedChunkIndexLeafReader(leafReader, LabelStorageType.BINARY, 2000L, 2000L);

            // Query that includes this timestamp
            assertTrue(
                "Query [2000, 3000) should overlap with single timestamp leaf [2000, 2000]",
                metricsReader.overlapsTimeRange(2000L, 3000L)
            );

            // Query that excludes this timestamp (ends at 2000)
            assertFalse(
                "Query [1000, 2000) should NOT overlap with single timestamp leaf [2000, 2000]",
                metricsReader.overlapsTimeRange(1000L, 2000L)
            );

            // Query after this timestamp
            assertFalse(
                "Query [3000, 4000) should NOT overlap with single timestamp leaf [2000, 2000]",
                metricsReader.overlapsTimeRange(3000L, 4000L)
            );
        }
    }
}
