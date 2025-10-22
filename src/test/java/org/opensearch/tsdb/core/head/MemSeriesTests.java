/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.Constants;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MemSeriesTests extends OpenSearchTestCase {

    public void testAppend() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);

        ChunkOptions options = new ChunkOptions(8000, 8);

        // Append samples up to 25% of samplesPerChunk (2 samples)
        long timestamp1 = 1000L;
        double value1 = 10.0;
        boolean created1 = series.append(0, timestamp1, value1, options);

        long timestamp2 = 2000L;
        double value2 = 20.0;
        boolean created2 = series.append(0, timestamp2, value2, options);

        // Verify that the first two appends didn't create a new chunk
        assertTrue(created1);
        assertFalse(created2);
        assertEquals(timestamp1, series.getHeadChunk().getMinTimestamp());
        assertEquals(timestamp2, series.getHeadChunk().getMaxTimestamp());
        assertEquals(2, series.getHeadChunk().getChunk().numSamples());

        // Append more samples to reach samplesPerChunk
        for (int i = 3; i <= 8; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            boolean created = series.append(0, timestamp, value, options);

            // The 9th sample should create a new chunk because we reached samplesPerChunk
            if (i == 8) {
                assertTrue(created);
                assertEquals(8000L, series.getHeadChunk().getMinTimestamp());
                assertEquals(8000L, series.getHeadChunk().getMaxTimestamp());
                assertEquals(1, series.getHeadChunk().getChunk().numSamples());
            } else {
                assertFalse(created);
            }
        }
    }

    public void testGetClosableChunksHeadExpiration() {
        MemSeries series = new MemSeries(123L, ByteLabels.fromStrings("k1", "v1", "k2", "v2"));
        MemSeries.ClosableChunkResult result = series.getClosableChunks(2000L);
        assertTrue(result.closableChunks().isEmpty());
        assertEquals(Long.MAX_VALUE, result.minSeqNo());

        ChunkOptions options = new ChunkOptions(8000, 8);

        series.append(10, 1000L, 10.0, options);
        series.append(11, 2000L, 20.0, options);

        // cannot close the head chunk if it's modified recently
        result = series.getClosableChunks(2000L);
        assertTrue(result.closableChunks().isEmpty());
        assertEquals(10, result.minSeqNo());

        // can close the head chunk if its hasn't been modified for a long time
        result = series.getClosableChunks(2000L + Constants.Time.DEFAULT_CHUNK_EXPIRY + 1);
        assertEquals(1, result.closableChunks().size());
        assertEquals(Long.MAX_VALUE, result.minSeqNo());
    }

    public void testGetClosableChunksHead() {
        MemSeries series = createMemSeries(3); // creates 3 chunks with 8 samples each
        assertEquals("3 chunks created", 3, series.getHeadChunk().len());

        MemSeries.ClosableChunkResult result = series.getClosableChunks(25000L);
        assertEquals(2, result.closableChunks().size());
        assertEquals(16, result.minSeqNo());
        assertFalse("head chunk is not closable", result.closableChunks().contains(series.getHeadChunk()));
    }

    public void testGetClosableChunksHeadWithOutOfOrderSeqNoIngestion() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);
        ChunkOptions options = new ChunkOptions(8000, 8);

        // Ingest samples in decreasing seqNo.
        for (int i = 12; i > 0; i--) {
            long timestamp = (12 - i) * 1000L;
            double value = i * 10.0;
            series.append(i, timestamp, value, options);
        }

        var result = series.getClosableChunks(10000L);
        assertEquals(1, result.closableChunks().size());
        assertEquals(1, result.minSeqNo());
    }

    public void testDropChunks() {
        MemSeries series = createMemSeries(3); // creates 3 chunks with 8 samples each

        MemChunk first = series.getHeadChunk().oldest();
        MemChunk second = first.getNext();
        MemChunk third = second.getNext();

        Set<MemChunk> toDelete = Set.of(first, second);
        series.dropClosedChunks(toDelete);
        assertEquals("only one chunk should remain", 1, series.getHeadChunk().len());
        assertEquals("the remaining chunk should be the third one", third, series.getHeadChunk());

        series = createMemSeries(3);
        first = series.getHeadChunk().oldest();
        second = first.getNext();
        third = second.getNext();

        toDelete = Set.of(first, third);
        series.dropClosedChunks(toDelete);
        assertEquals("only one chunk should remain", 1, series.getHeadChunk().len());
        assertEquals("the remaining chunk should be the second one", second, series.getHeadChunk().oldest());
        assertNull(series.getHeadChunk().getNext());
        assertNull(series.getHeadChunk().getPrev());

        series = createMemSeries(3);
        first = series.getHeadChunk().oldest();
        second = first.getNext();
        third = second.getNext();
        toDelete = Set.of(second);
        series.dropClosedChunks(toDelete);
        assertEquals("two chunks should remain", 2, series.getHeadChunk().len());
        assertEquals("the oldest chunk should be the first one", first, series.getHeadChunk().oldest());
        assertEquals("the newest chunk should be the third one", third, series.getHeadChunk());
        assertEquals("first chunk next now points to the third chunk", first.getNext(), third);
        assertEquals("third chunk prev now points to the first chunk", third.getPrev(), first);

        series = createMemSeries(3);
        first = series.getHeadChunk().oldest();
        second = first.getNext();
        third = second.getNext();
        toDelete = Set.of(first, second, third);
        series.dropClosedChunks(toDelete);
        assertNull("no chunks should remain", series.getHeadChunk());
    }

    public void testGettersAndSetters() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long reference = 123L;
        MemSeries series = new MemSeries(reference, labels);

        // Test getters for constructor parameters
        assertEquals(reference, series.getReference());
        assertEquals(labels, series.getLabels());

        // Test initial state of optional fields
        assertNull(series.getHeadChunk());
        assertEquals(0L, series.getMaxMMapTimestamp());

        // Test setMaxMMapTimestamp and getMaxMMapTimestamp
        long testTimestamp = 12345L;
        series.setMaxMMapTimestamp(testTimestamp);
        assertEquals(testTimestamp, series.getMaxMMapTimestamp());

        // Test getHeadChunk after appending data
        ChunkOptions options = new ChunkOptions(8000, 8);
        series.append(0, 1000L, 10.0, options);
        assertNotNull(series.getHeadChunk());
        assertEquals(1000L, series.getHeadChunk().getMinTimestamp());
        assertEquals(1000L, series.getHeadChunk().getMaxTimestamp());
    }

    public void testIsOOO() {
        MemSeries seriesWithNoChunks = new MemSeries(123L, ByteLabels.fromStrings("k1", "v1", "k2", "v2"));
        assertFalse(seriesWithNoChunks.isOOO(1000L));

        MemSeries seriesWithChunks = createMemSeries(2);
        assertFalse(seriesWithChunks.isOOO(16000L));
        assertTrue(seriesWithChunks.isOOO(14000L));
    }

    public void testChunkCreationOnTimeRange() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);

        // Use a small chunkRange to test time-based chunk creation
        ChunkOptions options = new ChunkOptions(5000, 100); // 5 second chunk range, high sample count

        // Add first sample at timestamp 1000
        boolean created1 = series.append(0, 1000L, 10.0, options);
        assertTrue("First sample should create a chunk", created1);
        MemChunk firstChunk = series.getHeadChunk();

        // Add samples within the time range (should not create new chunk)
        boolean created2 = series.append(1, 3000L, 20.0, options);
        boolean created3 = series.append(2, 4999L, 30.0, options);
        assertFalse("Sample within time range should not create chunk", created2);
        assertFalse("Sample at edge of time range should not create chunk", created3);
        assertEquals(firstChunk, series.getHeadChunk());

        // Add sample that exceeds the time range (should create new chunk)
        boolean created4 = series.append(3, 6000L, 40.0, options);
        assertTrue("Sample beyond time range should create new chunk", created4);
        assertNotEquals("Should have a new head chunk", firstChunk, series.getHeadChunk());
        assertEquals(6000L, series.getHeadChunk().getMinTimestamp());
    }

    public void testAppendVariableRate() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);

        ChunkOptions options = new ChunkOptions(1000000L, 8);

        // first 25% of samples widely spaced, predict the end
        series.append(0, 1000L, 10.0, options);
        series.append(1, 2000L, 10.0, options);

        // next samples at different step size, try to maintain chunk boundary
        for (int i = 2; i < 16; i++) {
            series.append(i, 2000L + i * 100L, 10.0, options);
        }
        assertEquals("Should still be using the same chunk", 1, series.getHeadChunk().len());

        // do not allow chunks to be too large, chunk can never contain more than 2 * samplesPerChunk
        series.append(16, 3600L, 10.0, options);
        assertEquals("Should have created a new chunk after exceeding max samples", 2, series.getHeadChunk().len());
    }

    public void testPreventSmallChunks() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);

        ChunkOptions options = new ChunkOptions(9000, 8);

        for (int i = 0; i < 9; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            series.append(i, timestamp, value, options);
        }

        // Two relatively evenly sized chunks, despite only the 9th sample being outside the chunk range of the first chunk
        assertEquals("Two chunks", 2, series.getHeadChunk().len());
        assertEquals("First chunk has 5 samples", 5, series.getHeadChunk().oldest().getChunk().numSamples());
        assertEquals("Second chunk has 4 samples", 4, series.getHeadChunk().getChunk().numSamples());
    }

    public void testAwaitPersistedAndMarkPersisted() throws InterruptedException {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);
        final AtomicBoolean threadCompleted = new AtomicBoolean(false);
        final AtomicReference<Exception> threadException = new AtomicReference<>();

        Thread waitingThread = new Thread(() -> {
            try {
                series.awaitPersisted();
                threadCompleted.set(true);
            } catch (InterruptedException e) {
                threadException.set(e);
            }
        });
        waitingThread.start();
        waitUntil(() -> waitingThread.getState() == Thread.State.WAITING);
        assertFalse("Thread should be blocked before markPersisted is called", threadCompleted.get());

        // Mark persisted
        series.markPersisted();

        waitingThread.join(1000);
        assertTrue("Thread should complete after markPersisted is called", threadCompleted.get());
        assertNull("Thread should not have thrown an InterruptedException", threadException.get());

        waitUntil(() -> {
            try {
                series.awaitPersisted();
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        });
    }

    private MemSeries createMemSeries(int chunkCount) {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels);

        ChunkOptions options = new ChunkOptions(8000, 8);

        for (int i = 0; i < chunkCount * 8; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            series.append(i, timestamp, value, options);
        }
        return series;
    }
}
