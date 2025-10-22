/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MemSeries is an in-memory representation of a series. MemSeries manages the lifecycle of in-memory chunks.
 */
public class MemSeries {
    /**
     * Series reference. This is a hash, but may differ from labels.stableHash() if the hash function changes between versions.
     * The value is a hash created by the hash function used when the series was created.
     */
    private final long reference;

    // Series Labels
    private final Labels labels;

    // Lock generally should be held when using any vars defined below this point
    private final ReentrantLock seriesLock = new ReentrantLock();

    // A doubly linked list of chunks in memory, points to the most recent chunk.
    private MemChunk headChunk;

    // Max timestamp of an already indexed chunk, this is NOT updated atomically when chunks are indexed.
    private long maxMMapTimestamp;

    // The timestamp at which to cut the next chunk
    private long nextAt;

    // Appender for the head chunk
    private ChunkAppender chunkAppender;

    // Indicates if the series labels have been written to the translog
    private final CountDownLatch firstWriteLatch = new CountDownLatch(1);

    // The seqNo corresponding to the most recent operation appended to this series
    private long maxSeqNo;

    /**
     * Constructs a new MemSeries instance.
     * @param reference the unique series reference ID
     * @param labels the series labels
     */
    public MemSeries(long reference, Labels labels) {
        this.reference = reference;
        this.labels = labels;
    }

    /**
     * Get the series labels.
     * @return the series labels
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * Get the unique series reference ID.
     * @return the series reference ID
     */
    public long getReference() {
        return reference;
    }

    /**
     * Append an in order sample to the series. The series lock should be held when calling this method
     * @param seqNo the sequence number for ordering
     * @param timestamp the sample timestamp
     * @param value the sample value
     * @param options the chunk options
     *
     * @return true if a new chunk was created, false otherwise
     */
    public boolean append(long seqNo, long timestamp, double value, ChunkOptions options) {
        boolean created = appendPreprocessor(seqNo, timestamp, Encoding.XOR, options);
        chunkAppender.append(timestamp, value);
        headChunk.setMaxTimestamp(timestamp);
        if (seqNo > this.maxSeqNo) {
            this.maxSeqNo = seqNo;
        }

        if (seqNo < headChunk.getMinSeqNo()) {
            headChunk.setMinSeqNo(seqNo);
        }
        return created;
    }

    /**
     * Checks if the given timestamp is out of order (earlier than the max timestamp of the head chunk).
     * @param t the timestamp to check
     * @return true if the timestamp is out of order, false otherwise
     */
    public boolean isOOO(long t) {
        return headChunk != null && t < headChunk.getMaxTimestamp();
    }

    /**
     * Get the head chunk of the series.
     * @return the head chunk
     */
    public MemChunk getHeadChunk() {
        return headChunk;
    }

    /**
     * Get the maximum timestamp of the mmaped chunks.
     * @return the maximum mmap timestamp
     */
    public long getMaxMMapTimestamp() {
        return maxMMapTimestamp;
    }

    /**
     * Set the maximum timestamp of the mmaped chunks.
     * @param maxMMapTimestamp the maximum mmap timestamp to set
     */
    public void setMaxMMapTimestamp(long maxMMapTimestamp) {
        this.maxMMapTimestamp = maxMMapTimestamp;
    }

    /**
     * Identify chunks that can be closed and memory-mapped.
     *
     * @param highWatermarkTimestamp the largest timestamp seen during ingestion so far
     * @return ClosableChunkResult contains a list of closable chunks, and the smallest seqNo of any sample still in-memory for the series
     */
    public ClosableChunkResult getClosableChunks(long highWatermarkTimestamp) {
        lock();
        try {
            List<MemChunk> closableChunks = new ArrayList<>();
            MemChunk headChunk = this.headChunk;
            if (headChunk == null) {
                return new ClosableChunkResult(closableChunks, Long.MAX_VALUE); // nothing to map, no samples in memory
            }

            // all chunks but the head chunk can always be closed, as they will not receive any more samples
            MemChunk prevChunk = headChunk.getPrev();
            while (prevChunk != null) {
                closableChunks.add(prevChunk);
                prevChunk = prevChunk.getPrev();
            }

            // if the head chunk hasn't been updated for a long time (based on high watermark of ingested samples), close it
            long minSeqNo = headChunk.getMinSeqNo();
            if (highWatermarkTimestamp - headChunk.getMaxTimestamp() > Constants.Time.DEFAULT_CHUNK_EXPIRY) {
                closableChunks.add(headChunk);
                minSeqNo = Long.MAX_VALUE;
            }

            return new ClosableChunkResult(closableChunks, minSeqNo);
        } finally {
            unlock();
        }
    }

    /**
     * Result of identifying chunks that can be closed.
     *
     * @param closableChunks list of chunks that are ready to be closed and compressed
     * @param minSeqNo       the minimum sequence number among the closable chunks
     */
    public record ClosableChunkResult(List<MemChunk> closableChunks, long minSeqNo) {
    }

    /**
     * Drops the specified closed chunks from the series.
     * @param closedChunks the set of closed chunks to drop
     */
    public void dropClosedChunks(Set<MemChunk> closedChunks) {
        lock();
        try {
            MemChunk curr = headChunk;
            while (curr != null) {
                if (closedChunks.contains(curr)) {
                    if (curr.getPrev() != null) {
                        curr.getPrev().setNext(curr.getNext());
                    }

                    if (curr.getNext() != null) {
                        curr.getNext().setPrev(curr.getPrev());
                    }

                    if (curr == headChunk) {
                        headChunk = curr.getPrev();
                    }
                }
                curr = curr.getPrev();
            }
        } finally {
            unlock();
        }
    }

    /**
     * Lock the series for thread-safe operations.
     */
    public void lock() {
        seriesLock.lock();
    }

    /**
     * Unlock the series after thread-safe operations.
     */
    public void unlock() {
        seriesLock.unlock();
    }

    private boolean appendPreprocessor(long seqNo, long timestamp, Encoding encoding, ChunkOptions options) {
        boolean created = false;
        if (headChunk == null) {
            // TODO: when supporting out-of-order samples, we need to create a special chunk
            headChunk = createHeadChunk(seqNo, timestamp, encoding, options.chunkRange());
            created = true;
        }

        MemChunk chunk = headChunk;

        int numSamples = chunk.getChunk().numSamples();
        if (numSamples == 0) {
            // a new chunk
            chunk.setMinTimestamp(timestamp);
            this.nextAt = rangeForTimestamp(timestamp, options.chunkRange());
        }

        // If we reach 25% of chunk's target sample count, predict an end time for this chunk to equally distribute samples
        // in the remaining chunks for this chunk range. The prediction assumes future behavior will mimic past behavior within
        // the current chunk. Subsequent chunks will be predicted when they reach 25% full as well, allowing for dynamic
        // adjustment as sample rates change and resilience against momentary irregularities such as dropped samples.
        if (numSamples == options.samplesPerChunk() / 4) {
            this.nextAt = computeChunkEndTime(chunk.getMinTimestamp(), chunk.getMaxTimestamp(), this.nextAt, 4);
        }

        // Cut if the timestamp is larger than the nextAt or if we have too many samples
        if (timestamp >= this.nextAt || numSamples >= options.samplesPerChunk() * 2) {
            createHeadChunk(seqNo, timestamp, encoding, options.chunkRange());
            created = true;
        }
        return created;
    }

    /**
     * Estimate end timestamp based on beginning timestamp, current timestamp, and upper bound end timestamp. Assumes this
     * is called when chunk is 1 / ratio full.
     *
     * @return the estimated end timestamp for the chunk, strictly less than or equal to nextAt
     */
    private long computeChunkEndTime(long minTimestamp, long maxTimestamp, long nextAt, int ratio) {
        double n = (double) (nextAt - minTimestamp) / ((double) (maxTimestamp - minTimestamp + 1) * ratio);
        if (n <= 1) {
            return nextAt;
        }
        return (long) (minTimestamp + (nextAt - minTimestamp) / Math.floor(n));
    }

    private MemChunk createHeadChunk(long seqNo, long minTime, Encoding encoding, long chunkRange) {
        MemChunk chunk = new MemChunk(seqNo, minTime, Long.MIN_VALUE, headChunk);

        switch (encoding) {
            case XOR:
                chunk.setChunk(new XORChunk());
                break;
        }

        this.headChunk = chunk;
        this.nextAt = rangeForTimestamp(minTime, chunkRange);
        this.chunkAppender = chunk.getChunk().appender();
        return chunk;
    }

    /**
     * Calculates the end timestamp for the given timestamp based on the chunk range.
     */
    private long rangeForTimestamp(long t, long chunkRange) {
        return (t / chunkRange) * chunkRange + chunkRange;
    }

    /**
     * Wait until the series is marked as persisted. Should only be called by threads other which did not create the series.
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void awaitPersisted() throws InterruptedException {
        firstWriteLatch.await();
    }

    /**
     * Mark the series as persisted. Should only be called by the thread which created the series, after receiving a translog location.
     */
    public void markPersisted() {
        firstWriteLatch.countDown();
    }

    /**
     * Get the maximum sequence number for this series.
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return maxSeqNo;
    }

    /**
     * Set the maximum sequence number for this series.
     * @param maxSeqNo the maximum sequence number to set
     */
    public void setMaxSeqNo(long maxSeqNo) {
        this.maxSeqNo = maxSeqNo;
    }
}
