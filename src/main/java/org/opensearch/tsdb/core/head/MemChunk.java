/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.tsdb.core.chunk.Chunk;

/**
 * MemChunk represents time series data stored in memory and open for writes. A MemChunk is an element in a doubly linked list.
 * <p>
 * Note that this object is not designed to be thread-safe and synchronization is left to the caller.
 * </p>
 */
public class MemChunk {
    private long minSeqNo; // minimum sequence number corresponding to a sample in this chunk
    private Chunk chunk;
    private long minTimestamp;
    private long maxTimestamp;
    private MemChunk prev; // link to the previous chunk on the linked list
    private MemChunk next; // link to the next chunk on the linked list

    /**
     * Constructs a new MemChunk instance.
     * @param seqNo the sequence number for ordering
     * @param minTimestamp the minimum timestamp in the chunk
     * @param maxTimestamp the maximum timestamp in the chunk
     * @param prev the previous MemChunk in the linked list, or null if this is the first chunk
     */
    public MemChunk(long seqNo, long minTimestamp, long maxTimestamp, MemChunk prev) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minSeqNo = seqNo;
        this.prev = prev;
        if (prev != null) {
            prev.next = this;
        }
    }

    /**
     * Returns the length of the linked list starting from this MemChunk.
     * @return the number of MemChunk elements in the linked list
     */
    public int len() {
        int count = 0;
        MemChunk elem = this;
        while (elem != null) {
            count++;
            elem = elem.prev;
        }
        return count;
    }

    /**
     * Returns the oldest MemChunk in the linked list.
     * @return the oldest MemChunk element
     */
    public MemChunk oldest() {
        MemChunk elem = this;
        while (elem.prev != null) {
            elem = elem.prev;
        }
        return elem;
    }

    /**
     * Returns the MemChunk at the specified offset from this chunk.
     * @param offset the offset from this chunk (0 for this chunk, 1 for previous, etc.)
     * @return the MemChunk at the specified offset, or null if the offset is out of bounds
     */
    public MemChunk atOffset(int offset) {
        if (offset < 0) {
            return null;
        }
        MemChunk elem = this;
        for (int i = 0; i < offset; i++) {
            if (elem == null) {
                return null;
            }
            elem = elem.prev;
        }
        return elem;
    }

    /**
     * Returns the underlying Chunk instance.
     * @return the Chunk instance
     */
    public Chunk getChunk() {
        return chunk;
    }

    /**
     * Sets the underlying Chunk instance.
     * @param chunk the Chunk instance to set
     */
    public void setChunk(Chunk chunk) {
        this.chunk = chunk;
    }

    /**
     * Returns the minimum timestamp in the chunk.
     * @return the minimum timestamp
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Sets the minimum timestamp in the chunk.
     * @param timestamp the minimum timestamp to set
     */
    public void setMinTimestamp(long timestamp) {
        this.minTimestamp = timestamp;
    }

    /**
     * Returns the maximum timestamp in the chunk.
     * @return the maximum timestamp
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Sets the maximum timestamp in the chunk.
     * @param timestamp the maximum timestamp to set
     */
    public void setMaxTimestamp(long timestamp) {
        this.maxTimestamp = timestamp;
    }

    /**
     * Returns the previous MemChunk in the linked list.
     * @return the previous MemChunk, or null if this is the first chunk
     */
    public MemChunk getPrev() {
        return prev;
    }

    /**
     * Returns the next MemChunk in the linked list.
     * @return the next MemChunk, or null if this is the last chunk
     */
    public MemChunk getNext() {
        return next;
    }

    /**
     * Sets the next MemChunk in the linked list.
     * @param chunk the next MemChunk to set
     */
    public void setNext(MemChunk chunk) {
        this.next = chunk;
    }

    /**
     * Sets the previous MemChunk in the linked list.
     * @param chunk the previous MemChunk to set
     */
    public void setPrev(MemChunk chunk) {
        this.prev = chunk;
    }

    /**
     * Sets  minimum sequence number in the chunk.
     * @param seqNo minimum sequence number in the chunk.
     */
    public void setMinSeqNo(long seqNo) {
        this.minSeqNo = seqNo;
    }

    /**
     * Returns the minimum sequence number in the chunk.
     * @return the minimum sequence number
     */
    public long getMinSeqNo() {
        return minSeqNo;
    }
}
