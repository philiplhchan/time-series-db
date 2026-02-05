/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.nio.charset.StandardCharsets;

/**
 * Reads TSDB documents from Arrow columnar vectors.
 * This class assumes data arrives via Arrow Flight as pre-populated vectors.
 *
 * <p>With native Arrow Flight, labels are pre-split on the client side as a ListVector
 * of VarChar elements (key-value pairs), avoiding String.split() overhead on the server.
 *
 * <p>This class is NOT thread-safe. Use per-thread instances or thread-local.
 */
public class ArrowBulkDecoder implements AutoCloseable {

    private final BufferAllocator allocator;

    // Arrow vectors for reading
    private ListVector labelKVPairsVector;  // List<VarChar> for pre-split label key-value pairs
    private BigIntVector timestampsVector;
    private Float8Vector valuesVector;

    private int currentBatchSize;
    private int capacity;

    /**
     * Creates an ArrowBulkDecoder with default initial capacity of 1024.
     */
    public ArrowBulkDecoder() {
        this(1024);
    }

    /**
     * Creates an ArrowBulkDecoder with specified initial capacity.
     *
     * @param initialCapacity the initial capacity for Arrow vectors
     */
    public ArrowBulkDecoder(int initialCapacity) {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.capacity = initialCapacity;
        initializeVectors();
    }

    private void initializeVectors() {
        labelKVPairsVector = ListVector.empty("label_kv_pairs", allocator);
        labelKVPairsVector.addOrGetVector(
            org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.pojo.ArrowType.Utf8.INSTANCE)
        );

        timestampsVector = new BigIntVector("timestamp", allocator);
        valuesVector = new Float8Vector("value", allocator);

        timestampsVector.allocateNew(capacity);
        valuesVector.allocateNew(capacity);
    }

    /**
     * Get the allocator for this decoder.
     *
     * @return the buffer allocator
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Get the label key-value pairs vector for direct population from Arrow Flight.
     *
     * @return the label KV pairs ListVector
     */
    public ListVector getLabelKVPairsVector() {
        return labelKVPairsVector;
    }

    /**
     * Get the timestamps vector for direct population from Arrow Flight.
     *
     * @return the timestamps vector
     */
    public BigIntVector getTimestampsVector() {
        return timestampsVector;
    }

    /**
     * Get the values vector for direct population from Arrow Flight.
     *
     * @return the values vector
     */
    public Float8Vector getValuesVector() {
        return valuesVector;
    }

    /**
     * Populate vectors directly with data (simulating Arrow Flight receive).
     * Labels are stored as ListVector of VarChar (pre-split key-value pairs).
     *
     * @param docCount number of documents in the batch
     * @param labelKVPairs array of pre-split label key-value pairs per document
     * @param timestamps array of timestamp values
     * @param values array of sample values
     */
    public void populateFromArrowFlight(int docCount, String[][] labelKVPairs, long[] timestamps, double[] values) {
        // Ensure capacity for simple vectors
        if (docCount > capacity) {
            timestampsVector.reAlloc();
            valuesVector.reAlloc();
            capacity = Math.max(docCount, capacity * 2);
        }

        // Populate ListVector with label key-value pairs using UnionListWriter
        UnionListWriter listWriter = labelKVPairsVector.getWriter();
        for (int i = 0; i < docCount; i++) {
            listWriter.setPosition(i);
            listWriter.startList();
            if (labelKVPairs[i] != null) {
                for (String kv : labelKVPairs[i]) {
                    byte[] bytes = kv.getBytes(StandardCharsets.UTF_8);
                    org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(bytes.length);
                    buf.setBytes(0, bytes);
                    listWriter.varChar().writeVarChar(0, bytes.length, buf);
                    buf.close();
                }
            }
            listWriter.endList();

            timestampsVector.setSafe(i, timestamps[i]);
            valuesVector.setSafe(i, values[i]);
        }

        labelKVPairsVector.setValueCount(docCount);
        timestampsVector.setValueCount(docCount);
        valuesVector.setValueCount(docCount);
        currentBatchSize = docCount;
    }

    /**
     * Get the parsed Labels at the specified index.
     * Reads from the ListVector of pre-split key-value pairs.
     *
     * @param index the document index
     * @return the Labels object or null if not present
     */
    public Labels getLabels(int index) {
        if (labelKVPairsVector.isNull(index)) return null;

        int start = labelKVPairsVector.getElementStartIndex(index);
        int end = labelKVPairsVector.getElementEndIndex(index);
        int size = end - start;

        if (size == 0) return null;

        VarCharVector dataVector = (VarCharVector) labelKVPairsVector.getDataVector();
        String[] kvPairs = new String[size];
        for (int i = 0; i < size; i++) {
            byte[] bytes = dataVector.get(start + i);
            kvPairs[i] = new String(bytes, StandardCharsets.UTF_8);
        }

        return ByteLabels.fromStrings(kvPairs);
    }

    /**
     * Get the timestamp at the specified index.
     *
     * @param index the document index
     * @return the timestamp value
     */
    public long getTimestamp(int index) {
        return timestampsVector.get(index);
    }

    /**
     * Get the value at the specified index.
     *
     * @param index the document index
     * @return the sample value
     */
    public double getValue(int index) {
        return valuesVector.get(index);
    }

    /**
     * Create TSDBDocument for a given index.
     *
     * @param index the document index
     * @return a new TSDBDocument instance
     */
    public TSDBDocument getTSDBDocument(int index) {
        return new TSDBDocument(getLabels(index), getTimestamp(index), getValue(index), null, null);
    }

    /**
     * Get the current batch size.
     *
     * @return the number of documents in the current batch
     */
    public int getBatchSize() {
        return currentBatchSize;
    }

    /**
     * Reset vectors for next batch (reuses memory).
     */
    public void reset() {
        labelKVPairsVector.reset();
        timestampsVector.reset();
        valuesVector.reset();
        currentBatchSize = 0;
    }

    /**
     * Get the amount of memory currently allocated by this decoder.
     *
     * @return allocated memory in bytes
     */
    public long getAllocatedMemory() {
        return allocator.getAllocatedMemory();
    }

    @Override
    public void close() {
        labelKVPairsVector.close();
        timestampsVector.close();
        valuesVector.close();
        allocator.close();
    }
}
