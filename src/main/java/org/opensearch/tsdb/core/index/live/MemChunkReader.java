/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.opensearch.tsdb.core.head.MemChunk;

import java.util.List;

/**
 * Interface for reading memory chunks by reference.
 */
public interface MemChunkReader {

    /**
     * Gets chunks associated with the given reference.
     *
     * @param reference the reference to lookup chunks for
     * @return list of chunks for the reference
     */
    List<MemChunk> getChunks(long reference);
}
