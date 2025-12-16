/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

public interface MemSeriesReader {

    /**
     * Gets the MemSeries associated with the given reference.
     *
     * @param reference the reference to lookup MemSeries for
     * @return the MemSeries for the reference
     */
    MemSeries getMemSeries(long reference);
}
