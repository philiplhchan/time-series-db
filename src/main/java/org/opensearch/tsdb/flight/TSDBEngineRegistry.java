/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.flight;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.TSDBEngine;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for TSDBEngine instances to enable direct access from Arrow Flight producer.
 * This is a POC pattern - production would use proper OpenSearch service discovery.
 */
public final class TSDBEngineRegistry {

    private static final Logger logger = LogManager.getLogger(TSDBEngineRegistry.class);
    private static final TSDBEngineRegistry INSTANCE = new TSDBEngineRegistry();

    private final ConcurrentHashMap<String, TSDBEngine> engines = new ConcurrentHashMap<>();

    private TSDBEngineRegistry() {
        // Singleton
    }

    /**
     * Get the singleton registry instance.
     */
    public static TSDBEngineRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Register a TSDBEngine for an index.
     *
     * @param indexName the index name
     * @param engine    the TSDBEngine instance
     */
    public void register(String indexName, TSDBEngine engine) {
        TSDBEngine previous = engines.put(indexName, engine);
        if (previous != null) {
            logger.warn("Replaced existing engine for index [{}]", indexName);
        } else {
            logger.info("Registered TSDBEngine for index [{}]", indexName);
        }
    }

    /**
     * Unregister a TSDBEngine for an index.
     *
     * @param indexName the index name
     */
    public void unregister(String indexName) {
        TSDBEngine removed = engines.remove(indexName);
        if (removed != null) {
            logger.info("Unregistered TSDBEngine for index [{}]", indexName);
        }
    }

    /**
     * Get the TSDBEngine for an index.
     *
     * @param indexName the index name
     * @return the TSDBEngine, or null if not found
     */
    public TSDBEngine get(String indexName) {
        return engines.get(indexName);
    }

    /**
     * Check if an engine is registered for an index.
     *
     * @param indexName the index name
     * @return true if registered
     */
    public boolean contains(String indexName) {
        return engines.containsKey(indexName);
    }

    /**
     * Get the number of registered engines.
     */
    public int size() {
        return engines.size();
    }

    /**
     * Clear all registered engines.
     * For testing purposes only.
     */
    public void clear() {
        engines.clear();
        logger.info("Cleared all registered engines");
    }
}
