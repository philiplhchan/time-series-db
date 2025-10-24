/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

/**
 * Example REST integration test for TSDB time series data.
 * Demonstrates the Time Series Testing Framework with REST API testing.
 *
 * <p>This test validates:
 * <ul>
 *   <li>Index creation with TSDB engine</li>
 *   <li>Time series data ingestion via REST API</li>
 *   <li>M3QL query execution via REST API</li>
 *   <li>Response validation against expected results</li>
 * </ul>
 *
 * <p>The test configuration (data, queries, expectations) is defined in the
 * YAML file: test_cases/tsdb_rest_it_example.yaml
 */
public class TSDBRestIT extends RestTimeSeriesTestFramework {

    private static final String TEST_YAML_PATH = "test_cases/tsdb_rest_it_example.yaml";

    /**
     * Test basic M3QL query execution via REST API.
     * The YAML configuration defines:
     * <ul>
     *   <li>Input time series data with HTTP request metrics</li>
     *   <li>M3QL queries for aggregation by method and status</li>
     *   <li>Expected results for validation</li>
     * </ul>
     *
     * @throws Exception If the test fails
     */
    public void testSimpleTSDBQuery() throws Exception {
        initializeTest(TEST_YAML_PATH);
        runBasicTest();
    }
}
