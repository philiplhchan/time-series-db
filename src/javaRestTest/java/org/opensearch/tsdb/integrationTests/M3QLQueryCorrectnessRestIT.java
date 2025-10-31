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
 * REST integration tests for TSDB M3QL query correctness.
 *
 * This class runs two complementary tests:
 *
 *  E2E_m3ql_query_execution_rest_it.yaml: broad coverage of fetch,
 *       unions, aggregations (min/avg), pipelines (scale/sort/timeshift/perSecond),
 *       and error handling over synthetic data (linear/wave/decreasing/mixed/nulls).
 *
 * functional_correctness_rest_it.yaml: focused
 *       checks of multi-stage pipelines (e.g., transformNull → perSecond → max → timeshift),
 *       label semantics, and correct results of complex queries.

 */
public class M3QLQueryCorrectnessRestIT extends RestTimeSeriesTestFramework {

    private static final String E2E_M3QL_QUERY_EXECUTION_REST_IT = "test_cases/E2E_m3ql_query_execution_rest_it.yaml";
    private static final String FUNCTIONAL_CORRECTNESS_REST_IT = "test_cases/functional_correctness_rest_it.yaml";

    /**
     * Runs the E2E M3QL query execution test suite via REST API.
     * @throws Exception if any test fails
     */
    public void testE2EM3QLQueryExecution() throws Exception {
        initializeTest(E2E_M3QL_QUERY_EXECUTION_REST_IT);
        runBasicTest();
    }

    /**
     * Runs the functional correctness test suite for M3QL queries via REST API.
     * @throws Exception if any test fails
     */
    public void testFunctionalCorrectness() throws Exception {
        initializeTest(FUNCTIONAL_CORRECTNESS_REST_IT);
        runBasicTest();
    }
}
