/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

/**
 * Common constants used across the REST test framework.
 */
public final class Common {

    // HTTP Status Codes
    public static final int HTTP_OK = 200;
    public static final int HTTP_CREATED = 201;

    // Common Endpoints
    public static final String ENDPOINT_DOC = "/_doc";
    public static final String ENDPOINT_BULK = "/_bulk";
    public static final String ENDPOINT_REFRESH = "/_refresh";

    // Field Names - Query/Response
    public static final String FIELD_QUERY = "query";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_DATA = "data";
    public static final String FIELD_RESULT_TYPE = "resultType";
    public static final String FIELD_RESULT = "result";
    public static final String FIELD_METRIC = "metric";
    public static final String FIELD_VALUES = "values";
    public static final String FIELD_ALIAS = "alias";

    // Error Messages
    public static final String UNKNOWN_ERROR = "Unknown error";

    private Common() {
        // Utility class, prevent instantiation
    }
}
