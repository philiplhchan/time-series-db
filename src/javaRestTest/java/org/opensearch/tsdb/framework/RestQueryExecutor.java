/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper.TimeSeriesResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.rest.RestRequest;

import static org.opensearch.tsdb.framework.Common.FIELD_DATA;
import static org.opensearch.tsdb.framework.Common.FIELD_METRIC;
import static org.opensearch.tsdb.framework.Common.FIELD_QUERY;
import static org.opensearch.tsdb.framework.Common.FIELD_RESULT;
import static org.opensearch.tsdb.framework.Common.FIELD_RESULT_TYPE;
import static org.opensearch.tsdb.framework.Common.FIELD_STATUS;
import static org.opensearch.tsdb.framework.Common.FIELD_VALUES;
import static org.opensearch.tsdb.framework.Common.UNKNOWN_ERROR;

/**
 * REST-based query executor for time series queries (M3QL/PromQL).
 * Extends BaseQueryExecutor to provide REST API execution capabilities.
 *
 * <p>This executor:
 * <ul>
 *   <li>Converts QueryConfig to REST API requests</li>
 *   <li>Executes queries via the appropriate REST endpoint (/_m3ql or /_promql)</li>
 *   <li>Parses Prometheus-format responses into PromMatrixResponse</li>
 *   <li>Delegates validation to BaseQueryExecutor</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class RestQueryExecutor extends BaseQueryExecutor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String RESULT_TYPE_MATRIX = "matrix";
    private static final String QUERY_PARAM_FORMAT = "%s?start=%d&end=%d&step=%d";

    private final RestClient restClient;

    public RestQueryExecutor(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * Execute a query via the REST API.
     *
     * @param queryConfig The query configuration
     * @param indexName   The index name to query (passed as partitions parameter)
     * @return The Prometheus matrix response
     * @throws Exception If query execution fails
     */
    @Override
    protected PromMatrixResponse executeQuery(QueryConfig queryConfig, String indexName) throws Exception {
        if (queryConfig == null) {
            throw new IllegalArgumentException("QueryConfig cannot be null");
        }

        Request request = buildRequest(queryConfig, indexName);

        try {
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            return parseResponse(responseBody);
        } catch (ResponseException e) {
            String errorBody = extractErrorBody(e);
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Query execution failed: status=%d, error=%s",
                    e.getResponse().getStatusLine().getStatusCode(),
                    errorBody
                ),
                e
            );
        }
    }

    /**
     * Build REST API request from query configuration.
     *
     * @param queryConfig The query configuration
     * @return The configured REST request
     * @throws IOException If request building fails
     */
    private Request buildRequest(QueryConfig queryConfig, String indexName) throws IOException {
        long startMillis = queryConfig.config().minTimestamp().toEpochMilli();
        long endMillis = queryConfig.config().maxTimestamp().toEpochMilli();
        long stepMillis = queryConfig.config().step().toMillis();

        String endpoint = queryConfig.type().getRestEndpoint();
        String url = String.format(Locale.ROOT, QUERY_PARAM_FORMAT, endpoint, startMillis, endMillis, stepMillis);

        // Add partitions parameter with the index name for better performance and test isolation
        // Note: This is optional - without it, OpenSearch searches all indices
        if (indexName != null && !indexName.isEmpty()) {
            url = url + "&partitions=" + indexName;
        }

        Request request = new Request(RestRequest.Method.POST.name(), url);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field(FIELD_QUERY, queryConfig.query()).endObject();
            request.setJsonEntity(builder.toString());
        }

        return request;
    }

    /**
     * Parse Prometheus-format response into PromMatrixResponse.
     *
     * @param responseBody The JSON response body
     * @return The parsed response
     * @throws IOException If parsing fails
     */
    private PromMatrixResponse parseResponse(String responseBody) throws IOException {
        Map<String, Object> responseMap = OBJECT_MAPPER.readValue(responseBody, Map.class);

        String status = (String) responseMap.get(FIELD_STATUS);
        if (status == null) {
            throw new IOException("Response missing 'status' field");
        }

        Map<String, Object> data = (Map<String, Object>) responseMap.get(FIELD_DATA);
        if (data == null) {
            throw new IOException("Response missing 'data' field");
        }

        String resultType = (String) data.get(FIELD_RESULT_TYPE);
        if (!RESULT_TYPE_MATRIX.equals(resultType)) {
            throw new IOException(String.format(Locale.ROOT, "Expected matrix result type, got: %s", resultType));
        }

        List<Map<String, Object>> resultList = (List<Map<String, Object>>) data.get(FIELD_RESULT);
        if (resultList == null) {
            throw new IOException("Response data missing 'result' field");
        }

        List<TimeSeriesResult> timeSeriesResults = new ArrayList<>(resultList.size());
        for (Map<String, Object> resultItem : resultList) {
            Map<String, String> metric = (Map<String, String>) resultItem.get(FIELD_METRIC);
            List<List<Object>> values = (List<List<Object>>) resultItem.get(FIELD_VALUES);

            if (metric == null) {
                throw new IOException(String.format(Locale.ROOT, "Result item missing 'metric' field: %s", resultItem));
            }
            if (values == null) {
                throw new IOException(String.format(Locale.ROOT, "Result item missing 'values' field for metric: %s", metric));
            }

            timeSeriesResults.add(new TimeSeriesResult(metric, values));
        }

        return new PromMatrixResponse(status, new PromMatrixData(timeSeriesResults));
    }

    /**
     * Extract error body from ResponseException.
     *
     * @param e The ResponseException
     * @return The error body or exception message if extraction fails
     */
    private String extractErrorBody(ResponseException e) {
        try {
            return EntityUtils.toString(e.getResponse().getEntity());
        } catch (Exception ex) {
            return ex.getMessage() != null ? ex.getMessage() : UNKNOWN_ERROR;
        }
    }
}
