/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.federation.FederationMetadata;
import org.opensearch.tsdb.query.utils.AggregationNameExtractor;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for M3QL queries.
 *
 * <p>This handler translates M3QL queries to OpenSearch DSL and executes them,
 * returning results in Prometheus matrix format. It provides a simple interface
 * for executing M3QL queries over time series data stored in OpenSearch.</p>
 *
 * <h2>Supported Routes:</h2>
 * <ul>
 *   <li>GET/POST /_m3ql - Execute M3QL query (use 'partitions' param for specific indices)</li>
 * </ul>
 *
 * <h2>Request Parameters:</h2>
 * <ul>
 *   <li><b>query</b> (required): M3QL query string (can be in body or URL param)</li>
 *   <li><b>start</b> (optional): Start time (default: "now-5m")</li>
 *   <li><b>end</b> (optional): End time (default: "now")</li>
 *   <li><b>step</b> (optional): Step interval in milliseconds (default: 10000)</li>
 *   <li><b>partitions</b> (optional): Comma-separated list of indices to query</li>
 *   <li><b>explain</b> (optional): Return translated DSL instead of executing (default: false)</li>
 *   <li><b>pushdown</b> (optional): Enable pushdown optimizations (default: true)</li>
 *   <li><b>include_metadata</b> (optional): Include metadata (step, start, end) for each time series (default: false)</li>
 *   <li><b>resolved_partitions</b> (optional, body only): Federation partition resolution info</li>
 * </ul>
 *
 * <h2>Request Body (JSON):</h2>
 * <pre>{@code
 * {
 *   "query": "fetch service:api | moving 5m sum",
 *   "resolved_partitions": {
 *     "partitions": [
 *       {
 *         "fetch_statement": "service:api",
 *         "partition_windows": [
 *           {
 *             "partition_id": "cluster1:index-a",
 *             "start": "2025-12-13T00:44:49Z",
 *             "end": "2025-12-13T02:14:49Z",
 *             "routing_keys": [
 *               {"key": "service", "value": "api"},
 *               {"key": "region", "value": "us-west"}
 *             ]
 *           }
 *         ]
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 *
 * <h2>Response Format:</h2>
 * <p>Returns Prometheus matrix format (see {@link PromMatrixResponseListener})</p>
 *
 * @see M3OSTranslator
 * @see PromMatrixResponseListener
 */
public class RestM3QLAction extends BaseTSDBAction {
    public static final String NAME = "m3ql_action";

    private static final Logger logger = LogManager.getLogger(RestM3QLAction.class);

    // Route path
    private static final String BASE_M3QL_PATH = "/_m3ql";

    // M3QL-specific parameter names
    private static final String RESOLVED_PARTITIONS_PARAM = "resolved_partitions";

    // Default parameter values
    private static final String DEFAULT_START_TIME = "now-5m";
    private static final String DEFAULT_END_TIME = "now";
    private static final long DEFAULT_STEP_MS = 10_000L; // 10 seconds

    // M3QL-specific response field names
    private static final String M3QL_QUERY_FIELD = "m3ql_query";
    private static final String TRANSLATED_DSL_FIELD = "translated_dsl";
    private static final String EXPLANATION_FIELD = "explanation";

    private static final Metrics METRICS = new Metrics();

    /**
     * Constructs a new RestM3QLAction handler.
     *
     * @param clusterSettings cluster settings for accessing dynamic cluster configurations
     */
    public RestM3QLAction(ClusterSettings clusterSettings) {
        super(clusterSettings);
    }

    /**
     * Returns the metrics container initializer for M3QL REST actions.
     *
     * @return metrics initializer
     */
    public static TSDBMetrics.MetricsInitializer getMetricsInitializer() {
        return METRICS;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_M3QL_PATH), new Route(POST, BASE_M3QL_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse and validate request parameters
        final RequestParams params;
        final Map<String, String> tags = new HashMap<>();
        tags.put("explain", "unknown");
        tags.put("pushdown", "unknown");
        try {
            try {
                params = parseRequestParams(request);
                tags.put("explain", String.valueOf(params.explain()));
                tags.put("pushdown", String.valueOf(params.pushdown()));
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Received M3QL request: query='{}', start={}, end={}, step={}, indices={}, explain={}, pushdown={}, profile={}, include_metadata={}, federation_metadata={}",
                        params.query,
                        params.startMs,
                        params.endMs,
                        params.stepMs,
                        Strings.arrayToCommaDelimitedString(params.indices),
                        params.explain,
                        params.pushdown,
                        params.profile,
                        params.includeMetadata,
                        params.federationMetadata()
                    );
                }
            } catch (IllegalArgumentException e) {
                tags.put("reached_step", "error__parse_request_params");
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, e.getMessage());
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
                };
            }

            // Validate query
            if (params.query == null || params.query.trim().isEmpty()) {
                tags.put("reached_step", "error__missing_query");
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, "Query cannot be empty");
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
                };
            }

            // Translate M3QL to OpenSearch DSL
            try {
                tags.put("reached_step", "translate_query");
                final SearchSourceBuilder searchSourceBuilder = translateQuery(params);

                // Handle explain mode
                if (params.explain) {
                    tags.put("reached_step", "explain");
                    return buildExplainResponse(params.query, searchSourceBuilder);
                }

                // Build and execute search request
                tags.put("reached_step", "build_search_request");
                final SearchRequest searchRequest = buildSearchRequest(params, searchSourceBuilder);
                final String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);

                tags.put("reached_step", "search");
                return channel -> client.search(
                    searchRequest,
                    new PromMatrixResponseListener(
                        channel,
                        finalAggName,
                        params.profile,
                        params.includeMetadata,
                        new PromMatrixResponseListener.QueryMetrics(
                            METRICS.executionLatency,
                            METRICS.collectPhaseLatencyMax,
                            METRICS.reducePhaseLatencyMax,
                            METRICS.collectPhaseCpuTimeMs,
                            METRICS.reducePhaseCpuTimeMs,
                            METRICS.shardLatencyMax
                        )
                    )
                );

            } catch (UnsupportedOperationException e) {
                // UnsupportedOperationException indicates a known M3QL function that's not yet implemented
                String stepReached = tags.getOrDefault("reached_step", "unknown");
                tags.put("reached_step", "error__" + stepReached);
                tags.put("error_type", "unimplemented_function");
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, e.getMessage());
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.NOT_IMPLEMENTED, response));
                };
            } catch (Exception e) {
                // All other errors return BAD_REQUEST
                String stepReached = tags.getOrDefault("reached_step", "unknown");
                tags.put("reached_step", "error__" + stepReached);
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, e.getMessage());
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
                };
            }
        } finally {
            Tags finalTags = Tags.create();
            tags.forEach(finalTags::addTag);
            TSDBMetrics.incrementCounter(METRICS.requestsTotal, 1, finalTags);
        }
    }

    /**
     * Parses request parameters from the REST request.
     *
     * @param request the REST request
     * @return parsed request parameters
     * @throws IOException if parsing fails
     */
    private RequestParams parseRequestParams(RestRequest request) throws IOException {
        // Parse request body (if present) to extract query and resolved_partitions in one pass
        RequestBody requestBody = parseRequestBody(request);

        // Extract query from body or fall back to URL parameter
        String query = (requestBody != null && requestBody.query() != null) ? requestBody.query() : request.param(QUERY_PARAM);

        // Capture base time once for consistent "now" across all time parameters in this request
        long nowMillis = System.currentTimeMillis();

        // Parse time range (default to last 5 minutes)
        long startMs = parseTimeParam(request, START_PARAM, DEFAULT_START_TIME, nowMillis);
        long endMs = parseTimeParam(request, END_PARAM, DEFAULT_END_TIME, nowMillis);

        // Validate time range: start must be before end
        if (startMs >= endMs) {
            throw new IllegalArgumentException(
                "Invalid time range: start time must be before end time (start=" + startMs + ", end=" + endMs + ")"
            );
        }

        // Parse step interval
        long stepMs = request.paramAsLong(STEP_PARAM, DEFAULT_STEP_MS);

        // Parse indices/partitions
        String[] indices = Strings.splitStringByCommaToArray(request.param(PARTITIONS_PARAM));

        // Parse flags
        boolean explain = request.paramAsBoolean(EXPLAIN_PARAM, false);
        boolean pushdown = resolvePushdownParam(request, true);
        boolean profile = request.paramAsBoolean(PROFILE_PARAM, false);
        boolean includeMetadata = request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);

        // Extract resolved partitions from request body (implements FederationMetadata)
        FederationMetadata federationMetadata = (requestBody != null) ? requestBody.resolvedPartitions() : null;

        return new RequestParams(query, startMs, endMs, stepMs, indices, explain, pushdown, profile, includeMetadata, federationMetadata);
    }

    /**
     * Parses the request body (both query and resolved_partitions) in a single pass.
     *
     * @param request the REST request
     * @return parsed RequestBody, or null if no body content
     * @throws IOException if parsing fails
     */
    private RequestBody parseRequestBody(RestRequest request) throws IOException {
        if (!request.hasContent()) {
            return null;
        }

        if (logger.isDebugEnabled()) {
            try {
                logger.debug("Parsing request body: {}", request.content().utf8ToString());
            } catch (Throwable ignore) {}
        }

        try (XContentParser parser = request.contentParser()) {
            return RequestBody.parse(parser);
        }
    }

    /**
     * Translates an M3QL query to OpenSearch DSL.
     *
     * @param params request parameters containing the query and time range
     * @return the translated SearchSourceBuilder
     */
    private SearchSourceBuilder translateQuery(RequestParams params) {
        M3OSTranslator.Params translatorParams = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            params.startMs,
            params.endMs,
            params.stepMs,
            params.pushdown,
            params.profile,
            params.federationMetadata
        );
        return M3OSTranslator.translate(params.query, translatorParams);
    }

    /**
     * Builds a SearchRequest from the translated query.
     *
     * @param params request parameters
     * @param searchSourceBuilder the translated search source
     * @return configured SearchRequest
     */
    private SearchRequest buildSearchRequest(RequestParams params, SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        // Disable query cache explicitly as TSDB TSDBDirectoryReader does not support caching
        // TODO: Remove this once TSDB TSDBDirectoryReader supports caching
        searchRequest.requestCache(false);

        // Set indices if specified
        if (params.indices.length > 0) {
            searchRequest.indices(params.indices);
        }

        return searchRequest;
    }

    /**
     * Builds a response for explain mode that returns the translated DSL.
     *
     * @param query the original M3QL query
     * @param searchSourceBuilder the translated DSL
     * @return a RestChannelConsumer that sends the explain response
     */
    private RestChannelConsumer buildExplainResponse(String query, SearchSourceBuilder searchSourceBuilder) {
        return channel -> {
            XContentBuilder response = channel.newBuilder();
            response.startObject();
            response.field(M3QL_QUERY_FIELD, query);
            response.field(TRANSLATED_DSL_FIELD, searchSourceBuilder.toString());
            response.field(EXPLANATION_FIELD, "M3QL query translated to OpenSearch DSL");
            response.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response));
        };
    }

    /**
     * Internal record holding parsed request parameters.
     */
    protected record RequestParams(String query, long startMs, long endMs, long stepMs, String[] indices, boolean explain, boolean pushdown,
        boolean profile, boolean includeMetadata, FederationMetadata federationMetadata) {
    }

    /**
     * Metrics container for RestM3QLAction.
     */
    static class Metrics implements TSDBMetrics.MetricsInitializer {
        static final String REQUESTS_TOTAL_METRIC_NAME = "tsdb.action.rest.m3ql.queries.total";
        Counter requestsTotal;
        Histogram executionLatency;
        Histogram collectPhaseLatencyMax;
        Histogram reducePhaseLatencyMax;
        Histogram collectPhaseCpuTimeMs;
        Histogram reducePhaseCpuTimeMs;
        Histogram shardLatencyMax;

        @Override
        public void register(MetricsRegistry registry) {
            requestsTotal = registry.createCounter(
                REQUESTS_TOTAL_METRIC_NAME,
                "total number of queries handled by the RestM3QLAction rest handler",
                TSDBMetricsConstants.UNIT_COUNT
            );
            executionLatency = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_EXECUTION_LATENCY,
                TSDBMetricsConstants.ACTION_REST_QUERIES_EXECUTION_LATENCY_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            collectPhaseLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            reducePhaseLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            collectPhaseCpuTimeMs = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS,
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            reducePhaseCpuTimeMs = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS,
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            shardLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_SHARD_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_SHARD_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
        }

        @Override
        public synchronized void cleanup() {
            requestsTotal = null;
            executionLatency = null;
            collectPhaseLatencyMax = null;
            reducePhaseLatencyMax = null;
            collectPhaseCpuTimeMs = null;
            reducePhaseCpuTimeMs = null;
            shardLatencyMax = null;
        }
    }
}
