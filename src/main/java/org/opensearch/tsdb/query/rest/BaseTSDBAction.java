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
import org.opensearch.action.search.StreamSearchAction;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.time.FormatNames;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.streaming.FlushModeResolver;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.TSDBPlugin;

import java.time.Instant;

import static org.opensearch.action.search.StreamSearchTransportService.STREAM_SEARCH_ENABLED;

/**
 * Base class for TSDB query REST handlers (M3QL and PromQL).
 *
 * <p>Provides common functionality including:
 * <ul>
 *   <li>Cluster settings integration for force_no_pushdown</li>
 *   <li>Time parameter parsing with date math support</li>
 *   <li>Common constants and utilities</li>
 * </ul>
 */
public abstract class BaseTSDBAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(BaseTSDBAction.class);

    // Common parameter names
    protected static final String QUERY_PARAM = "query";
    protected static final String START_PARAM = "start";
    protected static final String END_PARAM = "end";
    protected static final String STEP_PARAM = "step";
    protected static final String PARTITIONS_PARAM = "partitions";
    protected static final String EXPLAIN_PARAM = "explain";
    protected static final String PUSHDOWN_PARAM = "pushdown";
    protected static final String CCS_MINIMIZE_ROUNDTRIPS_PARAM = "ccs_minimize_roundtrips";
    protected static final String PROFILE_PARAM = "profile";
    protected static final String INCLUDE_METADATA_PARAM = "include_metadata";

    // Date format pattern
    protected static final String DATE_FORMAT_PATTERN = FormatNames.STRICT_DATE_OPTIONAL_TIME.getSnakeCaseName()
        + "||"
        + FormatNames.EPOCH_MILLIS.getSnakeCaseName();

    // Date parser for consistent time parsing across OpenSearch
    protected static final DateMathParser DATE_MATH_PARSER = DateFormatter.forPattern(DATE_FORMAT_PATTERN).toDateMathParser();

    // Response field names
    protected static final String ERROR_FIELD = "error";

    /**
     * Volatile flag to track cluster-wide pushdown override setting.
     * When true, forces pushdown=false regardless of request parameter.
     */
    private volatile boolean forceNoPushdown;

    private volatile boolean ccsMinimizeRoundTrips;

    /**
     * Volatile flag to track whether stream search is enabled cluster-wide.
     */
    private volatile boolean streamSearchEnabled;

    /**
     * Constructs a new BaseTSDBAction handler.
     *
     * @param clusterSettings cluster settings for accessing dynamic cluster configurations
     */
    protected BaseTSDBAction(ClusterSettings clusterSettings) {
        // Initialize no-pushdown flag from current settings
        this.forceNoPushdown = clusterSettings.get(TSDBPlugin.TSDB_ENGINE_FORCE_NO_PUSHDOWN);

        // Register listener to update no-pushdown flag when setting changes
        clusterSettings.addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_FORCE_NO_PUSHDOWN, newValue -> {
            this.forceNoPushdown = newValue;
            logger.info("Updated force_no_pushdown setting to: {}", newValue);
        });

        this.ccsMinimizeRoundTrips = clusterSettings.get(TSDBPlugin.TSDB_ENGINE_CCS_MINIMIZE_ROUNDTRIPS);
        clusterSettings.addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_CCS_MINIMIZE_ROUNDTRIPS, newValue -> {
            boolean oldValue = this.ccsMinimizeRoundTrips;
            this.ccsMinimizeRoundTrips = newValue;
            logger.info("Updated tsdb_engine.query.ccs_minimize_roundtrips setting from {} to: {}", oldValue, newValue);
        });

        this.streamSearchEnabled = clusterSettings.get(STREAM_SEARCH_ENABLED);
        clusterSettings.addSettingsUpdateConsumer(STREAM_SEARCH_ENABLED, newValue -> {
            this.streamSearchEnabled = newValue;
            logger.info("Updated stream.search.enabled setting to: {}", newValue);
        });
    }

    /**
     * Resolves the pushdown parameter, applying cluster-wide override if configured.
     *
     * @param request the REST request
     * @param defaultValue the default value if parameter is not provided
     * @return resolved pushdown value
     */
    protected boolean resolvePushdownParam(RestRequest request, boolean defaultValue) {
        boolean pushdown = request.paramAsBoolean(PUSHDOWN_PARAM, defaultValue);

        // If force_no_pushdown cluster setting is enabled, override to false
        if (forceNoPushdown) {
            pushdown = false;
        }

        return pushdown;
    }

    protected boolean resolveCcsMinimizeRoundTrips(RestRequest request) {
        return request.paramAsBoolean(CCS_MINIMIZE_ROUNDTRIPS_PARAM, ccsMinimizeRoundTrips);
    }

    /**
     * Parses a time parameter from the request and converts it to milliseconds since epoch.
     *
     * @param request the REST request
     * @param paramName the name of the time parameter
     * @param defaultValue the default value if parameter is not provided
     * @param nowMillis the base time in milliseconds to use for "now" references
     * @return the parsed time in milliseconds since epoch
     */
    protected long parseTimeParam(RestRequest request, String paramName, String defaultValue, long nowMillis) {
        String timeString = request.param(paramName, defaultValue);
        Instant instant = DATE_MATH_PARSER.parse(timeString, () -> nowMillis);
        return instant.toEpochMilli();
    }

    /**
     * Executes a search request, using streaming transport if eligible.
     *
     * <p>Uses {@link StreamSearchAction} when all of the following are true:
     * <ul>
     *   <li>{@code stream.search.enabled} cluster setting is true</li>
     *   <li>{@link FeatureFlags#STREAM_TRANSPORT} feature flag is enabled</li>
     *   <li>The aggregation tree is streamable (per {@link FlushModeResolver})</li>
     * </ul>
     * Otherwise falls back to the standard search path.
     *
     * @param client the node client
     * @param searchRequest the search request to execute
     * @param listener the action listener for the search response
     */
    protected void executeSearch(NodeClient client, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        if (streamSearchEnabled
            && FeatureFlags.isEnabled(FeatureFlags.STREAM_TRANSPORT)
            && FlushModeResolver.isStreamable(searchRequest.source().aggregations())) {
            logger.debug("Using stream search for TSDB query");
            client.execute(StreamSearchAction.INSTANCE, searchRequest, listener);
        } else {
            client.search(searchRequest, listener);
        }
    }
}
