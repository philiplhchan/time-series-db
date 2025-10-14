/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.List;

/**
 * Interface for pipeline stages that process time series.
 * Pipeline stages are executed in sequence to transform time series data.
 *
 * <p>This interface defines the contract for all pipeline stages in the time series
 * processing pipeline. Pipeline stages can be unary (operating on a single input)
 * or binary (operating on two inputs), and they transform time series data through
 * various operations such as aggregation, transformation, and filtering.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Serialization Support:</strong> Extends {@link Writeable} for
 *       distributed processing</li>
 *   <li><strong>XContent Support:</strong> Extends {@link ToXContent} for
 *       JSON serialization</li>
 *   <li><strong>Type Safety:</strong> Provides type-safe processing of time series data</li>
 * </ul>
 *
 */
public interface PipelineStage extends Writeable {
    /**
     * Get the name of this pipeline stage.
     *
     * @return The stage name
     */
    String getName();

    /**
     * Process time series data. This method should be implemented by all pipeline stages.
     * For unary stages, this processes a single input.
     * For binary stages, this processes two inputs (left and right).
     *
     * @param input The input time series to process
     * @return The processed time series result
     */
    List<TimeSeries> process(List<TimeSeries> input);

    // TODO: this should probably just implement ToXContent interface directly instead of inventing our own
    /**
     * Serialize this stage to XContent including all arguments.
     * @param builder The XContentBuilder to write to
     * @param params Serialization parameters
     * @throws IOException if serialization fails
     */
    void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException;

    /**
     * Write stage-specific data to the output stream for serialization.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    void writeTo(StreamOutput out) throws IOException;

    /**
     * Check if this stage must be executed only at the coordinator level and cannot be executed in the UnfoldAggregator.
     *
     * <p>Coordinator-only stages (like sort, histogramPercentile, binary operations) require access to all time series data
     * and must be executed after all shard-level processing is complete.</p>
     *
     * @return true if this stage must be executed only at the coordinator level, false otherwise
     */
    default boolean isCoordinatorOnly() {
        return false;
    }

    /**
     * Check if this stage supports concurrent segment search execution.
     *
     * <p>Concurrent segment search allows multiple segments to be processed in parallel,
     * which can improve performance for stages that don't require a full view of the time series.
     * This defaults to false because some stages require access to the complete time series
     * to process correctly (e.g., moving window operations that need to see all data points
     * to calculate rolling averages or other windowed functions).</p>
     *
     * @return true if this stage supports concurrent segment search, false otherwise
     */
    default boolean supportConcurrentSegmentSearch() {
        return false;
    }
}
