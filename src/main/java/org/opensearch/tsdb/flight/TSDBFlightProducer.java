/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.flight;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.TSDBEngine;

/**
 * Arrow Flight producer for TSDB bulk ingestion.
 * Handles DoPut requests to index time-series data directly via Arrow vectors.
 */
public class TSDBFlightProducer extends NoOpFlightProducer {

    private static final Logger logger = LogManager.getLogger(TSDBFlightProducer.class);

    /**
     * Constructs a TSDBFlightProducer.
     */
    public TSDBFlightProducer() {
        // No dependencies needed - uses singleton TSDBEngineRegistry
    }

    /**
     * Handles DoPut requests to ingest Arrow RecordBatches into TSDB.
     *
     * The FlightDescriptor path should contain the index name as the first element.
     * For example: FlightDescriptor.path("my-tsdb-index")
     *
     * Expected Arrow schema:
     * - labels: List of Utf8 - Pre-split label KV pairs ["key1", "val1", "key2", "val2", ...]
     * - timestamp: Int64 - Epoch milliseconds
     * - value: Float64 - Sample value
     *
     * @param context the call context
     * @param flightStream the incoming stream of RecordBatches
     * @param ackStream the acknowledgment stream to send results back
     * @return a Runnable to execute the put operation
     */
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> {
            try {
                // Extract index name from FlightDescriptor path
                FlightDescriptor descriptor = flightStream.getDescriptor();
                if (descriptor == null || descriptor.getPath() == null || descriptor.getPath().isEmpty()) {
                    ackStream.onError(
                        CallStatus.INVALID_ARGUMENT.withDescription("FlightDescriptor must contain index name in path").toRuntimeException()
                    );
                    return;
                }

                String indexName = descriptor.getPath().get(0);
                logger.info("Received DoPut request for index [{}]", indexName);

                // Look up TSDBEngine from registry
                TSDBEngine engine = TSDBEngineRegistry.getInstance().get(indexName);
                if (engine == null) {
                    ackStream.onError(
                        CallStatus.NOT_FOUND.withDescription(
                            "TSDB index not found: " + indexName + ". Make sure the index exists and is a TSDB index."
                        ).toRuntimeException()
                    );
                    return;
                }

                int totalDocs = 0;
                int totalBatches = 0;

                // Process each RecordBatch in the stream
                while (flightStream.next()) {
                    VectorSchemaRoot root = flightStream.getRoot();
                    int rowCount = root.getRowCount();

                    if (rowCount > 0) {
                        int indexedCount = engine.indexArrowBatch(root);
                        totalDocs += indexedCount;
                        totalBatches++;

                        // Send simple acknowledgment (POC: no metadata to avoid ArrowBuf complexity)
                        ackStream.onNext(PutResult.empty());

                        logger.debug("Indexed batch {} with {} documents for index [{}]", totalBatches, indexedCount, indexName);
                    }
                }

                logger.info("Completed DoPut for index [{}]: {} batches, {} documents", indexName, totalBatches, totalDocs);
                ackStream.onCompleted();

            } catch (Exception e) {
                logger.error("Error processing DoPut request", e);
                ackStream.onError(
                    CallStatus.INTERNAL.withDescription("Error processing DoPut: " + e.getMessage()).withCause(e).toRuntimeException()
                );
            }
        };
    }
}
