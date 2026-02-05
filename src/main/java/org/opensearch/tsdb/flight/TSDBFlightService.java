/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.flight;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.transport.AuxTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * TSDB Flight Service manages the Arrow Flight server for high-performance bulk ingestion.
 * This is an auxiliary transport that runs alongside the standard OpenSearch transport.
 */
public class TSDBFlightService extends AuxTransport {

    private static final Logger logger = LogManager.getLogger(TSDBFlightService.class);
    private static final String TSDB_FLIGHT_TRANSPORT_SETTING_KEY = "tsdb-arrow-flight";

    private final Settings settings;
    private BufferAllocator allocator;
    private FlightServer flightServer;
    private BoundTransportAddress boundAddress;

    /**
     * Constructs a TSDBFlightService.
     *
     * @param settings the node settings
     */
    public TSDBFlightService(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String settingKey() {
        return TSDB_FLIGHT_TRANSPORT_SETTING_KEY;
    }

    @Override
    public BoundTransportAddress getBoundAddress() {
        return boundAddress;
    }

    @SuppressWarnings("removal")
    @Override
    protected void doStart() {
        try {
            logger.info("Starting TSDB Flight Service...");

            // Create Arrow allocator with privileged access
            allocator = AccessController.doPrivileged((PrivilegedAction<BufferAllocator>) () -> new RootAllocator(Long.MAX_VALUE));

            // Get configuration
            String host = TSDBFlightSettings.TSDB_FLIGHT_HOST.get(settings);
            int port = TSDBFlightSettings.TSDB_FLIGHT_PORT.get(settings);

            // Create Flight location
            Location location = Location.forGrpcInsecure(host, port);

            // Create producer
            TSDBFlightProducer producer = new TSDBFlightProducer();

            // Build and start Flight server with privileged access
            flightServer = AccessController.doPrivileged((PrivilegedAction<FlightServer>) () -> {
                try {
                    FlightServer server = FlightServer.builder(allocator, location, producer).build();
                    server.start();
                    return server;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to start Flight server", e);
                }
            });

            // Create bound address
            int actualPort = flightServer.getPort();
            InetAddress bindAddress = InetAddresses.forString(host.equals("0.0.0.0") ? "127.0.0.1" : host);
            TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(bindAddress, actualPort));
            boundAddress = new BoundTransportAddress(new TransportAddress[] { publishAddress }, publishAddress);

            logger.info("TSDB Flight Service started on {}:{}", host, actualPort);

        } catch (Exception e) {
            logger.error("Failed to start TSDB Flight Service", e);
            doClose();
            throw new RuntimeException("Failed to start TSDB Flight Service", e);
        }
    }

    @Override
    protected void doStop() {
        logger.info("Stopping TSDB Flight Service...");
        doClose();
    }

    @Override
    protected void doClose() {
        try {
            if (flightServer != null) {
                flightServer.close();
                flightServer = null;
                logger.info("Flight server closed");
            }
        } catch (Exception e) {
            logger.warn("Error closing Flight server", e);
        }

        try {
            if (allocator != null) {
                allocator.close();
                allocator = null;
                logger.info("Arrow allocator closed");
            }
        } catch (Exception e) {
            logger.warn("Error closing Arrow allocator", e);
        }
    }

    /**
     * Returns the port the Flight server is listening on.
     * Useful for testing when port 0 is used.
     */
    public int getPort() {
        if (flightServer != null) {
            return flightServer.getPort();
        }
        return -1;
    }
}
