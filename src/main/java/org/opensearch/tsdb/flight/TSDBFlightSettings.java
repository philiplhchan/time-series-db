/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.flight;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Settings for the TSDB Arrow Flight server.
 */
public final class TSDBFlightSettings {

    private TSDBFlightSettings() {
        // Utility class
    }

    /**
     * Enable/disable the TSDB Flight server.
     */
    public static final Setting<Boolean> TSDB_FLIGHT_ENABLED = Setting.boolSetting(
        "tsdb.flight.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Port for the TSDB Flight server.
     */
    public static final Setting<Integer> TSDB_FLIGHT_PORT = Setting.intSetting(
        "tsdb.flight.port",
        9400,
        1,
        65535,
        Setting.Property.NodeScope
    );

    /**
     * Host/address for the TSDB Flight server to bind to.
     */
    public static final Setting<String> TSDB_FLIGHT_HOST = Setting.simpleString("tsdb.flight.host", "0.0.0.0", Setting.Property.NodeScope);

    /**
     * Returns all settings for registration with OpenSearch.
     */
    public static List<Setting<?>> getSettings() {
        return List.of(TSDB_FLIGHT_ENABLED, TSDB_FLIGHT_PORT, TSDB_FLIGHT_HOST);
    }
}
