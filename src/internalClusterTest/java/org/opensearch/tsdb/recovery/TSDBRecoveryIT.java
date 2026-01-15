/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.SetOnce;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.recovery.RecoveryStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.indices.recovery.PeerRecoverySourceService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTranslogOperationsRequest;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.IndicesService;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for TSDB recovery scenarios.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TSDBRecoveryIT extends TSDBRecoveryITBase {

    private static final String INDEX_NAME = "recovery_test_index";
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 0;

    /**
     * Tests basic gateway recovery after cluster restart.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testGatewayRecovery()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Start single node cluster</li>
     *   <li>Create TSDB index with samples</li>
     *   <li>Restart cluster</li>
     *   <li>Validate recovery state and TSDB-specific components</li>
     * </ol>
     */
    public void testGatewayRecovery() throws Exception {
        logger.info("--> starting single node");
        String node = internalCluster().startNode();

        logger.info("--> creating TSDB index with samples");
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, SHARD_COUNT, REPLICA_COUNT);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        // Ingest time series samples
        int sampleCount = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(sampleCount, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, INDEX_NAME);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        logger.info("--> indexed {} samples", sampleCount);

        logger.info("--> restarting cluster to trigger gateway recovery");
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        logger.info("--> verifying recovery response");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        assertThat("Should have recovery states", response.shardRecoveryStates().size(), equalTo(SHARD_COUNT));

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat("Should have one recovery state", recoveryStates.size(), equalTo(1));

        RecoveryState recoveryState = recoveryStates.get(0);

        // Validate recovery metadata
        assertRecoveryState(
            recoveryState,
            0,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            null,
            node
        );

        // Validate recovery index state
        validateIndexRecoveryState(recoveryState);

        // Validate TSDB recovery - query primary and replicas to ensure data consistency
        validateTSDBRecovery(INDEX_NAME, 0, baseTimestamp, samplesIntervalMillis, sampleCount);

        logger.info("--> gateway recovery test completed successfully");
    }

    /**
     * Tests gateway recovery with activeOnly filter for recovery states.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testGatewayRecoveryTestActiveOnly()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Start cluster and create TSDB index</li>
     *   <li>Index time series samples</li>
     *   <li>Restart cluster to trigger gateway recovery</li>
     *   <li>Request recovery states with activeOnly=true filter</li>
     *   <li>Verify that completed recoveries are not included</li>
     * </ol>
     */
    public void testGatewayRecoveryActiveOnly() throws Exception {
        logger.info("--> starting single node");
        internalCluster().startNode();

        logger.info("--> creating TSDB index");
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, SHARD_COUNT, REPLICA_COUNT);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        // Ingest time series samples
        int sampleCount = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(sampleCount, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, INDEX_NAME);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        logger.info("--> requesting recoveries with activeOnly filter");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).setActiveOnly(true).execute().actionGet();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat("Should have no active recoveries", recoveryStates.size(), equalTo(0));
    }

    /**
     * Validates index recovery state metrics.
     * Ensures recovery percentage and timing metrics are reasonable.
     *
     * @param state The recovery state index component
     */
    private void validateIndexRecoveryState(RecoveryState state) {
        ReplicationLuceneIndex indexState = state.getIndex();

        assertThat("Recovery time should be non-negative", indexState.time(), greaterThanOrEqualTo(0L));
        assertThat("Recovered files percent should be valid", indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat("Recovered files percent should not exceed 100%", indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat("Recovered bytes percent should be valid", indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat("Recovered bytes percent should not exceed 100%", indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    /**
     * Helper method to slow down recovery for testing throttling.
     */
    private void slowDownRecovery(ByteSizeValue shardSize) {
        long chunkSize = Math.max(1, shardSize.getBytes() / 10);
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        // one chunk per sec
                        .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSize, ByteSizeUnit.BYTES)
                        // small chunks
                        .put(INDICES_RECOVERY_CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkSize, ByteSizeUnit.BYTES))
                )
                .get()
                .isAcknowledged()
        );
    }

    /**
     * Helper method to restore normal recovery speed.
     */
    private void restoreRecoverySpeed() {
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "20mb")
                        .put(
                            INDICES_RECOVERY_CHUNK_SIZE_SETTING.getKey(),
                            RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING.getDefault(Settings.EMPTY)
                        )
                )
                .get()
                .isAcknowledged()
        );
    }

    /**
     * Helper method to get matcher for throttling validation.
     */
    private Matcher<Long> getMatcherForThrottling(long value) {
        return greaterThan(value);
    }

    /**
     * Helper to find recovery states for a target node.
     */
    private List<RecoveryState> findRecoveriesForTargetNode(String nodeName, List<RecoveryState> recoveryStates) {
        List<RecoveryState> nodeResponses = new ArrayList<>();
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getTargetNode() != null && recoveryState.getTargetNode().getName().equals(nodeName)) {
                nodeResponses.add(recoveryState);
            }
        }
        return nodeResponses;
    }

    /**
     * Helper to create and populate TSDB index.
     */
    private IndicesStatsResponse createAndPopulateTSDBIndex(String name, int shards, int replicas) throws Exception {
        logger.info("--> creating TSDB index: {}", name);
        IndexConfig indexConfig = createDefaultIndexConfig(name, shards, replicas);
        createTimeSeriesIndex(indexConfig);
        ensureGreen();

        logger.info("--> indexing time-series samples");
        int sampleCount = randomIntBetween(500, 1000);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 10000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(sampleCount, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, name);
        client().admin().indices().prepareFlush(name).setForce(true).get();

        return client().admin().indices().prepareStats(name).execute().actionGet();
    }

    /**
     * Tests manual shard relocation with recovery throttling.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testRerouteRecovery()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with time series samples</li>
     *   <li>Configure recovery throttling limits</li>
     *   <li>Trigger manual shard relocation via reroute API</li>
     *   <li>Verify recovery completes successfully despite throttling</li>
     * </ol>
     */
    public void testRerouteRecovery() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create TSDB index on node: {}", nodeA);
        ByteSizeValue shardSize = createAndPopulateTSDBIndex(INDEX_NAME, SHARD_COUNT, REPLICA_COUNT).getShards()[0].getStats()
            .getStore()
            .size();

        logger.info("--> start node B");
        final String nodeB = internalCluster().startNode();

        ensureGreen();

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeB))
            .execute()
            .actionGet()
            .getState();

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(INDEX_NAME);
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(), equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(), equalTo(1));
        }, 60, java.util.concurrent.TimeUnit.SECONDS);

        logger.info("--> request recoveries");
        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeARecoveryStates.get(0),
            0,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            null,
            nodeA
        );
        validateIndexRecoveryState(nodeARecoveryStates.get(0));

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0));

        logger.info("--> request node recovery stats");
        NodesStatsResponse statsResponse = client().admin()
            .cluster()
            .prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();

        long nodeAThrottling = Long.MAX_VALUE;
        long nodeBThrottling = Long.MAX_VALUE;
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            if (nodeStats.getNode().getName().equals(nodeA)) {
                assertThat("node A should have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(1));
                assertThat("node A should not have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(0));
                nodeAThrottling = recoveryStats.throttleTime().millis();
            }
            if (nodeStats.getNode().getName().equals(nodeB)) {
                assertThat("node B should not have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(0));
                assertThat("node B should have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(1));
                nodeBThrottling = recoveryStats.throttleTime().millis();
            }
        }

        logger.info("--> checking throttling increases");
        final long finalNodeAThrottling = nodeAThrottling;
        final long finalNodeBThrottling = nodeBThrottling;
        assertBusy(() -> {
            NodesStatsResponse statsResponse1 = client().admin()
                .cluster()
                .prepareNodesStats()
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
                .get();
            List<NodeStats> dataNodeStats = statsResponse1.getNodes()
                .stream()
                .filter(nodeStats -> nodeStats.getNode().isDataNode())
                .collect(Collectors.toList());
            assertThat(dataNodeStats, hasSize(2));
            for (NodeStats nodeStats : dataNodeStats) {
                final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
                if (nodeStats.getNode().getName().equals(nodeA)) {
                    assertThat(
                        "node A throttling should increase",
                        recoveryStats.throttleTime().millis(),
                        getMatcherForThrottling(finalNodeAThrottling)
                    );
                }
                if (nodeStats.getNode().getName().equals(nodeB)) {
                    assertThat(
                        "node B throttling should increase",
                        recoveryStats.throttleTime().millis(),
                        getMatcherForThrottling(finalNodeBThrottling)
                    );
                }
            }
        });

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();

        // wait for it to be finished
        ensureGreen();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(1));

        assertRecoveryState(
            recoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(recoveryStates.get(0));

        Consumer<String> assertNodeHasThrottleTimeAndNoRecoveries = nodeName -> {
            NodesStatsResponse nodesStatsResponse = client().admin()
                .cluster()
                .prepareNodesStats()
                .setNodesIds(nodeName)
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
                .get();
            assertThat(nodesStatsResponse.getNodes(), hasSize(1));
            NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
            assertThat(nodeName + " throttling should be >0", recoveryStats.throttleTime().millis(), getMatcherForThrottling(0));
        };
        // we have to use assertBusy as recovery counters are decremented only when the last reference to the RecoveryTarget
        // is decremented, which may happen after the recovery was done.
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> bump replica count");
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put("number_of_replicas", 1))
            .execute()
            .actionGet();
        ensureGreen();

        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> start node C");
        String nodeC = internalCluster().startNode();
        int nodeCount = internalCluster().getNodeNames().length;
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(String.valueOf(nodeCount)).get().isTimedOut());

        logger.info("--> slowing down recoveries");
        slowDownRecovery(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeC))
            .execute()
            .actionGet()
            .getState();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeARecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            false,
            RecoveryState.Stage.DONE,
            nodeB,
            nodeA
        );
        validateIndexRecoveryState(nodeARecoveryStates.get(0));

        assertRecoveryState(
            nodeBRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(nodeBRecoveryStates.get(0));

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0));

        logger.info("--> speeding up recoveries");
        restoreRecoverySpeed();
        ensureGreen();

        response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();
        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(0));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeBRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(nodeBRecoveryStates.get(0));

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(
            nodeCRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            false,
            RecoveryState.Stage.DONE,
            nodeB,
            nodeC
        );
        validateIndexRecoveryState(nodeCRecoveryStates.get(0));
    }

    /**
     * Tests that recovery can be cancelled and uses existing shard copy.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testCancelNewShardRecoveryAndUsesExistingShardCopy()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index on node A with samples</li>
     *   <li>Start node B and add replica</li>
     *   <li>Stop node B mid-recovery</li>
     *   <li>Restart node B - should use existing shard copy instead of starting new recovery</li>
     * </ol>
     */
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create TSDB index on node: {}", nodeA);
        IndexConfig indexConfig = createDefaultIndexConfig(INDEX_NAME, 1, 1);
        createTimeSeriesIndex(indexConfig);

        int numSamples = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, INDEX_NAME);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        logger.info("--> start node B");
        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();

        logger.info("--> add replica for {} on node: {}", INDEX_NAME, nodeB);
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)
            )
            .get();
        ensureGreen(INDEX_NAME);

        logger.info("--> start node C");
        final String nodeC = internalCluster().startNode();

        ensureGreen(INDEX_NAME);

        // hold peer recovery on phase 2 after nodeB down
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeA);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action)) {
                phase1ReadyBlocked.countDown();
                try {
                    allowToCompletePhase1Latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> restart node B");
        internalCluster().restartNode(nodeB, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                phase1ReadyBlocked.await();
                // nodeB stopped, peer recovery from nodeA to nodeC, it will be cancelled after nodeB get started.
                RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX_NAME).execute().actionGet();

                List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
                List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
                assertThat(nodeCRecoveryStates.size(), equalTo(1));

                assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeA, nodeC);
                validateIndexRecoveryState(nodeCRecoveryStates.get(0));

                return super.onNodeStopped(nodeName);
            }
        });

        // wait for peer recovery from nodeA to nodeB which is a no-op recovery so it skips the CLEAN_FILES stage and hence is not blocked
        ensureGreen();
        allowToCompletePhase1Latch.countDown();
        transportService.clearAllRules();

        // make sure nodeA has primary and nodeB has replica
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(2));
        for (ShardRouting shardRouting : startedShards) {
            if (shardRouting.primary()) {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeA));
            } else {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeB));
            }
        }
    }

    /**
     * Tests local recovery up to global checkpoint before peer recovery.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testRecoverLocallyUpToGlobalCheckpoint()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with samples on multiple nodes</li>
     *   <li>Fail a shard to trigger recovery</li>
     *   <li>Verify that local translog operations up to global checkpoint are recovered</li>
     *   <li>Ensure peer recovery only transfers operations after global checkpoint</li>
     * </ol>
     */
    public void testRecoverLocallyUpToGlobalCheckpoint() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> nodes = randomSubsetOf(
            2,
            StreamSupport.stream(Spliterators.spliterator(clusterService().state().nodes().getDataNodes().values(), 0), false)
                .map(node -> node.getName())
                .collect(Collectors.toSet())
        );
        String indexName = "test-index";
        java.util.HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put("index.routing.allocation.include._name", String.join(",", nodes));

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 1, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(indexName);

        int numSamples = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, indexName);

        client().admin().indices().prepareFlush(indexName).setForce(true).get();
        client().admin().indices().prepareRefresh(indexName).get(); // avoid refresh when we are failing a shard

        String failingNode = randomFrom(nodes);
        PlainActionFuture<StartRecoveryRequest> startRecoveryRequestFuture = new PlainActionFuture<>();
        // Peer recovery fails if the primary does not see the recovering replica in the replication group (when the cluster state
        // update on the primary is delayed). To verify the local recovery stats, we have to manually remember this value in the
        // first try because the local recovery happens once and its stats is reset when the recovery fails.
        SetOnce<Integer> localRecoveredOps = new SetOnce<>();
        for (String node : nodes) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    final RecoveryState recoveryState = internalCluster().getInstance(IndicesService.class, failingNode)
                        .getShardOrNull(new ShardId(resolveIndex(indexName), 0))
                        .recoveryState();
                    assertThat(recoveryState.getTranslog().recoveredOperations(), equalTo(recoveryState.getTranslog().totalLocal()));
                    if (startRecoveryRequestFuture.isDone()) {
                        assertThat(recoveryState.getTranslog().totalLocal(), equalTo(0));
                        recoveryState.getTranslog().totalLocal(localRecoveredOps.get());
                        recoveryState.getTranslog().incrementRecoveredOperations(localRecoveredOps.get());
                    } else {
                        localRecoveredOps.set(recoveryState.getTranslog().totalLocal());
                        startRecoveryRequestFuture.onResponse((StartRecoveryRequest) request);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        IndexShard shard = internalCluster().getInstance(IndicesService.class, failingNode)
            .getShardOrNull(new ShardId(resolveIndex(indexName), 0));
        final long lastSyncedGlobalCheckpoint = shard.getLastSyncedGlobalCheckpoint();
        final long localCheckpointOfSafeCommit;
        try (GatedCloseable<IndexCommit> wrappedSafeCommit = shard.acquireSafeIndexCommit()) {
            localCheckpointOfSafeCommit = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                wrappedSafeCommit.get().getUserData().entrySet()
            ).localCheckpoint;
        }
        final long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        shard.failShard("test", new IOException("simulated"));
        StartRecoveryRequest startRecoveryRequest = startRecoveryRequestFuture.actionGet();
        logger.info(
            "--> start recovery request: starting seq_no {}, commit {}",
            startRecoveryRequest.startingSeqNo(),
            startRecoveryRequest.metadataSnapshot().getCommitUserData()
        );
        SequenceNumbers.CommitInfo commitInfoAfterLocalRecovery = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            startRecoveryRequest.metadataSnapshot().getCommitUserData().entrySet()
        );
        // TSDB recovery is from the start of the live index
        assertThat(commitInfoAfterLocalRecovery.localCheckpoint, lessThanOrEqualTo(lastSyncedGlobalCheckpoint));
        assertThat(commitInfoAfterLocalRecovery.maxSeqNo, equalTo(lastSyncedGlobalCheckpoint));
        assertThat(startRecoveryRequest.startingSeqNo(), lessThanOrEqualTo(lastSyncedGlobalCheckpoint + 1));
        ensureGreen(indexName);
        assertThat((long) localRecoveredOps.get(), equalTo(lastSyncedGlobalCheckpoint - localCheckpointOfSafeCommit));
        for (RecoveryState recoveryState : client().admin().indices().prepareRecoveries().get().shardRecoveryStates().get(indexName)) {
            if (startRecoveryRequest.targetNode().equals(recoveryState.getTargetNode())) {
                assertThat("expect an operation-based recovery", recoveryState.getIndex().fileDetails(), org.hamcrest.Matchers.empty());
                assertThat(
                    "total recovered translog operations must include both local and remote recovery",
                    recoveryState.getTranslog().recoveredOperations(),
                    greaterThanOrEqualTo(Math.toIntExact(maxSeqNo - localCheckpointOfSafeCommit))
                );
            }
        }
        for (String node : nodes) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            transportService.clearAllRules();
        }
    }

    /**
     * Tests that allocating an empty primary resets global checkpoint.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testAllocateEmptyPrimaryResetsGlobalCheckpoint()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with replica and index samples</li>
     *   <li>Stop all nodes holding shard copies (simulate total data loss)</li>
     *   <li>Allocate empty primary on new node</li>
     *   <li>Verify global checkpoint is reset to NO_OPS_PERFORMED</li>
     * </ol>
     */
    public void testAllocateEmptyPrimaryResetsGlobalCheckpoint() throws Exception {
        internalCluster().startClusterManagerOnlyNode(Settings.EMPTY);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final Settings randomNodeDataPathSettings = internalCluster().dataPathSettings(randomFrom(dataNodes));

        final String indexName = "test";
        IndexConfig indexConfig = createDefaultIndexConfig(indexName, 1, 1);
        createTimeSeriesIndex(indexConfig);

        int numSamples = between(10, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 1000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, indexName);
        ensureGreen();

        internalCluster().stopRandomDataNode();
        internalCluster().stopRandomDataNode();
        final String nodeWithoutData = internalCluster().startDataOnlyNode();
        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new AllocateEmptyPrimaryAllocationCommand(indexName, 0, nodeWithoutData, true))
                .get()
        );
        internalCluster().startDataOnlyNode(randomNodeDataPathSettings);
        ensureGreen();

        for (ShardStats shardStats : client().admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()) {
            assertThat(shardStats.getSeqNoStats().getMaxSeqNo(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
            assertThat(shardStats.getSeqNoStats().getLocalCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
            assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        }
    }

    /**
     * Tests that disk space is properly reserved during peer recovery phase one.
     *
     * <p>Adopted from: {@code IndexRecoveryIT.testReservesBytesDuringPeerRecoveryPhaseOne()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with samples on primary</li>
     *   <li>Add replica to trigger peer recovery</li>
     *   <li>Intercept FILES_INFO and FILE_CHUNK recovery actions</li>
     *   <li>Verify disk space is reserved on target node during file transfer</li>
     *   <li>Verify reservation is released after recovery completes</li>
     * </ol>
     */
    public void testReservesBytesDuringPeerRecoveryPhaseOne() throws Exception {
        internalCluster().startNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);

        String indexName = "test-index";
        HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put("index.routing.allocation.include._name", String.join(",", dataNodes));

        IndexConfig indexConfig = new IndexConfig(indexName, 1, 0, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(indexName);

        int numSamples = randomIntBetween(400, 500);
        long baseTimestamp = System.currentTimeMillis();
        long samplesIntervalMillis = 200000L;
        List<TimeSeriesSample> samples = generateTimeSeriesSamples(numSamples, baseTimestamp, samplesIntervalMillis);
        ingestSamples(samples, indexName);
        assertThat(client().admin().indices().prepareFlush(indexName).setForce(true).get().getFailedShards(), equalTo(0));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode nodeWithPrimary = clusterState.nodes()
            .get(clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId());
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nodeWithPrimary.getName()
        );

        final AtomicBoolean fileInfoIntercepted = new AtomicBoolean();
        final AtomicBoolean fileChunkIntercepted = new AtomicBoolean();

        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILES_INFO)) {
                if (fileInfoIntercepted.compareAndSet(false, true)) {
                    final NodeIndicesStats nodeIndicesStats = client().admin()
                        .cluster()
                        .prepareNodesStats(connection.getNode().getId())
                        .clear()
                        .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Store))
                        .get()
                        .getNodes()
                        .get(0)
                        .getIndices();
                    assertThat(nodeIndicesStats.getStore().getReservedSize().getBytes(), equalTo(0L));
                    assertThat(
                        nodeIndicesStats.getShardStats(clusterState.metadata().index(indexName).getIndex())
                            .stream()
                            .flatMap(s -> Arrays.stream(s.getShards()))
                            .map(s -> s.getStats().getStore().getReservedSize().getBytes())
                            .collect(java.util.stream.Collectors.toList()),
                        everyItem(equalTo(StoreStats.UNKNOWN_RESERVED_BYTES))
                    );
                }
            } else if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                if (fileChunkIntercepted.compareAndSet(false, true)) {
                    assertThat(
                        client().admin()
                            .cluster()
                            .prepareNodesStats(connection.getNode().getId())
                            .clear()
                            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Store))
                            .get()
                            .getNodes()
                            .get(0)
                            .getIndices()
                            .getStore()
                            .getReservedSize()
                            .getBytes(),
                        greaterThan(0L)
                    );
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        assertAcked(
            client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("index.number_of_replicas", 1))
        );
        ensureGreen();
        assertTrue(fileInfoIntercepted.get());
        assertTrue(fileChunkIntercepted.get());

        assertThat(
            client().admin()
                .cluster()
                .prepareNodesStats()
                .get()
                .getNodes()
                .stream()
                .mapToLong(n -> n.getIndices().getStore().getReservedSize().getBytes())
                .sum(),
            equalTo(0L)
        );
    }

    /**
     * Tests that translog operations are replayed in forward order during peer recovery.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with small translog generation threshold</li>
     *   <li>Index time series samples in multiple batches with force flush between batches</li>
     *   <li>Force flush creates multiple translog generations</li>
     *   <li>Add replica to trigger peer recovery</li>
     *   <li>Intercept TRANSLOG_OPS action to track operation replay order</li>
     *   <li>Verify operations are replayed in ascending seqNo order (forward read)</li>
     * </ol>
     *
     * <p>This test verifies that the {@code index.translog.read_forward=true} setting
     * makes translog generations to be read from oldest to newest during recovery.
     */
    public void testTranslogReplayInForwardOrderDuringRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        HashMap<String, Object> settings = getDefaultTSDBSettings();
        settings.put("index.translog.generation_threshold_size", "1kb");  // Force frequent rolling
        IndexConfig indexConfig = new IndexConfig(INDEX_NAME, 1, 0, settings, parseMappingFromConstants(), null);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(INDEX_NAME);

        long baseTimestamp = System.currentTimeMillis();
        int batchSize = 150;
        int numBatches = 3;
        List<Long> expectedSeqNos = new ArrayList<>();

        for (int batch = 0; batch < numBatches; batch++) {
            List<TimeSeriesSample> samples = generateTimeSeriesSamples(batchSize, baseTimestamp + batch * 30000L, 10000L);
            ingestSamples(samples, INDEX_NAME);
            for (int i = 0; i < samples.size(); i++) {
                expectedSeqNos.add((long) (batch * batchSize + i));
            }
            logger.info("--> force flushing to roll translog generation after batch {}", batch);
            client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        }

        logger.info("--> start node B");
        String nodeB = internalCluster().startNode();

        logger.info("--> setting up interception on node A to track translog operation replay order");
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeA);

        // Track actual seqNo order during recovery
        List<Long> actualSeqNos = new ArrayList<>();

        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.TRANSLOG_OPS)) {
                // Extract seqNos from the translog operations being sent
                RecoveryTranslogOperationsRequest translogRequest = (RecoveryTranslogOperationsRequest) request;
                for (Translog.Operation op : translogRequest.operations()) {
                    actualSeqNos.add(op.seqNo());
                }
                logger.info(
                    "--> intercepted TRANSLOG_OPS with {} operations, seqNo range: [{}-{}]",
                    translogRequest.operations().size(),
                    translogRequest.operations().get(0).seqNo(),
                    translogRequest.operations().get(translogRequest.operations().size() - 1).seqNo()
                );
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> adding replica to trigger peer recovery");
        client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put("number_of_replicas", 1)).get();
        ensureGreen(INDEX_NAME);

        assertThat("Should have replayed all operations", actualSeqNos.size(), equalTo(expectedSeqNos.size()));
        assertThat(
            "SeqNos should be in ascending order, proving forward translog read (oldest generation first)",
            actualSeqNos,
            equalTo(expectedSeqNos)
        );
        validateTSDBRecovery(INDEX_NAME, 0, baseTimestamp, 10000L, expectedSeqNos.size());
    }

    /**
     * Tests that empty series are NOT dropped during replica recovery, even when a flush occurs mid-recovery.
     */
    public void testDropEmptySeriesNotAllowedDuringRecovery() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(2, 3));

        final String indexName = "test";
        IndexConfig indexConfig = createDefaultIndexConfig(indexName, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(indexName);

        final long baseTimestamp = System.currentTimeMillis();
        final long samplesIntervalMillis = 10000L;
        final AtomicInteger numSamplesIndexed = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);

        // ingest 1 very old sample, outside of the ooo cut_off window
        String toBeEmptyLabel = "pod_test";
        TimeSeriesSample firstSample = new TimeSeriesSample(
            Instant.ofEpochMilli(baseTimestamp - 48 * 60 * 60 * 1000),
            0.0,
            Map.of("metric", "cpu_usage", "instance", toBeEmptyLabel, "env", "prod")
        );
        ingestSamples(List.of(firstSample), indexName);

        // ingest 1 sample near the base timestamp
        TimeSeriesSample secondSample = new TimeSeriesSample(
            Instant.ofEpochMilli(baseTimestamp - 1000),
            0.0,
            Map.of("metric", "cpu_usage", "instance", "pod_0", "env", "prod")
        );
        ingestSamples(List.of(secondSample), indexName);

        // add background indexing traffic
        Thread indexingThread = new Thread(() -> {
            while (finished.get() == false && numSamplesIndexed.get() < 10_000) {
                long timestamp = baseTimestamp + (numSamplesIndexed.get() * samplesIntervalMillis);
                TimeSeriesSample sample = new TimeSeriesSample(
                    Instant.ofEpochMilli(timestamp),
                    numSamplesIndexed.get() * 1.0,
                    Map.of("metric", "cpu_usage", "instance", "pod" + (numSamplesIndexed.get() % 5), "env", "prod")
                );
                try {
                    ingestSamples(List.of(sample), indexName);
                    numSamplesIndexed.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        indexingThread.start();

        // ingest 1 sample with the base timestamp and toBeEmptyLabel, if drop empty series is allowed during recovery,
        // this sample would be dropped during recovery since the series label is empty
        TimeSeriesSample newSample = new TimeSeriesSample(
            Instant.ofEpochMilli(baseTimestamp),
            0.0,
            Map.of("metric", "cpu_usage", "instance", toBeEmptyLabel, "env", "prod")
        );
        ingestSamples(List.of(newSample), indexName);

        // Configure recovery to replay translog operations one at a request batch with minimal chunk size
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("indices.recovery.chunk_size", "1b"))
            .get();

        // add replica to trigger peer recovery
        // Intercept on primary when sending seqNo 2 and do a force flush
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting primaryShard = state.routingTable().index(indexName).shard(0).primaryShard();
        String primaryNodeName = state.nodes().get(primaryShard.currentNodeId()).getName();

        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNodeName
        );
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.TRANSLOG_OPS)) {
                RecoveryTranslogOperationsRequest translogRequest = (RecoveryTranslogOperationsRequest) request;
                for (Translog.Operation op : translogRequest.operations()) {
                    if (op.seqNo() == 2) {
                        logger.info("--> intercepting translog operation with seqNo 2, doing a force flush");
                        client().admin().indices().prepareFlush(indexName).setForce(true).get();
                        break;
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();

        // wait for the indexing thread to finish
        finished.set(true);
        indexingThread.join();
        refresh(indexName);
        ensureGreen(indexName);

        // validate the recovery
        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, numSamplesIndexed.get() + 1);
    }

}
