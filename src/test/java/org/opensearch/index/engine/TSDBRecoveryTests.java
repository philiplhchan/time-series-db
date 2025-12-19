/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.utils.TSDBTestUtils;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.opensearch.tsdb.core.mapping.Constants.Mapping.DEFAULT_INDEX_MAPPING;

/**
 * Multi-node replication tests for TSDBEngine to verify thread safety during peer recovery and concurrent operations.
 *
 * This corresponds to the RecoveryDuringReplicationTests for the default engine. TODO: expand test coverage to match
 */
public class TSDBRecoveryTests extends OpenSearchIndexLevelReplicationTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Recreate thread pool with mgmt executor
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = new TestThreadPool(
            getClass().getName(),
            new FixedExecutorBuilder(Settings.EMPTY, TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 100, "tsdb.mgmt")
        );

        // Register dynamic settings that are consumed by a settings updater
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
    }

    public void testIndexingDuringRecovery() throws Exception {
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.enabled", true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL.getKey(), TimeValue.timeValueSeconds(60))
            .build();

        ReplicationGroup shards = createGroup(0, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();

            // Index first batch of samples
            int firstBatchDocs = indexSamples(shards, 1000, 0);
            shards.flush(); // Flush for consistency with RecoveryDuringReplicationTests (todo: is it needed?)
            logger.info("Indexed first batch of {} documents", firstBatchDocs);

            // Add a replica
            IndexShard replica = shards.addReplica();

            // Setup blocking recovery
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);

            // Start async recovery, blocking at a random stage
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(
                replica,
                (indexShard, node) -> new BlockingTarget(
                    randomFrom(BlockingTarget.SUPPORTED_STAGES),
                    recoveryBlocked,
                    releaseRecovery,
                    indexShard,
                    node,
                    recoveryListener,
                    logger,
                    threadPool
                )
            );

            // Wait for recovery to reach blocking stage
            recoveryBlocked.await();

            // Index second batch of samples while blocked
            int secondBatchDocs = indexSamples(shards, 1000, firstBatchDocs);

            // Release recovery to continue
            releaseRecovery.countDown();

            // Wait for recovery to complete
            recoveryFuture.get();
            logger.info("Recovery completed");

            // Verify all documents are present on both primary and replica by counting samples in TSDB engine
            int totalDocs = firstBatchDocs + secondBatchDocs;

            // Flush for consistency with RecoveryDuringReplicationTests (todo: is it needed?)
            shards.flush();
            shards.refresh("test");

            // Verify sample counts match between primary and replica
            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            long primarySampleCount = TSDBTestUtils.countSamples(primaryEngine);
            assertEquals("Primary should have all samples", totalDocs, primarySampleCount);

            for (IndexShard replicaShard : shards.getReplicas()) {
                TSDBEngine replicaEngine = (TSDBEngine) getEngine(replicaShard);
                long replicaSampleCount = TSDBTestUtils.countSamples(replicaEngine);
                assertEquals("Replica should have same sample count as primary", primarySampleCount, replicaSampleCount);
            }

            logger.info("Test completed successfully. Total docs indexed: {}, sample count verified: {}", totalDocs, primarySampleCount);
        } finally {
            // Manually close shards to avoid primary term doc values check in ReplicationGroup.close(),
            // which isn't supported by the TSDBDirectoryReader used by TSDBEngine
            closeShards(shards.getPrimary());
            for (IndexShard replica : shards.getReplicas()) {
                closeShards(replica);
            }
        }
    }

    /**
     * Tests that the translog retention lock prevents trimming during peer recovery Phase 1.
     *
     * This test validates that acquireHistoryRetentionLock() protects translog generations
     * from being deleted during recovery, even when trimTranslog() is explicitly called.
     */
    public void testFlushDuringPhase1DoesNotTrimTranslog() throws Exception {
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.enabled", true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        ReplicationGroup shards = createGroup(0, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();
            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            InternalTranslogManager primaryTranslogManager = (InternalTranslogManager) primaryEngine.translogManager();

            // Index first batch
            int firstBatchDocs = indexSamples(shards, 100, 0);
            long generationBeforeRoll = primaryTranslogManager.getTranslogGeneration().translogFileGeneration;

            // Roll translog to create generation 2
            primaryEngine.translogManager().rollTranslogGeneration();
            long generationAfterRoll = primaryTranslogManager.getTranslogGeneration().translogFileGeneration;
            assertEquals(generationBeforeRoll + 1, generationAfterRoll);
            assertEquals(generationBeforeRoll, primaryTranslogManager.getTranslog().getMinFileGeneration());

            // Index second batch into new generation
            int secondBatchDocs = indexSamples(shards, 100, firstBatchDocs);
            // Set local checkpoint of safe commit to a value in the second batch
            // This makes the first generation eligible for deletion
            primaryEngine.translogManager()
                .getDeletionPolicy()
                .setLocalCheckpointOfSafeCommit(randomLongBetween(firstBatchDocs + 1, firstBatchDocs + secondBatchDocs));
            primaryEngine.translogManager().syncTranslog();

            // Start async recovery blocked at INDEX stage
            IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);

            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(
                replica,
                (indexShard, node) -> new BlockingTarget(
                    RecoveryState.Stage.INDEX,
                    recoveryBlocked,
                    releaseRecovery,
                    indexShard,
                    node,
                    recoveryListener,
                    logger,
                    threadPool
                )
            );

            // Wait for recovery to block at Phase 1
            recoveryBlocked.await();
            logger.info("Recovery blocked at INDEX stage");

            // Trigger translog trimming while recovery is in progress
            // The retention lock should prevent deletion of the first generation
            shards.getPrimary().trimTranslog();
            primaryTranslogManager.syncTranslog();

            // Verify first generation is still present (protected by retention lock)
            assertEquals(generationBeforeRoll, primaryTranslogManager.getTranslog().getMinFileGeneration());

            // Release recovery to continue
            releaseRecovery.countDown();
            logger.info("Released recovery to continue");

            // Wait for recovery to complete
            recoveryFuture.get();
            logger.info("Recovery completed successfully");

            // Verify replica has all samples
            shards.refresh("test");
            long primarySampleCount = TSDBTestUtils.countSamples(primaryEngine);
            for (IndexShard replicaShard : shards.getReplicas()) {
                TSDBEngine replicaEngine = (TSDBEngine) getEngine(replicaShard);
                long replicaSampleCount = TSDBTestUtils.countSamples(replicaEngine);
                assertEquals("Replica should have same sample count as primary", primarySampleCount, replicaSampleCount);
            }
        } finally {
            closeShards(shards.getPrimary());
            for (IndexShard replica : shards.getReplicas()) {
                closeShards(replica);
            }
        }
    }

    /**
     * Index sample documents into the primary shard.
     *
     * @param shards The replication group
     * @param count Number of samples to index
     * @param startIdx Starting index for sample IDs
     * @return Number of documents indexed
     */
    private int indexSamples(ReplicationGroup shards, int count, int startIdx) throws Exception {
        long baseTimestamp = 1633072800000L;
        Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");

        for (int i = 0; i < count; i++) {
            int sampleId = startIdx + i;
            long timestamp = baseTimestamp + (sampleId * 10000L); // 10 second intervals
            double value = sampleId * 1.0;

            // Create sample JSON using TSDBTestUtils
            String id = String.valueOf(sampleId);
            String source = TSDBTestUtils.createSampleJson(labels, timestamp, value);

            shards.index(new IndexRequest("test").id(id).source(source, MediaTypeRegistry.JSON));
        }

        return count;
    }

    /**
     * BlockingTarget that pauses recovery at a specific stage to allow controlled concurrent operations.
     * This is the same pattern used in RecoveryDuringReplicationTests.
     */
    public static class BlockingTarget extends RecoveryTarget {
        private final CountDownLatch recoveryBlocked;
        private final CountDownLatch releaseRecovery;
        private final RecoveryState.Stage stageToBlock;
        static final EnumSet<RecoveryState.Stage> SUPPORTED_STAGES = EnumSet.of(
            RecoveryState.Stage.INDEX,
            RecoveryState.Stage.TRANSLOG,
            RecoveryState.Stage.FINALIZE
        );
        private final Logger logger;

        public BlockingTarget(
            RecoveryState.Stage stageToBlock,
            CountDownLatch recoveryBlocked,
            CountDownLatch releaseRecovery,
            IndexShard shard,
            DiscoveryNode sourceNode,
            ReplicationListener listener,
            Logger logger,
            ThreadPool threadPool
        ) {
            super(shard, sourceNode, listener, threadPool);
            this.recoveryBlocked = recoveryBlocked;
            this.releaseRecovery = releaseRecovery;
            this.stageToBlock = stageToBlock;
            this.logger = logger;
            if (SUPPORTED_STAGES.contains(stageToBlock) == false) {
                throw new UnsupportedOperationException(stageToBlock + " is not supported");
            }
        }

        private boolean hasBlocked() {
            return recoveryBlocked.getCount() == 0;
        }

        private void blockIfNeeded(RecoveryState.Stage currentStage) {
            if (currentStage == stageToBlock) {
                logger.info("--> blocking recovery on stage [{}]", currentStage);
                recoveryBlocked.countDown();
                try {
                    releaseRecovery.await();
                    logger.info("--> recovery continues from stage [{}]", currentStage);
                } catch (InterruptedException e) {
                    throw new RuntimeException("blockage released");
                }
            }
        }

        @Override
        public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxAutoIdTimestamp,
            final long maxSeqNoOfUpdates,
            final RetentionLeases retentionLeases,
            final long mappingVersion,
            final ActionListener<Long> listener
        ) {
            if (hasBlocked() == false) {
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            super.indexTranslogOperations(
                operations,
                totalTranslogOps,
                maxAutoIdTimestamp,
                maxSeqNoOfUpdates,
                retentionLeases,
                mappingVersion,
                listener
            );
        }

        @Override
        public void cleanFiles(
            int totalTranslogOps,
            long globalCheckpoint,
            org.opensearch.index.store.Store.MetadataSnapshot sourceMetadata,
            ActionListener<Void> listener
        ) {
            blockIfNeeded(RecoveryState.Stage.INDEX);
            super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener);
        }

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
            if (hasBlocked() == false) {
                // it maybe that no ops have been transferred, block now
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            blockIfNeeded(RecoveryState.Stage.FINALIZE);
            super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
        }
    }
}
