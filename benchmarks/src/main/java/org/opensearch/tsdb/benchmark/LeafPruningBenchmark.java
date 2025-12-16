/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.DirectoryReaderWithMetadata;
import org.opensearch.tsdb.core.reader.TSDBDirectoryReader;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregatorFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Benchmark for leaf pruning in TSDBDirectoryReader with non-overlapping time ranges.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LeafPruningBenchmark extends BaseTSDBBenchmark {

    @Param({ "24" })
    public int numClosedChunkIndexes;

    @Param({ "100000" })
    public int cardinality;

    @Param({ "10" })
    public int sampleCount;

    @Param({ "single", "half", "all" })
    public String queryScenario;

    private List<ClosedChunkIndex> closedChunkIndexes;
    private List<Path> tempDirs;
    private TSDBDirectoryReader tsdbReader;
    private List<Releasable> releasables;
    private TimeSeriesUnfoldAggregator aggregator;

    private static final long TIME_RANGE_PER_INDEX = 1_000_000L;
    private static final long BASE_START_TIME = 1_000_000_000L;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDirs = new ArrayList<>();
        closedChunkIndexes = new ArrayList<>();
        releasables = new ArrayList<>();

        setupBenchmark(1, 1, 1);

        for (int i = 0; i < numClosedChunkIndexes; i++) {
            Path tempDir = Files.createTempDirectory("jmh-pruning-benchmark-" + i);
            tempDirs.add(tempDir);

            long indexMinTime = BASE_START_TIME + (i * TIME_RANGE_PER_INDEX);
            long indexMaxTime = indexMinTime + TIME_RANGE_PER_INDEX - 1;

            ClosedChunkIndex index = new ClosedChunkIndex(
                tempDir,
                new ClosedChunkIndex.Metadata(tempDir.getFileName().toString(), indexMinTime, indexMaxTime),
                TimeUnit.MILLISECONDS,
                Settings.EMPTY
            );

            // Index time series data within this time range
            indexTimeSeriesForRange(index, cardinality, sampleCount, indexMinTime, indexMaxTime, i);
            index.commitWithMetadata(List.of());
            index.getDirectoryReaderManager().maybeRefresh();

            closedChunkIndexes.add(index);
        }

        List<DirectoryReaderWithMetadata> closedReaders = new ArrayList<>();
        for (int i = 0; i < closedChunkIndexes.size(); i++) {
            ClosedChunkIndex index = closedChunkIndexes.get(i);
            DirectoryReader reader = index.getDirectoryReaderManager().acquire();
            releasables.add(() -> {
                try {
                    index.getDirectoryReaderManager().release(reader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            ClosedChunkIndex.Metadata metadata = index.getMetadata();
            closedReaders.add(new DirectoryReaderWithMetadata(reader, metadata.minTimestamp(), metadata.maxTimestamp()));
        }

        // Using ClosedChunkIndex for live reader simplifies setup without affecting pruning behavior
        Path liveDir = Files.createTempDirectory("jmh-pruning-benchmark-live");
        tempDirs.add(liveDir);
        ClosedChunkIndex liveIndex = new ClosedChunkIndex(
            liveDir,
            new ClosedChunkIndex.Metadata("live", 0, Long.MAX_VALUE),
            TimeUnit.MILLISECONDS,
            Settings.EMPTY
        );
        liveIndex.commitWithMetadata(List.of());
        liveIndex.getDirectoryReaderManager().maybeRefresh();
        DirectoryReader liveReader = liveIndex.getDirectoryReaderManager().acquire();
        closedChunkIndexes.add(liveIndex);
        releasables.add(() -> {
            try {
                liveIndex.getDirectoryReaderManager().release(liveReader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        tsdbReader = new TSDBDirectoryReader(
            liveReader,
            () -> BASE_START_TIME + ((numClosedChunkIndexes - 1) * TIME_RANGE_PER_INDEX), // live reader bounded to the most recent chunk
            closedReaders,
            null,
            new HashMap<>(),
            null,
            LabelStorageType.BINARY,
            0L

        );
        indexSearcher = new IndexSearcher(tsdbReader);

        // Set cache size to ensure cache hit rate will be low
        LRUQueryCache queryCache = new LRUQueryCache(10, 10 * 1024); // 10 queries, 10KB max
        indexSearcher.setQueryCache(queryCache);
    }

    @Setup(Level.Invocation)
    public void setupQuery() throws IOException {
        long queryStartTime;
        long queryEndTime;

        switch (queryScenario) {
            case "single":
                queryStartTime = BASE_START_TIME;
                queryEndTime = BASE_START_TIME + TIME_RANGE_PER_INDEX;
                break;
            case "half":
                int halfIndexes = Math.max(1, numClosedChunkIndexes / 2);
                queryStartTime = BASE_START_TIME;
                queryEndTime = BASE_START_TIME + (halfIndexes * TIME_RANGE_PER_INDEX);
                break;
            case "all":
            default:
                queryStartTime = BASE_START_TIME;
                queryEndTime = BASE_START_TIME + (numClosedChunkIndexes * TIME_RANGE_PER_INDEX);
                break;
        }

        Random random = new Random();
        String m3Query;
        if (random.nextInt(10) < 9) {
            // 90% queries: non-existent labels to force term collection without matches
            int nonExistentJob = 100 + random.nextInt(100000);
            m3Query = String.format("fetch job:job_%d*", nonExistentJob);
        } else {
            // 10% queries: match ~5 docs in a single index
            int jobNum = random.nextInt(100);
            int statusNum = random.nextInt(20);
            int indexNum = random.nextInt(numClosedChunkIndexes);
            m3Query = String.format("fetch job:job_%d status:status_%d index_id:idx_%d", jobNum, statusNum, indexNum);
        }

        long actualQueryStart = queryStartTime + random.nextInt((int) TIME_RANGE_PER_INDEX / 3);
        long actualQueryEnd = queryEndTime - random.nextInt((int) TIME_RANGE_PER_INDEX / 3);

        SearchSourceBuilder searchSourceBuilder = M3OSTranslator.translate(
            m3Query,
            new M3OSTranslator.Params(TimeUnit.MILLISECONDS, actualQueryStart, actualQueryEnd, 1000L, true, false, null)
        );

        QueryBuilder queryBuilder = searchSourceBuilder.query();
        QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(searchContext.getQueryShardContext());
        query = rewrittenQueryBuilder.toQuery(searchContext.getQueryShardContext());
        rewritten = indexSearcher.rewrite(query);

        TimeSeriesUnfoldAggregatorFactory factory = new TimeSeriesUnfoldAggregatorFactory(
            "unfold",
            searchContext.getQueryShardContext(),
            null,
            AggregatorFactories.builder(),
            Collections.emptyMap(),
            List.of(),
            actualQueryStart,
            actualQueryEnd,
            1000L
        );

        aggregator = (TimeSeriesUnfoldAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.ONE,
            Collections.emptyMap()
        );
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (tsdbReader != null) {
            tsdbReader.close();
        }

        Releasables.close(releasables);
        releasables.clear();

        for (ClosedChunkIndex index : closedChunkIndexes) {
            index.close();
        }
        closedChunkIndexes.clear();

        for (Path tempDir : tempDirs) {
            deleteDirectory(tempDir);
        }
        tempDirs.clear();

        tearDownBenchmark();
    }

    /**
     * Benchmark query execution with pruning
     */
    @Benchmark
    public void benchmarkQueryWithPruning(Blackhole bh) throws IOException {
        aggregator.preCollection();
        indexSearcher.search(rewritten, aggregator);
        aggregator.postCollection();

        InternalAggregation result = aggregator.buildTopLevel();
        InternalAggregation.ReduceContext context = createReduceContext(aggregator);
        InternalAggregation reduced = result.reduce(List.of(result), context);
        bh.consume(reduced);
    }

    /**
     * Index time series data within a specific time range for one ClosedChunkIndex
     */
    private void indexTimeSeriesForRange(
        ClosedChunkIndex index,
        int cardinality,
        int sampleCount,
        long minTime,
        long maxTime,
        int indexNumber
    ) throws IOException {
        long timeStep = (maxTime - minTime) / sampleCount;

        for (int i = 0; i < cardinality; i++) {
            Map<String, String> labelsMap = new HashMap<>();
            labelsMap.put("__name__", "http_requests_total");
            labelsMap.put("job", "job_" + (i % 100));
            labelsMap.put("instance", "instance_" + i);
            labelsMap.put("status", "status_" + (i % 20));
            labelsMap.put("method", "method_" + (i % 10));
            labelsMap.put("path", "/api/v1/endpoint_" + (i % 50));
            labelsMap.put("region", "region_" + (i % 10));
            labelsMap.put("az", "az_" + (i % 5));
            labelsMap.put("index_id", "idx_" + indexNumber);
            Labels labels = ByteLabels.fromMap(labelsMap);

            MemChunk memChunk = new MemChunk(0, minTime, maxTime, null, Encoding.XOR);
            for (int j = 0; j < sampleCount; j++) {
                long timestamp = minTime + (j * timeStep);
                double value = BASE_SAMPLE_VALUE * j + 1;
                memChunk.append(timestamp, value, 0L);
            }
            index.addNewChunk(labels, memChunk);
        }
    }

    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory).sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    System.err.println("Failed to delete " + path + ": " + e.getMessage());
                }
            });
        }
    }

    private InternalAggregation.ReduceContext createReduceContext(TimeSeriesUnfoldAggregator aggregator) {
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        return InternalAggregation.ReduceContext.forFinalReduction(
            aggregator.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    @Override
    protected MapperService mapperServiceMock(SearchContext searchContext, IndexSettings indexSettings) {
        MapperService mapperService = super.mapperServiceMock(searchContext, indexSettings);

        // Add TSDB field mappings so rewrite works correctly
        KeywordFieldMapper.KeywordFieldType labelsFieldType = new KeywordFieldMapper.KeywordFieldType(Constants.IndexSchema.LABELS);

        RangeFieldMapper.RangeFieldType timestampRangeFieldType = new RangeFieldMapper.RangeFieldType(
            Constants.IndexSchema.TIMESTAMP_RANGE,
            RangeType.LONG
        );

        // Use anyString() to catch all field lookups and return appropriate field types
        when(mapperService.fieldType(anyString())).thenAnswer(invocation -> {
            String fieldName = invocation.getArgument(0);
            return switch (fieldName) {
                case Constants.IndexSchema.LABELS -> labelsFieldType;
                case Constants.IndexSchema.TIMESTAMP_RANGE -> timestampRangeFieldType;
                default -> null;
            };
        });

        return mapperService;
    }
}
