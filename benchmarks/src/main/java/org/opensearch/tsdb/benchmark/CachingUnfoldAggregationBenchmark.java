package org.opensearch.tsdb.benchmark;

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
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.tsdb.lang.m3.stage.CopyStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.UnionStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for performance implication regarding caching the unfold aggregation
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = { "-Xms4g", "-Xmx4g", "-XX:+HeapDumpOnOutOfMemoryError" })
public class CachingUnfoldAggregationBenchmark extends BaseTSDBBenchmark {
    @Param({"100", "10000" })
    public int cardinality;

    @Param({"20" })
    public int sampleCount;

    @Param({ "20" })
    public int labelCount;

    private GlobalAggregationBuilder aggregationBuilderNoCache;
    private GlobalAggregationBuilder aggregationBuilderWithCache;
    private GlobalAggregationBuilder baselineUnfold;
    private GlobalAggregationBuilder UnfoldWithCopy;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        setupBenchmark(this.cardinality, this.sampleCount, this.labelCount);
        aggregationBuilderNoCache = new GlobalAggregationBuilder("aggregation_no_cache");
        aggregationBuilderNoCache
                .subAggregation(new TimeSeriesUnfoldAggregationBuilder(
                        "unfold_a",
                        List.of(new ScaleStage(2.0)),
                        MIN_TS,
                        maxTs,
                        STEP
                ))
                .subAggregation(new TimeSeriesUnfoldAggregationBuilder(
                        "unfold_b",
                        List.of(new ScaleStage(2.0)),
                        MIN_TS,
                        maxTs,
                        STEP
                ))
                .subAggregation(new TimeSeriesCoordinatorAggregationBuilder(
                        "final",
                        List.of(new UnionStage("unfold_b")),
                        new LinkedHashMap<>(),
                        Map.of("unfold_a", "unfold_a", "unfold_b", "unfold_b"),
                        "unfold_a"
                ));

        aggregationBuilderWithCache = new GlobalAggregationBuilder("aggregation_with_cache");
        aggregationBuilderWithCache
                .subAggregation(new TimeSeriesUnfoldAggregationBuilder(
                        "unfold_a",
                        List.of(new ScaleStage(2.0)),
                        MIN_TS,
                        maxTs,
                        STEP
                ))
                .subAggregation(new TimeSeriesCoordinatorAggregationBuilder(
                        "copy_a",
                        List.of(new CopyStage()),
                        new LinkedHashMap<>(),
                        Map.of("unfold_a", "unfold_a"),
                        "unfold_a"
                ))
                .subAggregation(new TimeSeriesCoordinatorAggregationBuilder(
                        "final",
                        List.of(new UnionStage("unfold_b")),
                        new LinkedHashMap<>(),
                        Map.of("unfold_a", "unfold_a", "unfold_b", "copy_a"),
                        "unfold_a"
                ));

        baselineUnfold = new GlobalAggregationBuilder("baseline_unfold");
        baselineUnfold
                .subAggregation(new TimeSeriesUnfoldAggregationBuilder(
                        "unfold_a",
                        List.of(new ScaleStage(2.0)),
                        MIN_TS,
                        maxTs,
                        STEP
                ));
        UnfoldWithCopy = new GlobalAggregationBuilder("unfold_copy");
        UnfoldWithCopy
                .subAggregation(new TimeSeriesUnfoldAggregationBuilder(
                        "unfold_a",
                        List.of(new ScaleStage(2.0)),
                        MIN_TS,
                        maxTs,
                        STEP
                ))
                .subAggregation(new TimeSeriesCoordinatorAggregationBuilder(
                        "copy_a",
                        List.of(new CopyStage()),
                        new LinkedHashMap<>(),
                        Map.of("unfold_a", "unfold_a"),
                        "unfold_a"
                ));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tearDownBenchmark();
    }

    @TearDown(Level.Invocation)
    public void cleanUp() {
        afterEachInvocation();
    }

    @Benchmark
    public void benchmarkWithoutCaching(Blackhole blackhole) throws IOException {
        blackhole.consume(searchAndReduce(indexSearcher, createSearchContext(indexSearcher, createIndexSettings(), query, createBucketConsumer()), rewritten, aggregationBuilderNoCache));
    }

    @Benchmark
    public void benchmarkWithCaching(Blackhole blackhole) throws IOException {
        blackhole.consume(searchAndReduce(indexSearcher, createSearchContext(indexSearcher, createIndexSettings(), query, createBucketConsumer()), rewritten, aggregationBuilderWithCache));
    }

    @Benchmark
    public void benchmarkUnfoldBaseline(Blackhole blackhole) throws IOException {
        blackhole.consume(searchAndReduce(indexSearcher, createSearchContext(indexSearcher, createIndexSettings(), query, createBucketConsumer()), rewritten, baselineUnfold));
    }

    @Benchmark
    public void benchmarkUnfoldAndCopy(Blackhole blackhole) throws IOException {
        blackhole.consume(searchAndReduce(indexSearcher, createSearchContext(indexSearcher, createIndexSettings(), query, createBucketConsumer()), rewritten, UnfoldWithCopy));
    }
}
