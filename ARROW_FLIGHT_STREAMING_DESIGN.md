# Arrow Flight Streaming for TSDB Aggregators

## What This Document Covers

This document explains how OpenSearch's Arrow Flight streaming aggregation framework works, and proposes how the TSDB plugin's aggregators can integrate with it. It's written to be understandable without deep prior knowledge of the streaming internals.

---

## 1. Background: How Aggregation Normally Works

When you run a query with aggregations in OpenSearch, the lifecycle looks like this:

1. **Collect phase** — The aggregator visits every matching document across all segments (a segment is a chunk of the Lucene index on a shard). State accumulates across segments.
2. **Post-collection** — Called once after all segments are done. The aggregator finalizes its internal data.
3. **Build** — `buildTopLevel()` produces one `InternalAggregation` result per shard.
4. **Reduce** — The coordinator node receives one result from each shard and merges them via `InternalAggregation.reduce()`.

This means each shard sends **one big result** to the coordinator after processing all its data. For large datasets, this creates memory pressure on both the shard (holding accumulated state) and the coordinator (receiving large payloads).

**Reference files (OpenSearch core):**
- `server/.../search/aggregations/Aggregator.java:212-216` — `buildTopLevel()`
- `server/.../search/aggregations/BucketCollectorProcessor.java:61-87` — `processPostCollection()`

---

## 2. What Arrow Flight Streaming Changes

Arrow Flight is a gRPC-based transport that OpenSearch can use instead of its default Netty4 transport. When combined with **streaming aggregation**, it enables a fundamentally different execution model.

### 2.1 PER_SEGMENT Streaming (True Streaming)

Instead of accumulating state across all segments and sending one result, the shard processes each segment independently:

```
Segment 1: collect → postCollection → buildTopLevelBatch → sendBatch → reset
Segment 2: collect → postCollection → buildTopLevelBatch → sendBatch → reset
Segment 3: collect → postCollection → buildTopLevelBatch → sendBatch → reset
...
Coordinator: receives N partial results → reduce/merge them all
```

The key code that triggers this lives in `ContextIndexSearcher.collectLeaf()`:

```java
// ContextIndexSearcher.java:399-408
// Called at the END of each leaf/segment collection
if (searchContext.isStreamSearch()
    && searchContext.getFlushMode() == FlushMode.PER_SEGMENT) {
    List<InternalAggregation> internalAggregation =
        searchContext.bucketCollectorProcessor().buildAggBatch(collector);
    if (!internalAggregation.isEmpty()) {
        sendBatch(internalAggregation);  // stream partial result over Arrow Flight
    }
}
```

Compare this to the **non-streaming path**, which runs only once after ALL segments:

```java
// BucketCollectorProcessor.java:61-87 (non-streaming)
public void processPostCollection(Collector collectorTree) {
    // ... unwrap collector tree ...
    if (currentCollector instanceof Aggregator aggregator) {
        bucketCollector.postCollection();  // finalize ALL accumulated data
        aggregator.buildTopLevel();        // build final result ONCE — no reset()
    }
}
```

And the **streaming path**, which runs after EACH segment:

```java
// BucketCollectorProcessor.java:93-123 (streaming)
public List<InternalAggregation> buildAggBatch(Collector collectorTree) {
    // ... unwrap collector tree ...
    if (currentCollector instanceof Aggregator aggregator) {
        bucketCollector.postCollection();             // finalize THIS segment's data
        aggregations.add(aggregator.buildTopLevelBatch());  // build + RESET
    }
}
```

The critical distinction is in the build methods:

```java
// Aggregator.java:212-216 — NON-STREAMING
public final InternalAggregation buildTopLevel() {
    this.internalAggregation.set(buildAggregations(new long[]{0})[0]);
    return internalAggregation.get();
    // NO reset() — aggregator is done, will be GC'd
}

// Aggregator.java:222-227 — STREAMING
public final InternalAggregation buildTopLevelBatch() {
    InternalAggregation batch = buildAggregations(new long[]{0})[0];
    reset();   // ← clears state so next segment starts fresh
    return batch;
}
```

### 2.2 Summary: Streaming vs Non-Streaming

| Aspect | Normal (Non-Streaming) | PER_SEGMENT Streaming |
|--------|------------------------|-----------------------|
| Results per shard | 1 | N (one per segment) |
| State lifetime | Entire shard | Single segment |
| Memory per shard | All segments accumulated | Only current segment |
| Build method | `buildTopLevel()` | `buildTopLevelBatch()` |
| `reset()` called? | No | Yes, after each segment |
| Transport | Netty4 (single response) | Arrow Flight gRPC (streamed batches) |

### 2.3 PER_SHARD (Not True Streaming)

`FlushMode.PER_SHARD` uses Arrow Flight as transport but does NOT stream per-segment. It behaves like normal aggregation — one result per shard, accumulated across all segments. This mode exists as a fallback when per-segment streaming isn't safe or cost-effective.

**Reference files (OpenSearch core):**
- `server/.../search/aggregations/Aggregator.java:222-227` — `buildTopLevelBatch()`
- `server/.../search/aggregations/AggregatorBase.java:321-326` — `reset()` cascades to sub-aggs
- `server/.../search/internal/ContextIndexSearcher.java:399-413` — PER_SEGMENT trigger
- `server/.../search/aggregations/BucketCollectorProcessor.java:93-123` — `buildAggBatch()`

---

## 3. How Serialization Works (It's Simpler Than You Think)

The "Arrow" in Arrow Flight might suggest that data must be converted into columnar Apache Arrow format. In practice, OpenSearch uses a **binary-wrapping approach**:

- Existing `StreamOutput` serialization (the same bytes your aggregators already produce) is wrapped inside a `VarBinaryVector` column in a `VectorSchemaRoot`
- Classes: `VectorStreamOutput` and `VectorStreamInput`
- **No new serialization code needed** — if your `InternalAggregation` already implements `writeTo(StreamOutput)` and has a constructor from `StreamInput`, it works with Arrow Flight

This means `InternalTimeSeries` already has the serialization it needs.

**Reference files (OpenSearch core):**
- `plugins/arrow-flight-rpc/.../VectorStreamOutput.java` — Wraps bytes into Arrow vectors
- `plugins/arrow-flight-rpc/.../VectorStreamInput.java` — Unwraps Arrow vectors back to bytes

---

## 4. The Two Gates: What Controls Whether Streaming Is Used

Before an aggregator can use PER_SEGMENT streaming, it must pass two independent checks.

### 4.1 Gate 1: `FlushModeResolver.isStreamable()` — The Hardcoded Allowlist

This is a static check on the **AggregationBuilder** tree (before aggregators are created). It currently hardcodes:

```java
// FlushModeResolver.java — simplified
static boolean isStreamable(AggregationBuilder builder) {
    // Top-level MUST be TermsAggregationBuilder
    if (!(builder instanceof TermsAggregationBuilder)) return false;
    // Sub-aggs must be Terms, Cardinality, Max, Min, or Sum
    for (AggregationBuilder sub : builder.getSubAggregations()) {
        if (!isAllowedSubAgg(sub)) return false;
    }
    return true;
}
```

**This is the blocker for TSDB.** `TimeSeriesUnfoldAggregationBuilder` is not `TermsAggregationBuilder`, so streaming is rejected before any TSDB code runs.

### 4.2 Gate 2: `StreamingCostEstimable` Interface — The Extensible Check

If Gate 1 passes, the framework walks the `AggregatorFactory` tree and calls `estimateStreamingCost()` on each factory. This returns a `StreamingCostMetrics` record:
- `streamable` (boolean) — is this factory safe to stream?
- `topNSize` (int) — estimated number of buckets per segment

The framework multiplies parent × child topN sizes to estimate total cost, then compares against `maxBucketCount` to decide PER_SEGMENT vs PER_SHARD.

**This gate IS extensible** — any plugin can implement `StreamingCostEstimable` on its factory.

### 4.3 Where The Decisions Happen

```java
// AggregatorFactories.java:309-333 — createTopLevelAggregators()
if (isStreamSearch) {
    StreamingCostMetrics metrics = StreamingCostMetrics.estimateFromFactories(factories, context);
    FlushMode mode = FlushModeResolver.decideFlushMode(metrics, context);
    context.setFlushModeIfAbsent(mode);
}
```

### 4.4 Historical Context: PR #20471

Before PR #20471, the streaming decision happened **after** aggregators were created, using a `Streamable` interface on the aggregator instances. PR #20471 moved this to **before** creation, using `StreamingCostEstimable` on factories. The motivation:
- Creating streaming aggregators is expensive if you later decide not to stream
- Factory-level checking is cheaper and happens earlier
- Cost estimation allows smarter decisions (e.g., skip streaming for low-cardinality fields)

**Reference files (OpenSearch core):**
- `server/.../search/streaming/FlushModeResolver.java` — Gate 1
- `server/.../search/streaming/StreamingCostEstimable.java` — Gate 2 interface
- `server/.../search/streaming/StreamingCostMetrics.java` — Cost estimation record
- `server/.../search/aggregations/AggregatorFactories.java:309-333` — Decision point

---

## 5. What a Streaming Aggregator Must Implement

Looking at existing streaming aggregators as reference patterns:

### 5.1 `doReset()` — Clear Per-Segment State

Every streaming aggregator overrides `doReset()` to clear its internal state between segments:

```java
// StreamStringTermsAggregator
@Override
protected void doReset() {
    valueCount = 0;
    sortedDocValuesPerBatch = null;
    leafCollectorCreated = false;
}

// StreamNumericTermsAggregator
@Override
protected void doReset() {
    if (bucketOrds != null) {
        Releasable.close(bucketOrds);
        bucketOrds = null;
    }
}

// StreamCardinalityAggregator (sub-agg)
@Override
protected void doReset() {
    Releasable.close(streamCollector);
    streamCollector = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
}
```

### 5.2 `StreamingCostEstimable` on the Factory

```java
// TermsAggregatorFactory implements StreamingCostEstimable
@Override
public StreamingCostMetrics estimateStreamingCost(SearchContext context) {
    // ... calculate topN based on field cardinality, segment count, etc.
    return new StreamingCostMetrics(true, segmentTopN);
}
```

### 5.3 Streaming-Specific Aggregator Creation (Optional)

Some factories create a different aggregator class for streaming:

```java
// TermsAggregatorFactory.createInternal()
if (context.isStreamSearch() && context.getFlushMode() == FlushMode.PER_SEGMENT) {
    return new StreamStringTermsAggregator(...);
} else {
    return new StringTermsAggregator(...);
}
```

This is optional — if the existing aggregator can handle both modes (with `doReset()`), a separate class isn't needed.

**Reference files (OpenSearch core):**
- `server/.../aggregations/bucket/terms/StreamStringTermsAggregator.java`
- `server/.../aggregations/bucket/terms/StreamNumericTermsAggregator.java`
- `server/.../aggregations/metrics/StreamCardinalityAggregator.java`
- `server/.../aggregations/bucket/terms/TermsAggregatorFactory.java:240-256, 605-728`

---

## 6. Why TSDB Can Benefit (The pushdown=false Case)

### 6.1 The Problem

When a TSDB query has `pushdown=false`, pipeline stages (like `rate()`, `avg_over_time()`, etc.) are deferred to the coordinator. The shard aggregator (`TimeSeriesUnfoldAggregator`) collects **raw samples** — every data point across all segments — and sends them to the coordinator.

For large time ranges or high-cardinality metrics, this means:
- The shard accumulates a massive `timeSeriesByBucket` map across all segments
- One enormous `InternalTimeSeries` result is built and serialized
- The coordinator receives these large payloads from every shard simultaneously

### 6.2 How Streaming Helps

With PER_SEGMENT streaming and `pushdown=false`:
- Each segment produces a **small partial `InternalTimeSeries`** with only that segment's samples
- Partial results are streamed to the coordinator as they're ready
- The shard only holds one segment's worth of data at a time
- The coordinator's `InternalTimeSeries.reduce()` already handles merging partial results by Labels using `SampleMerger`

### 6.3 Why This Is the Ideal First Use Case

1. **No pipeline stages run at the shard** (pushdown=false → stages list is empty) — nothing depends on cross-segment state
2. **Per-segment independence is already proven** — `supportsConcurrentSegmentSearch()` returns true when stages support CSS, and the empty-stages case trivially satisfies this
3. **The reduce path already works** — `InternalTimeSeries.reduce()` merges by Labels, which naturally handles N partial results the same way it handles N shard results

**Reference files (TSDB plugin):**
- `src/.../tsdb/query/aggregator/TimeSeriesUnfoldAggregator.java` — Shard-level aggregator
- `src/.../tsdb/query/aggregator/InternalTimeSeries.java:241-318` — `reduce()` with SampleMerger

---

## 7. Implementation Plan

### 7.1 Scope

- **Primary target**: `TimeSeriesUnfoldAggregator` (shard-level) for `pushdown=false` queries
- **Not in scope initially**: `TimeSeriesCoordinatorAggregator` (coordinator-level pipeline aggregator — unaffected by shard-level streaming)
- **Future extension**: `pushdown=true` with CSS-compatible stages

### 7.2 Step 1: Open Gate 1 — Make `FlushModeResolver` Extensible

**This requires a core OpenSearch change** (separate, minimal PR).

Add a `supportsStreaming()` method to `AggregationBuilder`:

```java
// AggregationBuilder.java (core)
public boolean supportsStreaming() {
    return false;  // default: not streamable
}
```

Modify `FlushModeResolver.isStreamable()` to check this method:

```java
// FlushModeResolver.java (core)
static boolean isStreamable(AggregationBuilder builder) {
    // Existing hardcoded path for Terms
    if (builder instanceof TermsAggregationBuilder) { ... }
    // NEW: extensible path for plugins
    if (builder.supportsStreaming()) {
        return builder.getSubAggregations().stream()
            .allMatch(sub -> sub.supportsStreaming() || isAllowedSubAgg(sub));
    }
    return false;
}
```

Then in the TSDB plugin, override it:

```java
// TimeSeriesUnfoldAggregationBuilder.java (TSDB plugin)
@Override
public boolean supportsStreaming() {
    return stages.isEmpty()
        || stages.stream().allMatch(UnaryPipelineStage::supportConcurrentSegmentSearch);
}
```

**Files to modify:**
| File | Repo | Change |
|------|------|--------|
| `AggregationBuilder.java` | core | Add default `supportsStreaming()` returning `false` |
| `FlushModeResolver.java` | core | Check `supportsStreaming()` in `isStreamable()` |
| `TimeSeriesUnfoldAggregationBuilder.java` | TSDB plugin | Override `supportsStreaming()` |

### 7.3 Step 2: Implement `StreamingCostEstimable` on Factory (Gate 2)

```java
// TimeSeriesUnfoldAggregatorFactory.java (TSDB plugin)
public class TimeSeriesUnfoldAggregatorFactory extends AggregatorFactory
    implements StreamingCostEstimable {

    @Override
    public StreamingCostMetrics estimateStreamingCost(SearchContext context) {
        if (stages != null && !stages.isEmpty()
            && !stages.stream().allMatch(
                UnaryPipelineStage::supportConcurrentSegmentSearch)) {
            return StreamingCostMetrics.nonStreamable();
        }
        // Neutral cost — no topN limiting needed for time series
        return StreamingCostMetrics.neutral();
    }
}
```

**Files to modify:**
| File | Change |
|------|--------|
| `TimeSeriesUnfoldAggregatorFactory.java` | Implement `StreamingCostEstimable` |

### 7.4 Step 3: Implement `doReset()` on the Aggregator

The aggregator must clear all per-segment state between segments:

```java
// TimeSeriesUnfoldAggregator.java (TSDB plugin)
@Override
protected void doReset() {
    timeSeriesByBucket.clear();
    processedTimeSeriesByBucket.clear();
    executionStats = new ExecutionStats();
    circuitBreakerBytes = 0;
    circuitBreakerBatcher.reset();
}
```

The `CircuitBreakerBatcher` also needs a `reset()` method:

```java
// CircuitBreakerBatcher.java
public void reset() {
    pending = 0;
}
```

**Files to modify:**
| File | Change |
|------|--------|
| `TimeSeriesUnfoldAggregator.java` | Add `doReset()` override |
| `CircuitBreakerBatcher.java` | Add `reset()` method |

### 7.5 Step 4: Verify Existing Code Works (No Changes Expected)

These should work without modification:

- **`buildAggregations()`** — Already builds `InternalTimeSeries` from whatever is in `timeSeriesByBucket`. In streaming mode, this map will contain only the current segment's data (because `doReset()` clears it between segments).
- **`InternalTimeSeries.reduce()`** — Already merges partial results by Labels using `SampleMerger`. When the coordinator receives N partial results (one per segment per shard), it merges them identically to how it merges N shard results.
- **`postCollection()`** — In `pushdown=false`, the stages list at the shard level is empty, so this is essentially a no-op. Safe for per-segment streaming.

---

## 8. What Doesn't Change

| Component | Why It's Unaffected |
|-----------|-------------------|
| `InternalTimeSeries` | Serialization (`writeTo`/`StreamInput` constructor) already works with Arrow Flight's binary wrapping. `reduce()` already handles partial merges. |
| `TimeSeriesCoordinatorAggregator` | Runs at coordinator after all shard results arrive. Unaffected by shard-level streaming. |
| Arrow serialization | No new Arrow vector formats needed. Existing binary wrapping handles everything. |
| `buildAggregations()` | Already builds from current state — works identically whether state is one segment or all segments. |

---

## 9. Verification Plan

### Unit Tests
- Test `doReset()` clears all state correctly (timeSeriesByBucket, stats, breaker)
- Test `buildAggregations()` after reset produces empty/correct result
- Test `estimateStreamingCost()` returns streamable for empty stages, non-streamable for non-CSS stages

### Integration Tests
- Run a pushdown=false TSDB query with Arrow Flight enabled
- Verify coordinator receives correct merged results
- Compare streaming vs non-streaming results for identical queries (must match exactly)
- Test with multiple segments per shard to exercise the per-segment lifecycle

### Correctness Validation
- For a known dataset, verify that streaming (N partial results merged) produces identical output to non-streaming (1 accumulated result)
- Test edge cases: empty segments, single-doc segments, segments with no matching time series

---

## 10. Summary of All Files to Modify

### Core OpenSearch (separate PR — minimal, backward-compatible)
| File | Change |
|------|--------|
| `AggregationBuilder.java` | Add `supportsStreaming()` default method returning `false` |
| `FlushModeResolver.java` | Check `supportsStreaming()` in `isStreamable()` |

### TSDB Plugin
| File | Change |
|------|--------|
| `TimeSeriesUnfoldAggregationBuilder.java` | Override `supportsStreaming()` |
| `TimeSeriesUnfoldAggregatorFactory.java` | Implement `StreamingCostEstimable` |
| `TimeSeriesUnfoldAggregator.java` | Add `doReset()` override |
| `CircuitBreakerBatcher.java` | Add `reset()` method |

### Not Modified
| File | Why |
|------|-----|
| `InternalTimeSeries.java` | Serialization and `reduce()` already work |
| `TimeSeriesCoordinatorAggregator.java` | Coordinator-level, unaffected by shard streaming |
