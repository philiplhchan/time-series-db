# Verifying Arrow Flight Streaming for TSDB Aggregators

## Prerequisites

1. **JDK 21+** (build.gradle targets Java 21)
2. **Core OpenSearch (`os-core-arrow-tsdb`)** built and published to mavenLocal with `StreamableAggregationBuilder`
3. **Arrow Flight RPC plugin** built: `arrow-flight-rpc-3.6.0-SNAPSHOT.zip`

### Build the core and flight plugin

```bash
cd ../os-core-arrow-tsdb

# Publish core to mavenLocal (includes StreamableAggregationBuilder)
JAVA_HOME=/opt/jvm/jdk-21 ./gradlew publishToMavenLocal -x test -x check

# Build the arrow-flight-rpc plugin zip
JAVA_HOME=/opt/jvm/jdk-21 ./gradlew :plugins:arrow-flight-rpc:bundlePlugin
# Output: plugins/arrow-flight-rpc/build/distributions/arrow-flight-rpc-3.6.0-SNAPSHOT.zip
```

## Step 1: Start the cluster with Arrow Flight

```bash
cd /path/to/real-arrow-research
JAVA_HOME=/opt/jvm/jdk-21 ./gradlew run -ParrowFlight -Dopensearch.version=3.6.0-SNAPSHOT
```

The `-ParrowFlight` flag (defined in `build.gradle`) does the following:
- Installs the `arrow-flight-rpc` plugin into the test cluster
- Sets JVM system property: `opensearch.experimental.feature.transport.stream.enabled=true`
- Sets opensearch.yml setting: `stream.search.enabled=true`
- Overrides Netty properties required by Arrow Flight:
  - `io.netty.allocator.numDirectArenas=1` (default is 0, Flight requires > 0)
  - `io.netty.noUnsafe=false` (default is true, Flight requires false)
  - `io.netty.tryUnsafe=true`
  - `io.netty.tryReflectionSetAccessible=true`

Wait for the cluster to start (you'll see `started` in the logs).

### Custom flight plugin path

If your arrow-flight-rpc zip is in a different location:

```bash
JAVA_HOME=/opt/jvm/jdk-21 ./gradlew run \
  -ParrowFlight \
  -ParrowFlightPluginPath=/path/to/arrow-flight-rpc-3.6.0-SNAPSHOT.zip \
  -Dopensearch.version=3.6.0-SNAPSHOT
```

## Step 2: Verify cluster settings

### Check plugins are loaded

```bash
curl -s 'http://localhost:9200/_cat/plugins?v'
```

Expected output:
```
name        component        version
integTest-0 arrow-flight-rpc 3.6.0-SNAPSHOT
integTest-0 tsdb             3.6.0.0-SNAPSHOT
```

### Check streaming settings

```bash
curl -s 'http://localhost:9200/_cluster/settings?include_defaults=true' \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
def find(obj, target, path=''):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if target in k.lower():
                print(f'{path}.{k} = {v}')
            find(v, target, f'{path}.{k}')
find(d, 'stream')
" | grep -E 'stream\.search|transport\.stream\.type|experimental.*stream'
```

Expected output:
```
.defaults.stream.search.enabled = true
.defaults.transport.stream.type.default = FLIGHT
.defaults.opensearch.experimental.feature.transport.stream.enabled = false
```

Note: `opensearch.experimental.feature.transport.stream.enabled` shows `false` as a
cluster setting default, but it's overridden by the JVM system property
`-Dopensearch.experimental.feature.transport.stream.enabled=true` (visible in the
JVM arguments log line at startup). The JVM property takes precedence.

### Verify Arrow Flight server is listening

Check the startup logs for:
```
Arrow Flight server started. Listening at [Location{uri=grpc+tcp://127.0.0.1:9400}]
```

## Step 3: Enable debug logging for streaming decisions

```bash
curl -s -X PUT 'http://localhost:9200/_cluster/settings' \
  -H 'Content-Type: application/json' -d '{
  "transient": {
    "logger.org.opensearch.search.aggregations.AggregatorFactories": "DEBUG"
  }
}'
```

## Step 4: Create a TSDB index and run a query

### Create index

```bash
curl -s -X PUT 'http://localhost:9200/test_tsdb' \
  -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "tsdb_engine.enabled": true,
      "tsdb_engine.lang.m3.default_step_size": "10s"
    }
  }
}'
```

### Run a time_series_unfold aggregation query

```bash
curl -s -X POST 'http://localhost:9200/benchmark/_search' \
  -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "ts_unfold": {
      "time_series_unfold": {
        "min_timestamp": 1000000,
        "max_timestamp": 1030000,
        "step": 10000
      }
    }
  }
}'
```

## Step 5: Check server logs for PER_SEGMENT streaming decision

```bash
grep "Streaming aggregation decision" build/testclusters/integTest-0/logs/integTest.log
```

Expected output:
```
[DEBUG][o.o.s.a.AggregatorFactories] Streaming aggregation decision: PER_SEGMENT | streamable=true, topN=1000 | maxBucket=100000
```

### What each field means

| Field | Value | Meaning |
|-------|-------|---------|
| Decision | `PER_SEGMENT` | Streaming mode selected (not `PER_SHARD` fallback) |
| streamable | `true` | `StreamableAggregationBuilder.supportsStreaming()` returned true |
| topN | `1000` | `StreamingCostEstimable.estimateStreamingCost()` estimate from factory |
| maxBucket | `100000` | `search.aggregations.streaming.max_estimated_bucket_count` threshold |

If you see `PER_SHARD` instead of `PER_SEGMENT`, streaming was rejected — check that
the aggregation builder implements `StreamableAggregationBuilder` and that
`supportsStreaming()` returns `true`.

## Step 6: Verify streaming via M3QL and PromQL endpoints

The `/_m3ql` and `/_promql` endpoints now use the same stream search logic as `_search`.
When `stream.search.enabled=true`, the feature flag is on, and the aggregation tree is
streamable, these endpoints will automatically use `StreamSearchAction` instead of the
normal search path.

### Enable debug logging for BaseTSDBAction

```bash
curl -s -X PUT 'http://localhost:9200/_cluster/settings' \
  -H 'Content-Type: application/json' -d '{
  "transient": {
    "logger.org.opensearch.tsdb.query.rest.BaseTSDBAction": "DEBUG",
    "logger.org.opensearch.search.aggregations.AggregatorFactories": "DEBUG"
  }
}'
```

### Run an M3QL query

```bash
curl -s 'http://localhost:9200/_m3ql?query=fetch%20test_metric&partitions=test_tsdb&start=now-5m&end=now&step=10000'
```

### Run a PromQL range query

```bash
curl -s 'http://localhost:9200/_promql/query_range?query=test_metric&partitions=test_tsdb&start=now-5m&end=now&step=10000'
```

### Check logs for stream search usage

```bash
grep -E "Using stream search|Streaming aggregation decision" \
  build/testclusters/integTest-0/logs/integTest.log
```

Expected output:
```
[DEBUG][o.o.t.q.r.BaseTSDBAction  ] Using stream search for TSDB query
[DEBUG][o.o.s.a.AggregatorFactories] Streaming aggregation decision: PER_SEGMENT | streamable=true, topN=1000 | maxBucket=100000
```

If you only see the `AggregatorFactories` line but not the `BaseTSDBAction` line, the
endpoint is using the normal search path — verify that `stream.search.enabled` is `true`
and the feature flag is set via the JVM property.

### Verify streaming is NOT used when disabled

```bash
# Disable stream search
curl -s -X PUT 'http://localhost:9200/_cluster/settings' \
  -H 'Content-Type: application/json' -d '{
  "transient": { "stream.search.enabled": false }
}'

# Run the same M3QL query
curl -s 'http://localhost:9200/_m3ql?query=fetch%20test_metric&partitions=test_tsdb&start=now-5m&end=now&step=10000'

# Confirm no "Using stream search" log line appears
grep "Using stream search" build/testclusters/integTest-0/logs/integTest.log

# Re-enable for further testing
curl -s -X PUT 'http://localhost:9200/_cluster/settings' \
  -H 'Content-Type: application/json' -d '{
  "transient": { "stream.search.enabled": true }
}'
```

## How the two gates work

### Gate 1: `FlushModeResolver.isStreamable()` (compile-time check on AggregationBuilder)

Checks if the aggregation tree is eligible for streaming. For TSDB, this checks:
- `TimeSeriesUnfoldAggregationBuilder implements StreamableAggregationBuilder`
- `supportsStreaming()` returns `true` (when stages are null or all support CSS)

### Gate 2: `StreamingCostEstimable.estimateStreamingCost()` (runtime check on AggregatorFactory)

After Gate 1 passes, the framework calls `estimateStreamingCost()` on each factory:
- `TimeSeriesUnfoldAggregatorFactory implements StreamingCostEstimable`
- Returns `StreamingCostMetrics(true, 1000)` for streamable cases
- Returns `StreamingCostMetrics.nonStreamable()` for non-CSS-compatible stages

The framework then calls `FlushModeResolver.decideFlushMode()` which compares
topN (1000) against maxBucket (100000). Since 1000 <= 100000, it selects `PER_SEGMENT`.
