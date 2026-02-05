# Arrow Flight POC for TSDB Bulk Indexing - Design Document

## Overview

This document describes two approaches for implementing Arrow Flight-based bulk indexing in the TSDB plugin. The goal is to enable high-performance bulk ingestion by bypassing JSON/SMILE parsing overhead.

**POC Target:** Single shard, single node (no routing complexity)

---

## Approach 1: Engine Registry (RECOMMENDED)

### Architecture

```
Python/Java Client
        │
        │ Arrow Flight DoPut (RecordBatches)
        ▼
┌─────────────────────────────────────────────┐
│  TSDBFlightService (AuxTransport)           │
│    └── TSDBFlightProducer                   │
│          - acceptPut(): receives batches    │
│          - parses FlightDescriptor for idx  │
└─────────────────────────────────────────────┘
        │
        │ TSDBEngineRegistry.get("my-index")
        ▼
┌─────────────────────────────────────────────┐
│  TSDBEngine.indexArrowBatch(VectorSchemaRoot)│
│    - Iterates Arrow vectors directly        │
│    - Creates Labels via ByteLabels          │
│    - Calls HeadAppender.preprocess/append   │
│    - Bypasses XContentParser entirely       │
└─────────────────────────────────────────────┘
```

### Implementation Details

**TSDBEngineRegistry.java** - Singleton registry
```java
public class TSDBEngineRegistry {
    private static final TSDBEngineRegistry INSTANCE = new TSDBEngineRegistry();
    private final ConcurrentHashMap<String, TSDBEngine> engines = new ConcurrentHashMap<>();

    public static TSDBEngineRegistry getInstance() { return INSTANCE; }

    public void register(String indexName, TSDBEngine engine) {
        engines.put(indexName, engine);
    }

    public void unregister(String indexName) {
        engines.remove(indexName);
    }

    public TSDBEngine get(String indexName) {
        return engines.get(indexName);
    }
}
```

**TSDBEngine modifications:**
- Constructor: `TSDBEngineRegistry.getInstance().register(indexName, this);`
- doClose(): `TSDBEngineRegistry.getInstance().unregister(indexName);`
- New method: `indexArrowBatch(VectorSchemaRoot root)` - direct Arrow vector processing

**TSDBFlightProducer.acceptPut():**
```java
@Override
public Runnable acceptPut(CallContext context, FlightStream flightStream,
                          StreamListener<PutResult> ackStream) {
    return () -> {
        String indexName = flightStream.getDescriptor().getPath().get(0);
        TSDBEngine engine = TSDBEngineRegistry.getInstance().get(indexName);

        while (flightStream.next()) {
            VectorSchemaRoot root = flightStream.getRoot();
            int count = engine.indexArrowBatch(root);
            ackStream.onNext(PutResult.metadata(...));
        }
        ackStream.onCompleted();
    };
}
```

### Pros
- **Direct engine access** - bypasses all parsing, maximum performance
- **Simple implementation** - just a ConcurrentHashMap with register/unregister
- **Full control** - can call `indexArrowBatch()` with raw Arrow vectors
- **Measures true benefit** - isolates Arrow vs JSON parsing overhead

### Cons
- **Custom pattern** - not standard OpenSearch engine access
- **Registry maintenance** - engines must register/unregister lifecycle
- **No standard safeguards** - bypasses routing, versioning, some error handling

---

## Approach 2: NodeClient API (NOT RECOMMENDED FOR POC)

### Architecture

```
Python/Java Client
        │
        │ Arrow Flight DoPut (RecordBatches)
        ▼
┌─────────────────────────────────────────────┐
│  TSDBFlightService (AuxTransport)           │
│    └── TSDBFlightProducer                   │
│          - acceptPut(): receives batches    │
│          - converts Arrow → JSON documents  │
└─────────────────────────────────────────────┘
        │
        │ NodeClient.bulk(BulkRequest)
        ▼
┌─────────────────────────────────────────────┐
│  TransportBulkAction (standard OpenSearch)  │
│    └── TransportShardBulkAction             │
│          └── IndexShard.index()             │
│                └── TSDBEngine.index()       │
│                      └── TSDBDocument.fromParsedDocument()
│                            ← PARSING STILL HAPPENS HERE
└─────────────────────────────────────────────┘
```

### Implementation Details

**TSDBFlightService:** Inject NodeClient via createComponents()

**TSDBFlightProducer.acceptPut():**
```java
@Override
public Runnable acceptPut(CallContext context, FlightStream flightStream,
                          StreamListener<PutResult> ackStream) {
    return () -> {
        String indexName = flightStream.getDescriptor().getPath().get(0);
        BulkRequest bulkRequest = new BulkRequest();

        while (flightStream.next()) {
            VectorSchemaRoot root = flightStream.getRoot();

            for (int i = 0; i < root.getRowCount(); i++) {
                // Convert Arrow row to JSON
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.field("labels", extractLabelsString(root, i));
                builder.field("timestamp", getTimestamp(root, i));
                builder.field("value", getValue(root, i));
                builder.endObject();

                bulkRequest.add(new IndexRequest(indexName).source(builder));
            }
        }

        // Execute via standard bulk API
        nodeClient.bulk(bulkRequest, ActionListener.wrap(
            response -> ackStream.onCompleted(),
            e -> ackStream.onError(...)
        ));
    };
}
```

### Pros
- **Standard OpenSearch pattern** - uses normal bulk indexing flow
- **All safeguards included** - routing, versioning, translog, error handling
- **No engine registry** - uses existing infrastructure
- **Works with any index** - not limited to TSDB engines

### Cons
- **Still requires parsing** - Arrow → JSON → ParsedDocument → TSDBDocument
- **Loses the POC benefit** - the whole point is to bypass XContentParser overhead
- **More overhead** - goes through full transport action stack
- **Defeats purpose** - Arrow data converted to JSON just to be parsed again

---

## Decision: Approach 1 Selected

**Rationale:**
1. The POC goal is to measure performance gains from bypassing JSON parsing
2. Approach 2 would still parse documents, negating the Arrow benefit
3. Engine Registry is simple enough for a POC
4. Can always add sophistication later if POC proves value

---

## Arrow Schema

| Column | Name | Type | Description |
|--------|------|------|-------------|
| 0 | `labels` | `List<Utf8>` | Pre-split KV pairs ["k1","v1","k2","v2",...] |
| 1 | `timestamp` | `Int64` | Epoch milliseconds |
| 2 | `value` | `Float64` | Sample value |

---

## Files to Create (Approach 1)

1. `src/main/java/org/opensearch/tsdb/flight/TSDBFlightSettings.java`
2. `src/main/java/org/opensearch/tsdb/flight/TSDBEngineRegistry.java`
3. `src/main/java/org/opensearch/tsdb/flight/TSDBFlightService.java`
4. `src/main/java/org/opensearch/tsdb/flight/TSDBFlightProducer.java`
5. `scripts/flight_client.py` (test client)

## Files to Modify (Approach 1)

1. `build.gradle` - Add Arrow Flight + gRPC dependencies
2. `src/main/java/org/opensearch/index/engine/TSDBEngine.java` - Registry hooks + indexArrowBatch()
3. `src/main/java/org/opensearch/tsdb/TSDBPlugin.java` - Wire Flight service

---

## Dependencies

```gradle
// Arrow Flight
implementation 'org.apache.arrow:flight-core:15.0.0'

// gRPC (required by Flight)
implementation 'io.grpc:grpc-api:1.58.0'
implementation 'io.grpc:grpc-netty:1.58.0'
implementation 'io.grpc:grpc-stub:1.58.0'
runtimeOnly 'io.grpc:grpc-core:1.58.0'
```

---

## Expected Performance

| Method | Throughput | Notes |
|--------|------------|-------|
| JSON Bulk API | ~50,000 docs/sec | Current baseline |
| Arrow Flight (Approach 1) | ~150,000-200,000 docs/sec | 3-4x improvement |

Improvement sources:
- No JSON/SMILE parsing
- Pre-split labels (no String.split())
- Columnar batch processing
- Direct memory access via Arrow

---

## POC Limitations

- No translog integration (data loss on restart)
- Single shard only (no routing)
- No authentication/SSL
- No backpressure handling
- Minimal error handling
