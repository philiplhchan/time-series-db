# Arrow Flight Bulk Ingestion for TSDB

High-performance bulk ingestion for TSDB using Apache Arrow Flight RPC protocol. This bypasses JSON/SMILE parsing overhead by sending pre-structured columnar data directly to the TSDB engine.

## Overview

Traditional OpenSearch bulk indexing requires:
1. Client serializes data to JSON/SMILE
2. Server parses JSON/SMILE to extract fields
3. Fields converted to internal document format

Arrow Flight eliminates parsing overhead:
1. Client sends pre-structured Arrow RecordBatches
2. Server reads columnar vectors directly
3. Data indexed without parsing

**Expected Performance Improvement:** 3-4x throughput compared to JSON bulk API.

## Quick Start

### 1. Start OpenSearch with Flight Enabled

```bash
./gradlew run -PflightEnabled=true
```

Optional: specify a custom port:
```bash
./gradlew run -PflightEnabled=true -PflightPort=9400
```

### 2. Create a TSDB Index

```bash
curl -X PUT "localhost:9200/my-tsdb-index" -H 'Content-Type: application/json' -d'{
  "settings": {
    "index.tsdb_engine.enabled": true,
    "index.tsdb_engine.lang.m3.default_step_size": "10s",
    "number_of_shards": 1,
    "number_of_replicas": 0
  }
}'
```

### 3. Send Data via Arrow Flight

```bash
pip install pyarrow
python scripts/flight_client.py --host localhost --port 9400 --index my-tsdb-index --count 10000
```

### 4. Verify

```bash
curl localhost:9200/my-tsdb-index/_count
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `tsdb.flight.enabled` | `false` | Enable/disable the Flight server |
| `tsdb.flight.port` | `9400` | Port for the Flight server |
| `tsdb.flight.host` | `0.0.0.0` | Host/address to bind to |

Settings can be configured in `opensearch.yml`:

```yaml
tsdb.flight.enabled: true
tsdb.flight.port: 9400
tsdb.flight.host: 0.0.0.0
aux.transport.types: tsdb-arrow-flight  # Required to start the transport
```

Or via Gradle properties (for `./gradlew run`):

```bash
./gradlew run -PflightEnabled=true -PflightPort=9400
```

## Arrow Schema

Data must be sent as Arrow RecordBatches with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `labels` | `List<Utf8>` | Pre-split label key-value pairs: `["key1", "val1", "key2", "val2", ...]` |
| `timestamp` | `Int64` | Epoch timestamp in milliseconds |
| `value` | `Float64` | Sample value |

### Example Record

```
labels:    ["__name__", "cpu_usage", "host", "server-1", "dc", "us-east"]
timestamp: 1704067200000
value:     75.5
```

## Python Client

The included Python client (`scripts/flight_client.py`) demonstrates how to send data:

```bash
python scripts/flight_client.py --help

usage: flight_client.py [-h] [--host HOST] [--port PORT] [--index INDEX]
                        [--count COUNT] [--batch-size BATCH_SIZE]
                        [--labels LABELS] [--verbose]

Options:
  --host        Flight server host (default: localhost)
  --port        Flight server port (default: 9400)
  --index       Target TSDB index name (default: my-tsdb-index)
  --count       Total documents to send (default: 10000)
  --batch-size  Documents per batch (default: 1000)
  --labels      Labels per document (default: 10)
  --verbose     Enable verbose output
```

### Client Code Example

```python
import pyarrow as pa
import pyarrow.flight as flight

# Connect
client = flight.FlightClient(flight.Location.for_grpc_tcp("localhost", 9400))

# Create schema
schema = pa.schema([
    ("labels", pa.list_(pa.utf8())),
    ("timestamp", pa.int64()),
    ("value", pa.float64()),
])

# Create descriptor with index name
descriptor = flight.FlightDescriptor.for_path("my-tsdb-index")

# Start DoPut
writer, _ = client.do_put(descriptor, schema)

# Create and send batch
labels = [["__name__", "cpu", "host", "server-1"]]
timestamps = [1704067200000]
values = [75.5]

batch = pa.RecordBatch.from_arrays([
    pa.array(labels, type=pa.list_(pa.utf8())),
    pa.array(timestamps, type=pa.int64()),
    pa.array(values, type=pa.float64()),
], schema=schema)

writer.write_batch(batch)
writer.close()
```

## Architecture

```
                                   ┌─────────────────────────────────┐
                                   │  Python/Java Client             │
                                   │  - Creates Arrow RecordBatches  │
                                   │  - Pre-splits labels            │
                                   └───────────────┬─────────────────┘
                                                   │
                                                   │ Arrow Flight DoPut
                                                   │ (gRPC + Arrow IPC)
                                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  OpenSearch Node                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  TSDBFlightService (AuxTransport on port 9400)                         │  │
│  │    └── TSDBFlightProducer                                              │  │
│  │          - Receives RecordBatches                                      │  │
│  │          - Extracts index name from FlightDescriptor                   │  │
│  │          - Looks up TSDBEngine from registry                           │  │
│  └──────────────────────────────┬─────────────────────────────────────────┘  │
│                                 │                                            │
│                                 │ Direct engine access                       │
│                                 ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  TSDBEngine.indexArrowBatch(VectorSchemaRoot)                          │  │
│  │    - Reads Arrow vectors directly (no parsing!)                        │  │
│  │    - Creates Labels from pre-split strings                             │  │
│  │    - Calls HeadAppender.preprocess() + append()                        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Design Decisions

### Engine Registry Pattern

The Flight producer accesses TSDBEngine instances directly via a singleton registry, bypassing the standard OpenSearch indexing path. This was chosen over using NodeClient because:

1. **NodeClient would still require parsing** - Arrow data would be converted to JSON, then parsed back to documents
2. **Direct access enables true zero-copy** - Arrow vectors are read in place without conversion
3. **Simple for POC** - ConcurrentHashMap with register/unregister lifecycle

See `arrow-flight-design.md` for detailed comparison of approaches.

## Files

| File | Description |
|------|-------------|
| `src/.../flight/TSDBFlightSettings.java` | Configuration settings |
| `src/.../flight/TSDBFlightService.java` | AuxTransport managing Flight server |
| `src/.../flight/TSDBFlightProducer.java` | Handles DoPut requests |
| `src/.../flight/TSDBEngineRegistry.java` | Engine instance registry |
| `scripts/flight_client.py` | Python test client |
| `arrow-flight-design.md` | Detailed design document |

## POC Limitations

This is a proof-of-concept with the following limitations:

- **No translog integration** - Data may be lost on node restart before flush
- **Single shard only** - No routing support for multi-shard indexes
- **No authentication/TLS** - Plain gRPC without security
- **No backpressure** - Client should manage send rate
- **Minimal error handling** - Basic error responses only
- **No metrics** - Ingestion metrics not yet integrated

## Troubleshooting

### Flight server not starting

Check that the setting is enabled:
```bash
curl localhost:9200/_cluster/settings?include_defaults=true | grep flight
```

### "TSDB index not found" error

Ensure the index exists and has `index.tsdb_engine.enabled: true`:
```bash
curl localhost:9200/my-tsdb-index/_settings
```

### Connection refused on port 9400

Verify the Flight server is listening:
```bash
netstat -tlnp | grep 9400
```

### Python client import error

Install pyarrow:
```bash
pip install pyarrow
```

## Future Enhancements

- [ ] Translog integration for durability
- [ ] Multi-shard routing support
- [ ] TLS/authentication
- [ ] Backpressure and flow control
- [ ] Ingestion metrics
- [ ] Java client library
- [ ] Batch size auto-tuning
