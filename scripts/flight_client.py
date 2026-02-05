#!/usr/bin/env python3
"""
TSDB ingestion benchmark client - supports both Arrow Flight and traditional _bulk API.

This client sends time-series data to TSDB using either:
  - Arrow Flight (high-performance bulk ingestion, bypasses JSON parsing)
  - OpenSearch _bulk API (traditional JSON-based indexing via opensearch-py)

Requirements:
    pip install pyarrow opensearch-py

Usage:
    # Arrow Flight mode (default)
    python flight_client.py --mode flight --host localhost --port 9400 --index my-tsdb-index --count 10000

    # Traditional _bulk API mode
    python flight_client.py --mode bulk --host localhost --port 9200 --index my-tsdb-index --count 10000

Example workflow:
    1. Start OpenSearch with TSDB plugin: ./gradlew run -PflightEnabled=true
    2. Create TSDB index:
       curl -X PUT "localhost:9200/my-tsdb-index" -H 'Content-Type: application/json' -d'{
         "settings": {"index.tsdb_engine.enabled": true, "number_of_shards": 1, "number_of_replicas": 0}
       }'
    3. Run benchmark with both modes:
       python flight_client.py --mode flight --count 100000
       python flight_client.py --mode bulk --count 100000
    4. Compare throughput results
"""

import argparse
import time
import sys

# Optional imports with graceful handling
try:
    import pyarrow as pa
    import pyarrow.flight as flight
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    from opensearchpy import OpenSearch
    from opensearchpy.helpers import bulk
    HAS_OPENSEARCH = True
except ImportError:
    HAS_OPENSEARCH = False


def create_arrow_batch(count: int, base_time: int, labels_per_doc: int = 10) -> 'pa.RecordBatch':
    """
    Create a batch of TSDB documents as Arrow RecordBatch.

    Args:
        count: Number of documents to create
        base_time: Base timestamp in milliseconds
        labels_per_doc: Number of label key-value pairs per document

    Returns:
        Arrow RecordBatch with labels, timestamp, and value columns
    """
    labels = []
    timestamps = []
    values = []

    for i in range(count):
        # Labels as pre-split key-value pairs: ["key1", "val1", "key2", "val2", ...]
        label_pairs = []
        label_pairs.extend(["__name__", "cpu_usage"])
        label_pairs.extend(["host", f"server-{i % 100}"])
        label_pairs.extend(["dc", f"dc-{i % 5}"])
        label_pairs.extend(["region", f"region-{i % 3}"])

        # Add more labels to reach the desired count
        for j in range(4, labels_per_doc):
            label_pairs.extend([f"label{j}", f"value{i}_{j}"])

        labels.append(label_pairs)
        timestamps.append(base_time)
        values.append(50.0 + (i % 100) + (i * 0.01))

    # Create Arrow arrays
    # Labels are List<Utf8> - each row is a list of strings
    labels_array = pa.array(labels, type=pa.list_(pa.utf8()))
    timestamp_array = pa.array(timestamps, type=pa.int64())
    value_array = pa.array(values, type=pa.float64())

    schema = pa.schema([
        ("labels", pa.list_(pa.utf8())),
        ("timestamp", pa.int64()),
        ("value", pa.float64()),
    ])

    return pa.RecordBatch.from_arrays(
        [labels_array, timestamp_array, value_array],
        schema=schema
    )


def generate_bulk_actions(count: int, index: str, base_time: int, labels_per_doc: int = 10):
    """
    Generate documents for OpenSearch bulk helper.

    Args:
        count: Number of documents to create
        index: Target index name
        base_time: Base timestamp in milliseconds
        labels_per_doc: Number of label key-value pairs per document

    Yields:
        Document dicts for opensearch-py bulk helper
    """
    for i in range(count):
        # Labels as space-separated key-value pairs (TSDB format per README.md)
        # Format: "key1 value1 key2 value2 ..."
        label_parts = [
            "__name__", "cpu_usage",
            "host", f"server-{i % 100}",
            "dc", f"dc-{i % 5}",
            "region", f"region-{i % 3}",
        ]
        # Add more labels
        for j in range(4, labels_per_doc):
            label_parts.extend([f"label{j}", f"value{i}_{j}"])

        labels_str = " ".join(label_parts)

        yield {
            "_index": index,
            "_source": {
                "labels": labels_str,
                "timestamp": base_time,
                "value": 50.0 + (i % 100) + (i * 0.01)
            }
        }


def run_flight_mode(args):
    """Run ingestion using Arrow Flight."""
    if not HAS_PYARROW:
        print("Error: pyarrow is required for flight mode. Install with: pip install pyarrow")
        sys.exit(1)

    print(f"Mode: Arrow Flight")
    print(f"Target: {args.host}:{args.port}")
    print(f"Index:  {args.index}")
    print(f"Documents: {args.count} (batch size: {args.batch_size})")
    print(f"Labels per document: {args.labels}")
    print()

    # Connect to Flight server
    location = flight.Location.for_grpc_tcp(args.host, args.port)
    print(f"Connecting to {location}...")

    try:
        client = flight.FlightClient(location)
    except Exception as e:
        print(f"Error: Failed to connect to Flight server: {e}")
        print(f"Make sure the server is running with tsdb.flight.enabled=true")
        sys.exit(1)

    print("Connected!")
    print()

    # Create schema
    schema = pa.schema([
        ("labels", pa.list_(pa.utf8())),
        ("timestamp", pa.int64()),
        ("value", pa.float64()),
    ])

    # Create descriptor with index name as path
    descriptor = flight.FlightDescriptor.for_path(args.index)

    # Start DoPut
    print(f"Starting DoPut to index '{args.index}'...")
    try:
        writer, metadata_reader = client.do_put(descriptor, schema)
    except Exception as e:
        print(f"Error: Failed to start DoPut: {e}")
        sys.exit(1)

    base_time = int(time.time() * 1000)
    total_docs = 0
    batch_count = 0
    start_time = time.time()

    # Send batches
    try:
        for batch_start in range(0, args.count, args.batch_size):
            batch_doc_count = min(args.batch_size, args.count - batch_start)
            batch = create_arrow_batch(batch_doc_count, base_time + batch_start * 1000, args.labels)
            writer.write_batch(batch)
            total_docs += batch_doc_count
            batch_count += 1

            if args.verbose:
                print(f"  Sent batch {batch_count}: {total_docs}/{args.count} documents")
            else:
                progress = total_docs / args.count * 100
                print(f"\rProgress: {progress:.1f}% ({total_docs}/{args.count})", end='', flush=True)

            base_time += 10_000  # add 10s

        print()
        writer.close()

    except Exception as e:
        print(f"\nError during data transfer: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time
    return total_docs, batch_count, elapsed


def run_bulk_mode(args):
    """Run ingestion using OpenSearch _bulk API."""
    if not HAS_OPENSEARCH:
        print("Error: opensearch-py is required for bulk mode. Install with: pip install opensearch-py")
        sys.exit(1)

    print(f"Mode: OpenSearch _bulk API")
    print(f"Target: {args.host}:{args.port}")
    print(f"Index:  {args.index}")
    print(f"Documents: {args.count} (batch size: {args.batch_size})")
    print(f"Labels per document: {args.labels}")
    print()

    # Create OpenSearch client
    print(f"Connecting to {args.host}:{args.port}...")
    try:
        client = OpenSearch(
            hosts=[{"host": args.host, "port": args.port}],
            http_compress=False,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False,
        )
        # Test connection
        info = client.info()
        print(f"Connected to OpenSearch {info['version']['number']}!")
    except Exception as e:
        print(f"Error: Failed to connect to OpenSearch: {e}")
        sys.exit(1)

    print()

    base_time = int(time.time() * 1000)
    total_docs = 0
    batch_count = 0
    start_time = time.time()

    # Send batches
    try:
        for batch_start in range(0, args.count, args.batch_size):
            batch_doc_count = min(args.batch_size, args.count - batch_start)
            actions = list(generate_bulk_actions(
                batch_doc_count, args.index, base_time + batch_start * 1000, args.labels
            ))

            success, errors = bulk(client, actions, raise_on_error=False)

            if errors and args.verbose:
                print(f"  Warning: {len(errors)} errors in batch {batch_count + 1}")

            total_docs += success
            batch_count += 1

            if args.verbose:
                print(f"  Sent batch {batch_count}: {total_docs}/{args.count} documents")
            else:
                progress = total_docs / args.count * 100
                print(f"\rProgress: {progress:.1f}% ({total_docs}/{args.count})", end='', flush=True)

            base_time += 10_000  # add 10s

        print()

    except Exception as e:
        print(f"\nError during data transfer: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time
    return total_docs, batch_count, elapsed


def main():
    parser = argparse.ArgumentParser(
        description='TSDB Ingestion Benchmark - Arrow Flight vs _bulk API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--mode', choices=['flight', 'bulk'], default='flight',
                        help='Ingestion mode: flight (Arrow Flight) or bulk (_bulk API) (default: flight)')
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=None,
                        help='Server port (default: 9400 for flight, 9200 for bulk)')
    parser.add_argument('--index', default='my-tsdb-index', help='Target TSDB index name (default: my-tsdb-index)')
    parser.add_argument('--count', type=int, default=10000, help='Total number of documents to send (default: 10000)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Documents per batch (default: 1000)')
    parser.add_argument('--labels', type=int, default=10, help='Number of labels per document (default: 10)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    # Set default port based on mode (override if user passed wrong port)
    default_port = 9400 if args.mode == 'flight' else 9200
    if args.port is None:
        args.port = default_port
    elif args.port != default_port:
        print(f"Note: Using port {default_port} for {args.mode} mode (ignoring --port {args.port})")
        args.port = default_port

    print(f"TSDB Ingestion Benchmark")
    print(f"========================")

    if args.mode == 'flight':
        total_docs, batch_count, elapsed = run_flight_mode(args)
    else:
        total_docs, batch_count, elapsed = run_bulk_mode(args)

    docs_per_sec = total_docs / elapsed if elapsed > 0 else 0

    print()
    print(f"Results")
    print(f"-------")
    print(f"Mode:                 {args.mode.upper()}")
    print(f"Total documents sent: {total_docs:,}")
    print(f"Total batches:        {batch_count}")
    print(f"Elapsed time:         {elapsed:.2f} seconds")
    print(f"Throughput:           {docs_per_sec:,.0f} docs/sec")
    print()
    print(f"To verify, run:")
    print(f"  curl localhost:9200/{args.index}/_count")
    print()
    print(f"To compare modes, run both:")
    print(f"  python {sys.argv[0]} --mode flight --count {args.count}")
    print(f"  python {sys.argv[0]} --mode bulk --count {args.count}")


if __name__ == "__main__":
    main()
