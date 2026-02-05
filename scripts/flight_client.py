#!/usr/bin/env python3
"""
Arrow Flight client for TSDB bulk ingestion POC.

This client sends time-series data to the TSDB Arrow Flight server for high-performance
bulk ingestion. The data is sent as Arrow RecordBatches with pre-split label key-value pairs.

Requirements:
    pip install pyarrow

Usage:
    python flight_client.py --host localhost --port 9400 --index my-tsdb-index --count 10000

Example workflow:
    1. Start OpenSearch with TSDB plugin: ./gradlew run
    2. Create TSDB index:
       curl -X PUT "localhost:9200/my-tsdb-index" -H 'Content-Type: application/json' -d'{
         "settings": {"index.tsdb_engine.enabled": true, "number_of_shards": 1, "number_of_replicas": 0}
       }'
    3. Run this client to ingest data
    4. Verify: curl localhost:9200/my-tsdb-index/_count
"""

import argparse
import time
import sys

try:
    import pyarrow as pa
    import pyarrow.flight as flight
except ImportError:
    print("Error: pyarrow is required. Install with: pip install pyarrow")
    sys.exit(1)


def create_batch(count: int, base_time: int, labels_per_doc: int = 10) -> pa.RecordBatch:
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
        timestamps.append(base_time + i * 1000)  # 1 second apart
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


def main():
    parser = argparse.ArgumentParser(
        description='TSDB Arrow Flight Client - High-performance bulk ingestion',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--host', default='localhost', help='Flight server host (default: localhost)')
    parser.add_argument('--port', type=int, default=9400, help='Flight server port (default: 9400)')
    parser.add_argument('--index', default='my-tsdb-index', help='Target TSDB index name (default: my-tsdb-index)')
    parser.add_argument('--count', type=int, default=10000, help='Total number of documents to send (default: 10000)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Documents per batch (default: 1000)')
    parser.add_argument('--labels', type=int, default=10, help='Number of labels per document (default: 10)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    args = parser.parse_args()

    print(f"TSDB Arrow Flight Client")
    print(f"========================")
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
            batch = create_batch(batch_doc_count, base_time + batch_start * 1000, args.labels)
            writer.write_batch(batch)
            total_docs += batch_doc_count
            batch_count += 1

            if args.verbose:
                print(f"  Sent batch {batch_count}: {total_docs}/{args.count} documents")
            else:
                # Progress indicator
                progress = total_docs / args.count * 100
                print(f"\rProgress: {progress:.1f}% ({total_docs}/{args.count})", end='', flush=True)

        print()  # Newline after progress
        writer.close()

    except Exception as e:
        print(f"\nError during data transfer: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time
    docs_per_sec = total_docs / elapsed if elapsed > 0 else 0

    print()
    print(f"Results")
    print(f"-------")
    print(f"Total documents sent: {total_docs:,}")
    print(f"Total batches:        {batch_count}")
    print(f"Elapsed time:         {elapsed:.2f} seconds")
    print(f"Throughput:           {docs_per_sec:,.0f} docs/sec")
    print()
    print(f"To verify, run:")
    print(f"  curl localhost:9200/{args.index}/_count")


if __name__ == "__main__":
    main()
