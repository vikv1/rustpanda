# Rustpanda TODO

## WAL (`src/wal.rs`)
- [ ] WalConfig struct for configurable batch size, pool size, disk block size
- [ ] Sparse index file for O(log n) cold path reads instead of linear scan
- [ ] Per-batch message offset index for O(1) scan_memory_block lookup
- [ ] Truncate corrupted last batch on open instead of panic
- [ ] Remove to_vec() in scan_memory_block, return zero-copy slice instead
- [ ] Log retention: delete old batch files after configurable time or size threshold
- [ ] Segment-based storage: split WAL into fixed-size segment files instead of one growing file

## Partition (`src/partition.rs`)
- [ ] Basic struct wrapping WAL with partition ID and topic name
- [ ] Produce operation: append message to WAL, return logical offset
- [ ] Consume operation: read message at offset, return message and next offset
- [ ] Consumer group offset tracking: persist committed offsets per consumer group
- [ ] Partition assignment: map partition ID to owning core

## Broker (`src/broker.rs`)
- [ ] LocalExecutor per core pinned via Placement::Fixed
- [ ] SO_REUSEPORT TCP listener so OS distributes connections across cores
- [ ] Partition map per core: which partitions this core owns
- [ ] Request routing: hash topic + key to partition, forward to owning core via shared_channel if needed
- [ ] Produce request handler
- [ ] Consume request handler
- [ ] Graceful shutdown: flush all active WAL buffers before exit

## Protocol (`src/protocol.rs`)
- [ ] Binary wire format for produce requests: topic, partition key, message bytes
- [ ] Binary wire format for consume requests: topic, partition id, offset
- [ ] Binary wire format for responses: offset (produce), message + next offset (consume)
- [ ] Error codes: backpressure, message too large, offset not found, corrupted data
- [ ] Request framing: length-prefixed so TCP stream can be parsed correctly

## Producer (`producer/`)
- [ ] CLI binary: --topic, --key, --message flags
- [ ] TCP connection to broker
- [ ] Encode produce request in wire format
- [ ] Handle backpressure response with retry and configurable backoff
- [ ] Batch mode: read messages from stdin and send as fast as possible for benchmarking

## Consumer (`consumer/`)
- [ ] CLI binary: --topic, --partition, --offset flags
- [ ] TCP connection to broker
- [ ] Encode consume request in wire format
- [ ] Print messages to stdout
- [ ] Follow mode: keep polling for new messages after reaching tail

## Load Generator (`loadgen/`)
- [ ] Configurable message size, count, concurrency
- [ ] Measure and report throughput in MB/s
- [ ] Measure and report latency with hdrhistogram: p50, p95, p99, p999
- [ ] Compare produce throughput with and without fdatasync
- [ ] Compare batch sizes: per-message flush vs batch of 100, 500, 1000

## Observability
- [ ] Prometheus metrics endpoint per broker instance
- [ ] Per-partition metrics: produce rate, consume rate, consumer lag, WAL size
- [ ] Per-core metrics: messages routed locally vs cross-core, channel queue depth
- [ ] Grafana dashboard: throughput, latency percentiles, consumer lag, backpressure events
- [ ] ServiceMonitor for kube-prometheus-stack scraping
- [ ] Structured tracing with opentelemetry: trace produce request end to end across cores

## Replication
- [ ] Leader election per partition
- [ ] In-sync replica set tracking
- [ ] Follower WAL replication from leader
- [ ] Producer acknowledgment modes: none, leader only, all replicas
- [ ] Leader failover: promote follower on leader crash
- [ ] Chaos testing with Toxiproxy: partition network, kill nodes, verify no data loss

## Kubernetes / Deployment
- [ ] Dockerfile for broker binary
- [ ] k8s deployment yaml with one pod per node
- [ ] Service and ServiceMonitor yamls
- [ ] k3d local cluster setup script
- [ ] Helm chart for full stack deployment

## Benchmarks
- [ ] WAL append throughput at multiple message sizes: 64b, 512b, 1kb, 4kb, 16kb
- [ ] WAL read throughput: hot path, warm path, cold path separately
- [ ] Broker end-to-end throughput: producer -> broker -> consumer
- [ ] Core scaling: throughput vs core count to prove linear scaling
- [ ] Comparison vs Kafka on same hardware
- [ ] Comparison vs Redpanda on same hardware
- [ ] Run final benchmarks on bare metal Linux, not WSL2

## Client Library
- [ ] Rust client crate with producer and consumer API
- [ ] Publish to crates.io
- [ ] Async API built on tokio for broader compatibility

## Future / Stretch
- [ ] Log compaction: keep only latest value per key
- [ ] Schema registry integration
- [ ] Multi-datacenter replication
- [ ] Zero-copy file-to-socket sendfile optimization on consume path
- [ ] Consumer group rebalancing protocol
- [ ] Admin API: list topics, describe partitions, reset offsets