# Rustpanda 🐼

A high-performance distributed message broker written in Rust, inspired by Redpanda's thread-per-core shared-nothing architecture. Built on [glommio](https://github.com/DataDog/glommio) and `io_uring` for Direct I/O with no kernel page cache overhead.

> Work in progress. This is a personal systems project built to learn high-performance distributed systems from first principles. Not production ready.

---

## Architecture

Rustpanda is built around three core principles:

**Thread-per-core, shared-nothing.** Each CPU core runs an isolated `glommio` executor. Partitions are pinned to cores so a partition is never accessed by more than one thread. No locks, no cross-thread contention, no `Arc<Mutex<T>>`.

**Direct I/O via io_uring.** All disk access bypasses the kernel page cache using `DmaFile`. Writes and reads are block-aligned, giving predictable latency instead of occasional stalls from OS cache flushes.

**Explicit alignment everywhere.** Record layout is designed so every read position is guaranteed to be block-aligned, enabling `read_at_aligned` on all reads with zero internal alignment overhead.

---

## Record Format

Each message is stored as a fixed-size header block followed by an aligned message body:

```
[alignment bytes: header]  -> first 8 bytes = message length (big-endian u64), rest = padding
[aligned message body]     -> raw message bytes, padded to alignment boundary
[next record...]
```

The header is always exactly one alignment block (512 or 4096 bytes depending on device), so the message body always begins at an aligned offset. This allows `read_at_aligned` for both reads in every operation.

---

## Components

### WAL (`src/wal.rs`)
Append-only write-ahead log backed by a `DmaFile`. Core operations:

- `open_or_create(path)` opens an existing WAL resuming from last offset, or creates a new one
- `append(message)` writes a length-prefixed record, fsyncs, and returns the offset
- `read(offset)` returns `(message, next_offset)`, the message bytes and the next aligned offset

Planned improvements:
- [ ] CRC32 checksum in header for corruption detection and crash recovery
- [ ] Batch fsync to group multiple appends before syncing for higher throughput
- [ ] Segment-based storage to support log retention and compaction

### Partition (`src/partition.rs`) -- WIP
Owns a WAL and manages offset tracking. Routes produce and consume requests. Pinned to a single core executor.

### Broker (`src/broker.rs`) -- WIP
TCP server that accepts producer and consumer connections and routes requests to the correct partition. One broker instance per core.

### Protocol (`src/protocol.rs`) -- WIP
Binary wire protocol between producers, consumers, and the broker.

---

## Design Decisions

**Why glommio over tokio?**
Tokio uses a work-stealing scheduler where tasks can migrate between threads, requiring shared state to use `Arc<Mutex<T>>`. Glommio pins tasks to a single thread via `LocalExecutor`, enabling truly shared-nothing data ownership. For a message broker where partitions are the unit of parallelism this maps directly: one partition per core, zero synchronization overhead.

**Why Direct I/O?**
The kernel page cache adds unpredictable latency. Writes appear fast until the OS decides to flush, causing occasional spikes. Direct I/O gives consistent write latency at the cost of managing alignment manually. For a broker where tail latency matters, this is the right tradeoff.

**Why a fixed-size header block instead of 8 bytes?**
An 8-byte length prefix means the message body starts at offset + 8, which is not guaranteed to be block-aligned. This would require `read_at` on every body read, which does internal alignment work on every call. Using a full alignment block for the header means every read position is guaranteed aligned, so `read_at_aligned` can be used everywhere.

**Why return next offset from read?**
Consumers cannot compute the next offset without knowing the alignment of the WAL. Exposing that detail would couple consumers to an internal implementation decision. Returning `(message, next_offset)` from read keeps the alignment logic inside the WAL where it belongs.

---

## Observability

Rustpanda is instrumented with Prometheus metrics and deployed alongside a local observability stack:

- k3d for a local multi-node Kubernetes cluster
- kube-prometheus-stack for Prometheus Operator and Grafana
- ServiceMonitor CRDs for automatic scrape configuration per component

Metrics exposed per partition: write throughput, read throughput, fsync latency, consumer lag.

---

## Getting Started

### Prerequisites
- Rust 1.85+
- Docker and k3d for the local cluster
- Linux kernel 5.8+ for io_uring support

### Build

```bash
cargo build --release
```

### Run locally

```bash
# start broker on default port
cargo run --bin broker

# produce a message
cargo run --bin producer -- --topic events --message "hello world"

# consume from beginning
cargo run --bin consumer -- --topic events --offset 0
```

### Deploy to local k3d cluster

```bash
k3d cluster create devcluster --agents 3
docker build -t rustpanda:latest .
k3d image import rustpanda:latest -c devcluster
kubectl apply -f k8s/
```

---

## Roadmap

- [x] WAL with Direct I/O and aligned reads/writes
- [ ] Partition layer with offset management
- [ ] TCP broker with binary protocol
- [ ] Producer and consumer CLI clients
- [ ] Multi-partition topics
- [ ] Replication with leader election
- [ ] Log retention and segment compaction
- [ ] Checksum-based crash recovery
- [ ] Batch fsync for throughput
- [ ] Rust client library on crates.io
- [ ] Benchmarks vs Kafka and Redpanda

---

## References

- [Redpanda architecture](https://redpanda.com/blog/tpc-buffers)
- [glommio](https://github.com/DataDog/glommio)
- [io_uring](https://kernel.dk/io_uring.pdf)
- [Kafka log design](https://kafka.apache.org/documentation/#log)
