use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::LocalExecutorBuilder;
use rustpanda::wal::Wal;
use std::path::Path;
use std::time::Instant;

fn cleanup(path: &str) {
	if Path::new(path).exists() {
		std::fs::remove_file(path).unwrap();
	}
}

fn bench_append(c: &mut Criterion) {
	let mut group = c.benchmark_group("wal_append");

	for size in [64, 512, 1024, 4096, 16384] {
		let message = vec![0u8; size];
		group.throughput(Throughput::Bytes(size as u64));

		group.bench_with_input(BenchmarkId::from_parameter(size), &message, |b, msg| {
			b.iter_custom(|iters| {
				let msg = msg.clone();
				LocalExecutorBuilder::default()
					.spawn(move || async move {
						let path = format!("/tmp/wal_bench_append_{}.log", msg.len());
						cleanup(&path);
						let mut wal = Wal::create(&path).await;

						// warmup
						for _ in 0..10 {
							wal.append(&msg).await;
						}

						let start = Instant::now();
						for _ in 0..iters {
							wal.append(&msg).await;
						}
						let elapsed = start.elapsed();

						cleanup(&path);
						elapsed
					})
					.unwrap()
					.join()
					.unwrap()
			});
		});
	}

	group.finish();
}

fn bench_append_sequential_throughput(c: &mut Criterion) {
	let mut group = c.benchmark_group("wal_append_throughput");
	// measure sustained throughput at different message sizes
	for size in [64, 512, 1024, 4096, 16384] {
		let message = vec![0u8; size];
		let batch = 1000u64;
		group.throughput(Throughput::Bytes(size as u64 * batch));

		group.bench_with_input(BenchmarkId::from_parameter(size), &message, |b, msg| {
			b.iter_custom(|iters| {
				let msg = msg.clone();
				LocalExecutorBuilder::default()
					.spawn(move || async move {
						let path = format!("/tmp/wal_bench_tput_{}.log", msg.len());
						cleanup(&path);
						let mut wal = Wal::create(&path).await;

						// warmup
						for _ in 0..100 {
							wal.append(&msg).await;
						}

						let start = Instant::now();
						for _ in 0..iters * batch {
							wal.append(&msg).await;
						}
						let elapsed = start.elapsed();

						cleanup(&path);
						elapsed
					})
					.unwrap()
					.join()
					.unwrap()
			});
		});
	}

	group.finish();
}

fn bench_read(c: &mut Criterion) {
	let mut group = c.benchmark_group("wal_read");

	for size in [64, 512, 1024, 4096, 16384] {
		let message = vec![0u8; size];
		group.throughput(Throughput::Bytes(size as u64));

		group.bench_with_input(BenchmarkId::from_parameter(size), &message, |b, msg| {
			b.iter_custom(|iters| {
				let msg = msg.clone();
				LocalExecutorBuilder::default()
					.spawn(move || async move {
						let path = format!("/tmp/wal_bench_read_{}.log", msg.len());
						cleanup(&path);
						let mut wal = Wal::create(&path).await;

						// pre-write messages to read back
						let mut offsets = vec![];
						for _ in 0..iters + 10 {
							offsets.push(wal.append(&msg).await);
						}

						// warmup
						for i in 0..10 {
							wal.read(offsets[i]).await;
						}

						let start = Instant::now();
						for i in 0..iters as usize {
							wal.read(offsets[i + 10]).await;
						}
						let elapsed = start.elapsed();

						cleanup(&path);
						elapsed
					})
					.unwrap()
					.join()
					.unwrap()
			});
		});
	}

	group.finish();
}

fn bench_read_sequential_throughput(c: &mut Criterion) {
	let mut group = c.benchmark_group("wal_read_throughput");
	let batch = 1000u64;

	for size in [64, 512, 1024, 4096, 16384] {
		let message = vec![0u8; size];
		group.throughput(Throughput::Bytes(size as u64 * batch));

		group.bench_with_input(BenchmarkId::from_parameter(size), &message, |b, msg| {
			b.iter_custom(|iters| {
				let msg = msg.clone();
				LocalExecutorBuilder::default()
					.spawn(move || async move {
						let path = format!("/tmp/wal_bench_read_tput_{}.log", msg.len());
						cleanup(&path);
						let mut wal = Wal::create(&path).await;

						// pre-write all messages
						let total = iters * batch + 100;
						let first_offset = wal.append(&msg).await;
						for _ in 1..total {
							wal.append(&msg).await;
						}

						// warmup by chaining through first 100
						let mut offset = first_offset;
						for _ in 0..100 {
							let (_, next) = wal.read(offset).await;
							offset = next;
						}

						// measure chained reads
						let start = Instant::now();
						for _ in 0..iters * batch {
							let (_, next) = wal.read(offset).await;
							offset = next;
						}
						let elapsed = start.elapsed();

						cleanup(&path);
						elapsed
					})
					.unwrap()
					.join()
					.unwrap()
			});
		});
	}

	group.finish();
}

fn bench_append_then_read_latency(c: &mut Criterion) {
	let mut group = c.benchmark_group("wal_append_read_latency");
	// simulates a producer writing and consumer immediately reading
	// most representative of real broker workload
	let message = vec![0u8; 1024];
	group.throughput(Throughput::Bytes(1024));

	group.bench_function("1kb_produce_consume", |b| {
		b.iter_custom(|iters| {
			LocalExecutorBuilder::default()
				.spawn(move || async move {
					let path = "/tmp/wal_bench_e2e.log";
					cleanup(path);
					let mut wal = Wal::create(path).await;
					let msg = vec![0u8; 1024];

					// warmup
					for _ in 0..10 {
						let offset = wal.append(&msg).await;
						wal.read(offset).await;
					}

					let start = Instant::now();
					for _ in 0..iters {
						let offset = wal.append(&msg).await;
						wal.read(offset).await;
					}
					let elapsed = start.elapsed();

					cleanup(path);
					elapsed
				})
				.unwrap()
				.join()
				.unwrap()
		});
	});

	group.finish();
}

criterion_group!(
	benches,
	bench_append,
	bench_append_sequential_throughput,
	bench_read,
	bench_read_sequential_throughput,
	bench_append_then_read_latency,
);
criterion_main!(benches);
