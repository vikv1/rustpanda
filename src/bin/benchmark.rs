use glommio::{LocalExecutorBuilder, Placement};
use rustpanda::wal::Wal; // Adjust this import based on your crate name
use std::fs;
use std::time::Instant;

fn main() {
	println!("=======================================================");
	println!("🚀 RUSTPANDA HYPERSCALE BENCHMARK SUITE");
	println!("=======================================================\n");

	let builder = LocalExecutorBuilder::new(Placement::Unbound);
	let handle = builder
		.spawn(|| async move {
			run_benchmarks().await;
		})
		.unwrap();

	handle.join().unwrap();
}

async fn run_benchmarks() {
	// Ensure we are hitting a real disk, not a tmpfs RAM drive
	let test_dir = "./target/bench_data";
	let _ = fs::remove_dir_all(test_dir);
	fs::create_dir_all(test_dir).unwrap();
	let path = format!("{}/bench.log", test_dir);

	let mut wal = Wal::create(&path).await;

	let payload_size = 1024; // 1 KB messages
	let message_count = 1_000_000; // 1 Million messages (~1 GB of data)
	let payload = vec![0xAB; payload_size];

	// =========================================================
	// TEST 1: APPEND THROUGHPUT (PRODUCERS)
	// =========================================================
	println!("Starting Append Benchmark (1 Million 1KB Messages)...");

	let start_write = Instant::now();
	for _ in 0..message_count {
		// In a real benchmark, you would handle Backpressure by yielding,
		// but because we are writing sync here, we just spin loop if the queue is full.
		loop {
			match wal.append(&payload) {
				Ok(_) => break,
				Err(rustpanda::wal::AppendError::Backpressure) => {
					// Yield to Glommio so the background DMA tasks can flush to disk
					glommio::yield_if_needed().await;
				}
				Err(e) => panic!("Unexpected error: {:?}", e),
			}
		}
	}

	// Force the final buffer to flush so we can measure total time accurately
	glommio::timer::sleep(std::time::Duration::from_millis(100)).await;

	let write_duration = start_write.elapsed();
	let total_bytes_written = message_count * (payload_size + 8); // payload + length prefix
	let write_mb_s = (total_bytes_written as f64 / 1_048_576.0) / write_duration.as_secs_f64();
	let msgs_per_sec = message_count as f64 / write_duration.as_secs_f64();

	// =========================================================
	// TEST 2: HOT PATH READ LATENCY (CONSUMERS)
	// =========================================================
	println!("Starting Hot Path Read Benchmark...");

	// Read the very last message written. It is guaranteed to be in the active RAM buffer.
	let hot_offset = wal.current_offset - 1;

	let start_hot_read = Instant::now();
	let mut hot_reads = 0;
	for _ in 0..100_000 {
		let _ = wal.read(hot_offset).await.unwrap();
		hot_reads += 1;
	}
	let hot_duration = start_hot_read.elapsed();
	let hot_latency_ns = hot_duration.as_nanos() / hot_reads as u128;

	// =========================================================
	// TEST 3: COLD PATH READ LATENCY (CONSUMERS)
	// =========================================================
	println!("Starting Cold Path Read Benchmark...");

	// Read offset 0. Because we wrote 1GB of data, offset 0 was flushed
	// to the physical disk a long time ago. This forces an NVMe O_DIRECT read.
	let start_cold_read = Instant::now();
	let mut cold_reads = 0;
	for _ in 0..1_000 {
		let _ = wal.read(0).await.unwrap();
		cold_reads += 1;
	}
	let cold_duration = start_cold_read.elapsed();
	let cold_latency_us = cold_duration.as_micros() / cold_reads as u128;

	// =========================================================
	// PRINT RESULTS
	// =========================================================
	println!("\n📊 BENCHMARK RESULTS");
	println!("-------------------------------------------------------");
	println!("Write Throughput : {:.2} MB/s", write_mb_s);
	println!("Write Speed      : {:.0} messages/sec", msgs_per_sec);
	println!("Hot Path Latency : {} ns / message (RAM)", hot_latency_ns);
	println!(
		"Cold Path Latency: {} µs / message (NVMe O_DIRECT)",
		cold_latency_us
	);
	println!("-------------------------------------------------------");

	// Cleanup
	let _ = fs::remove_dir_all(test_dir);
}
