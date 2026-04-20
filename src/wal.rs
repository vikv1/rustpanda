use std::{cell::RefCell, rc::Rc};

use glommio::{
	allocate_dma_buffer_global,
	io::{DmaBuffer, DmaFile, OpenOptions},
};

pub struct Wal {
	pub file: Rc<DmaFile>,
	pub current_physical_offset: u64, // physical byte offset for disk write
	pub current_offset: u64,          // logical message id
	pub active_buffer: Option<DmaBuffer>,
	pub active_bytes_written: usize,
	pub active_base_offset: u64,
	pub free_buffers: Rc<RefCell<Vec<DmaBuffer>>>,
	pub flushing_batches: Rc<RefCell<Vec<InFlightBatch>>>,
}

pub struct InFlightBatch {
	pub buffer: Rc<DmaBuffer>,
	pub base_offset: u64,
	pub message_count: u64,
	pub valid_length: usize,
}

#[derive(Debug, PartialEq)]
pub enum AppendError {
	Backpressure,
	MessageTooLarge,
}

#[derive(Debug, PartialEq)]
pub enum ReadError {
	NotFound,
	CorruptedData,
}

#[repr(C, packed)]
struct BatchHeader {
	crc32: u32,
	valid_length: u32,
	base_offset: u64,
	message_count: u32,
}

impl BatchHeader {
	pub fn parse(bytes: &[u8]) -> Result<Self, ReadError> {
		if bytes.len() < 20 {
			return Err(ReadError::CorruptedData);
		}

		Ok(BatchHeader {
			crc32: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
			valid_length: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
			base_offset: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
			message_count: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
		})
	}

	pub fn verify_crc(block_bytes: &[u8], header: &BatchHeader) -> Result<(), ReadError> {
		let mut hasher = crc32fast::Hasher::new();

		let valid_len = header.valid_length as usize;

		if valid_len > block_bytes.len() {
			return Err(ReadError::CorruptedData);
		}

		hasher.update(&block_bytes[20..valid_len]);
		let checksum = hasher.finalize();

		if checksum != header.crc32 {
			Err(ReadError::CorruptedData)
		} else {
			Ok(())
		}
	}
}

const ONE_MB: usize = 1 << 20;
const POOL_SIZE: usize = 10;
const DISK_BLOCK_SIZE: usize = 4096;

impl Wal {
	pub async fn open_or_create(path: &str) -> Self {
		if std::path::Path::new(path).exists() {
			Wal::open(path).await
		} else {
			Wal::create(path).await
		}
	}

	pub async fn create(path: &str) -> Self {
		let file = Rc::new(
			OpenOptions::new()
				.create_new(true)
				.read(true)
				.write(true)
				.dma_open(path)
				.await
				.unwrap(),
		);

		let (active_buffer, free_buffers) = Wal::allocate_buffers().await;
		Wal {
			file,
			current_offset: 0,
			current_physical_offset: 0,
			active_buffer,
			active_bytes_written: size_of::<BatchHeader>(),
			active_base_offset: 0,
			free_buffers: free_buffers,
			flushing_batches: Rc::new(RefCell::new(Vec::new())),
		}
	}

	pub async fn open(path: &str) -> Self {
		let file = Rc::new(
			OpenOptions::new()
				.read(true)
				.write(true)
				.dma_open(path)
				.await
				.unwrap(),
		);
		let size = file.file_size().await.unwrap();

		let (active_buffer, free_buffers) = Wal::allocate_buffers().await;

		let mut recovered_logical_offset = 0;
		let mut active_physical_offset = 0;

		if size > 0 {
			active_physical_offset = size - (ONE_MB as u64);

			let last_block = file
				.read_at_aligned(active_physical_offset, ONE_MB)
				.await
				.unwrap();
			let header = BatchHeader::parse(&last_block[0..20]).unwrap();

			match BatchHeader::verify_crc(&last_block, &header) {
				Ok(_) => {
					recovered_logical_offset = header.base_offset + (header.message_count as u64);
					active_physical_offset += ONE_MB as u64
				}
				Err(ReadError::CorruptedData) => {
               println!("Last block is corrupted");
               panic!("shutting down");
            }
            _ => unreachable!(),
			}
		}

		Wal {
			file,
			current_offset: recovered_logical_offset,
			current_physical_offset: active_physical_offset,
			active_buffer,
			active_bytes_written: size_of::<BatchHeader>(),
			active_base_offset: recovered_logical_offset,
			free_buffers: free_buffers,
			flushing_batches: Rc::new(RefCell::new(Vec::new())),
		}
	}

	pub fn flush(&mut self) {
		if self.active_bytes_written > size_of::<BatchHeader>() {
			self.roll_active_buffer();
		}
	}

	pub async fn allocate_buffers() -> (Option<DmaBuffer>, Rc<RefCell<Vec<DmaBuffer>>>) {
		let mut buffers = Vec::with_capacity(POOL_SIZE);
		for _ in 0..POOL_SIZE {
			buffers.push(allocate_dma_buffer_global(ONE_MB));
		}
		let active_buffer = buffers.pop();

		(active_buffer, Rc::new(RefCell::new(buffers)))
	}

	fn roll_active_buffer(&mut self) {
		let mut full_buffer = self.active_buffer.take().unwrap();
		let valid_bytes = self.active_bytes_written;
		let message_count = (self.current_offset - self.active_base_offset) as u32;

		let mut hasher = crc32fast::Hasher::new();
		hasher.update(&full_buffer.as_bytes()[20..valid_bytes]);
		let checksum = hasher.finalize();

		let bytes = full_buffer.as_bytes_mut();
		bytes[0..4].copy_from_slice(&checksum.to_be_bytes());
		bytes[4..8].copy_from_slice(&(valid_bytes as u32).to_be_bytes());
		bytes[8..16].copy_from_slice(&self.active_base_offset.to_be_bytes());
		bytes[16..20].copy_from_slice(&message_count.to_be_bytes());

		let new_buffer = self.free_buffers.borrow_mut().pop().unwrap();
		self.active_buffer = Some(new_buffer);
		self.active_bytes_written = size_of::<BatchHeader>();

		let batch_base_offset = self.active_base_offset; // will be used for sparse indexing
		self.active_base_offset = self.current_offset;

		let file_clone = Rc::clone(&self.file);
		let pool_clone = Rc::clone(&self.free_buffers);
		let write_offset = self.current_physical_offset;

		self.current_physical_offset += ONE_MB as u64;
		let rc_buffer = Rc::new(full_buffer);

		self.flushing_batches.borrow_mut().push(InFlightBatch {
			buffer: rc_buffer.clone(),
			base_offset: batch_base_offset,
			message_count: message_count as u64,
			valid_length: valid_bytes,
		});

		let flushing_batches_clone = Rc::clone(&self.flushing_batches);

		glommio::spawn_local(async move {
			// dma write to drive
			file_clone
				.write_rc_at(rc_buffer.clone(), write_offset)
				.await
				.unwrap();

			// below forces a disk flush if stricter durability is needed
			// file_clone.fdatasync().await.unwrap();

			// rm from warm path queue
			flushing_batches_clone
				.borrow_mut()
				.retain(|batch| batch.base_offset != batch_base_offset);

			let recycled_buffer = Rc::try_unwrap(rc_buffer).unwrap();

			pool_clone.borrow_mut().push(recycled_buffer);

			// TODO: update index file here for sparse indexing
		})
		.detach();
	}

	// write to memory buffer. rolls the active buffer is the message is going to overflow.
	pub fn append(&mut self, message: &[u8]) -> Result<u64, AppendError> {
		let required_space = 8 + message.len();

		if required_space > ONE_MB - 20 {
			return Err(AppendError::MessageTooLarge);
		}

		if self.active_bytes_written + required_space > ONE_MB {
			if self.free_buffers.borrow().len() == 0 {
				return Err(AppendError::Backpressure);
			}

			self.roll_active_buffer();
		}

		let buf = self.active_buffer.as_mut().unwrap();
		let bytes = buf.as_bytes_mut();
		let len_msg_bytes = (message.len() as u64).to_be_bytes();
		bytes[self.active_bytes_written..self.active_bytes_written + 8]
			.copy_from_slice(&len_msg_bytes);
		bytes[self.active_bytes_written + 8..self.active_bytes_written + required_space]
			.copy_from_slice(message);
		let assigned_offset = self.current_offset;
		self.active_bytes_written += required_space;
		self.current_offset += 1;
		Ok(assigned_offset)
	}

	// we can use read_at_aligned which is faster than read_at since the offsets are
	// always aligned. this does require the fn to return the next offset to consumers
	pub async fn read(&self, offset: u64) -> Result<Vec<u8>, ReadError> {
		// hot path
		if offset >= self.active_base_offset && offset < self.current_offset {
			let buffer = self.active_buffer.as_ref().unwrap();
			let bytes = buffer.as_bytes();
			return self.scan_memory_block(
				bytes,
				self.active_bytes_written,
				self.active_base_offset,
				offset,
			);
		}

		// warm path, batches currently flushing
		for batch in self.flushing_batches.borrow().iter() {
			if offset >= batch.base_offset && offset < batch.base_offset + batch.message_count {
				return self.scan_memory_block(
					batch.buffer.as_bytes(),
					batch.valid_length,
					batch.base_offset,
					offset,
				);
			}
		}

		// cold path
		// TODO: optimize with sparse index tree
		let file_size = self.file.file_size().await.unwrap();
		let mut physical_disk_offset = 0;

		while physical_disk_offset < file_size {
			let header_chunk = self
				.file
				.read_at_aligned(physical_disk_offset, DISK_BLOCK_SIZE)
				.await
				.unwrap();

			let header = BatchHeader::parse(&header_chunk[0..20])?;
			if offset >= header.base_offset
				&& offset < header.base_offset + header.message_count as u64
			{
				// found
				let full_block = self
					.file
					.read_at_aligned(physical_disk_offset, ONE_MB)
					.await
					.unwrap();

				BatchHeader::verify_crc(&full_block, &header)?;

				return self.scan_memory_block(
					&full_block,
					header.valid_length as usize,
					header.base_offset,
					offset,
				);
			}

			physical_disk_offset += ONE_MB as u64;
		}

		Err(ReadError::NotFound)
	}

	fn scan_memory_block(
		&self,
		block_bytes: &[u8],
		valid_length: usize,
		block_base_offset: u64,
		requested_offset: u64,
	) -> Result<Vec<u8>, ReadError> {
		let mut cursor = size_of::<BatchHeader>();
		let target_relative_idx = requested_offset - block_base_offset;
		let mut cur_relative_idx = 0;

		while cur_relative_idx < target_relative_idx {
			if cursor + 8 > valid_length {
				return Err(ReadError::CorruptedData);
			}

			let msg_len =
				u64::from_be_bytes(block_bytes[cursor..cursor + 8].try_into().unwrap()) as usize;

			if cursor + 8 + msg_len > valid_length {
				return Err(ReadError::CorruptedData);
			}

			cursor += 8 + msg_len;
			cur_relative_idx += 1
		}

		if cursor + 8 > valid_length {
			return Err(ReadError::CorruptedData);
		}

		let target_len =
			u64::from_be_bytes(block_bytes[cursor..cursor + 8].try_into().unwrap()) as usize;

		if cursor + 8 + target_len > valid_length {
			return Err(ReadError::CorruptedData);
		}

		let target_msg = block_bytes[cursor + 8..cursor + 8 + target_len].to_vec();
		Ok(target_msg)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use glommio::{LocalExecutorBuilder, Placement};
	use std::time::Duration;

	// Helper function to spin up a Glommio reactor for a single test
	fn run_glommio_test<F, Fut>(test_name: &str, test_logic: F)
	where
		F: FnOnce(String) -> Fut + Send + 'static,
		Fut: std::future::Future<Output = ()>,
	{
		// Force temp files into the local target directory to avoid tmpfs O_DIRECT errors
		std::fs::create_dir_all("./target/test_data").unwrap();
		let path = format!("./target/test_data/{}.log", test_name);
		let _ = std::fs::remove_file(&path); // Clean up previous runs

		let builder = LocalExecutorBuilder::new(Placement::Unbound);
		let handle = builder
			.spawn(move || async move {
				test_logic(path.clone()).await;
				let _ = std::fs::remove_file(&path); // Cleanup after success
			})
			.unwrap();

		handle.join().unwrap();
	}

	#[test]
	fn test_hot_path_memory_read() {
		run_glommio_test("hot_path", |path| async move {
			let mut wal = Wal::create(&path).await;

			let msg1 = b"hello hyperscale";
			let msg2 = b"thread per core";

			let offset1 = wal.append(msg1).unwrap();
			let offset2 = wal.append(msg2).unwrap();

			assert_eq!(offset1, 0);
			assert_eq!(offset2, 1);

			// Read them back immediately. This should hit RAM, not disk.
			let read1 = wal.read(offset1).await.unwrap();
			let read2 = wal.read(offset2).await.unwrap();

			assert_eq!(read1, msg1);
			assert_eq!(read2, msg2);
		});
	}

	#[test]
	fn test_cold_path_disk_read_and_batch_roll() {
		run_glommio_test("cold_path", |path| async move {
			let mut wal = Wal::create(&path).await;

			// 1. Force a roll! ONE_MB - 30 bytes + 8 byte length prefix = ONE_MB - 22 bytes.
			// Combined with the 20 byte BatchHeader, the buffer is now at ONE_MB - 2 bytes.
			let huge_msg = vec![0xAA; ONE_MB - 30];
			let offset1 = wal.append(&huge_msg).unwrap();

			// 2. Append a second message (16 bytes). Required space is 24 bytes.
			// (ONE_MB - 2) + 24 > ONE_MB. This WILL successfully trigger a roll!
			let msg2 = b"overflow message";
			let offset2 = wal.append(msg2).unwrap();

			assert_eq!(offset1, 0);
			assert_eq!(offset2, 1);

			// Yield the Glommio executor to let the background DMA task write to disk
			glommio::timer::sleep(Duration::from_millis(50)).await;

			// Read offset 0. This hits the cold disk path.
			let read1 = wal.read(offset1).await.unwrap();
			assert_eq!(read1, huge_msg);

			// Read offset 1. This hits the hot RAM path.
			let read2 = wal.read(offset2).await.unwrap();
			assert_eq!(read2, msg2);
		});
	}

	#[test]
	fn test_crash_recovery_and_invariants() {
		run_glommio_test("crash_recovery", |path| async move {
			let expected_recovered_offset;

			// --- PHASE 1: Write and Crash ---
			{
				let mut wal = Wal::create(&path).await;

				let huge_msg = vec![0xBB; ONE_MB - 30];
				wal.append(&huge_msg).unwrap(); // ID 0

				wal.append(b"trigger roll").unwrap(); // ID 1. Triggers roll of ID 0.

				// AT THIS MOMENT:
				// Disk contains ID 0.
				// RAM contains ID 1.
				// Because we simulate a crash, RAM is lost! We should ONLY recover ID 0.
				// The next available ID upon reboot will be exactly what the active_base_offset
				// was before the crash (which is 1).
				expected_recovered_offset = wal.active_base_offset;

				// Yield to ensure background flush finishes
				glommio::timer::sleep(Duration::from_millis(50)).await;
			}

			// --- PHASE 2: Reboot and Recover ---
			{
				let wal = Wal::open(&path).await;

				// Verify the sequence math invariants
				assert_eq!(
					wal.current_offset, expected_recovered_offset,
					"Global sequence corrupted"
				);
				assert_eq!(
					wal.active_base_offset, expected_recovered_offset,
					"Buffer base sequence corrupted"
				);
				assert_eq!(
					wal.active_bytes_written,
					std::mem::size_of::<BatchHeader>(),
					"Buffer cursor not reset"
				);

				// Verify historical data is still readable
				let old_data = wal.read(0).await.unwrap();
				assert_eq!(old_data[0], 0xBB);
			}
		});
	}

	#[test]
	fn test_backpressure_exhaustion() {
		run_glommio_test("backpressure", |path| async move {
			let mut wal = Wal::create(&path).await;

			// Create a message that is exactly 1/3 of a Megabyte
			let msg = vec![0xCC; 300_000];

			// We have POOL_SIZE buffers (e.g., 10).
			// If we fill all of them WITHOUT yielding the thread to let
			// the background task write to disk, the engine MUST push back.
			let mut backpressure_hit = false;

			// Loop enough times to exhaust the entire memory pool
			for _ in 0..50 {
				match wal.append(&msg) {
					Ok(_) => {}
					Err(AppendError::Backpressure) => {
						backpressure_hit = true;
						break;
					}
					Err(AppendError::MessageTooLarge) => panic!("Message size logic failed"),
				}
			}

			assert!(
				backpressure_hit,
				"Engine failed to protect itself against OOM"
			);
		});
	}

	#[test]
	fn test_oversized_message_rejection() {
		run_glommio_test("oversized_message", |path| async move {
			let mut wal = Wal::create(&path).await;

			// Attempt to append a message larger than 1MB
			let massive_msg = vec![0xDD; ONE_MB + 10];

			let result = wal.append(&massive_msg);
			assert!(matches!(result, Err(AppendError::MessageTooLarge)));
		});
	}

	#[test]
	fn test_cold_path_linear_scan_jump() {
		run_glommio_test("cold_path_linear_scan", |path| async move {
			let mut wal = Wal::create(&path).await;

			// 1. Append Message 0 (Small)
			let msg0 = b"first message";
			let offset0 = wal.append(msg0).unwrap();

			// 2. Append Message 1 (The TARGET message in the middle)
			let msg1 = b"target middle message";
			let offset1 = wal.append(msg1).unwrap();

			// 3. Append Message 2 (Massive - Forces the roll!)
			// msg0 takes 17 bytes (8+9), msg1 takes 29 bytes (8+21).
			// Adding ONE_MB - 50 will comfortably exceed the 1MB buffer limit and trigger a roll.
			let huge_msg = vec![0xEE; ONE_MB - 50];
			let offset2 = wal.append(&huge_msg).unwrap();

			assert_eq!(offset0, 0);
			assert_eq!(offset1, 1);
			assert_eq!(offset2, 2);

			// Yield the thread to let the Glommio background task write to disk
			glommio::timer::sleep(Duration::from_millis(50)).await;

			// 4. THE CRITICAL READ: Fetch Message 1
			// Because it rolled, this hits the cold disk path.
			// Because it's the middle message, the `scan_memory_block` while-loop
			// MUST execute exactly once: reading the length of msg0, jumping over it,
			// and correctly slicing msg1.
			let read_target = wal.read(offset1).await.unwrap();

			assert_eq!(
				read_target, msg1,
				"The linear scanner failed to jump over the first message correctly"
			);

			// Bonus: verify the other two just to be absolutely sure the whole block is intact
			let read0 = wal.read(offset0).await.unwrap();
			assert_eq!(read0, msg0);

			let read2 = wal.read(offset2).await.unwrap();
			assert_eq!(read2, huge_msg);
		});
	}

	#[test]
	fn test_warm_path_inflight_read() {
		run_glommio_test("warm_path_read", |path| async move {
			let mut wal = Wal::create(&path).await;

			let msg1 = b"ghost data message";
			let offset1 = wal.append(msg1).unwrap();

			// Append a massive message to force `msg1` out of the active buffer
			let huge_msg = vec![0xEE; ONE_MB - 30];
			wal.append(&huge_msg).unwrap();

			// CRITICAL: We DO NOT yield the executor here.
			// The background DMA task has been spawned, but it hasn't executed yet.
			// The disk file is empty. The data ONLY exists in `flushing_batches`.

			assert_eq!(
				wal.flushing_batches.borrow().len(),
				1,
				"Batch should be in-flight"
			);

			// This read MUST hit the Warm Path to succeed
			let read1 = wal.read(offset1).await.unwrap();
			assert_eq!(read1, msg1, "Failed to read from the Warm Path");
		});
	}

	#[test]
	fn test_multiple_inflight_batches() {
		run_glommio_test("multiple_inflight", |path| async move {
			let mut wal = Wal::create(&path).await;

			// We will force 3 consecutive rolls WITHOUT yielding to the disk.
			// This simulates a massive burst of incoming network traffic.
			let huge_msg = vec![0xFF; ONE_MB - 30];

			let offset0 = wal.append(b"batch 0 data").unwrap();
			wal.append(&huge_msg).unwrap(); // Rolls Batch 0

			let offset2 = wal.append(b"batch 1 data").unwrap();
			wal.append(&huge_msg).unwrap(); // Rolls Batch 1

			let offset4 = wal.append(b"batch 2 data").unwrap();
			wal.append(&huge_msg).unwrap(); // Rolls Batch 2

			// Verify the engine is holding 3 buffers in RAM waiting to flush
			assert_eq!(wal.flushing_batches.borrow().len(), 5);

			// Read from the middle of the Warm Path queue
			let read_middle = wal.read(offset2).await.unwrap();
			assert_eq!(read_middle, b"batch 1 data");

			// Now, let the kernel catch up. Yield for a long time to let all 3 writes finish.
			glommio::timer::sleep(Duration::from_millis(500)).await;

			// The Warm Path should be completely empty now. Memory is recycled!
			assert_eq!(wal.flushing_batches.borrow().len(), 0);

			// Reading them now should seamlessly route to the Cold Path (Disk)
			let read_cold = wal.read(offset0).await.unwrap();
			assert_eq!(read_cold, b"batch 0 data");
		});
	}

	#[test]
	fn test_warm_to_cold_transition() {
		run_glommio_test("warm_to_cold", |path| async move {
			let mut wal = Wal::create(&path).await;

			let msg = b"transition message";
			let offset = wal.append(msg).unwrap();

			// Force roll
			let huge_msg = vec![0x11; ONE_MB - 30];
			wal.append(&huge_msg).unwrap();

			// 1. Read from Warm Path
			let read_warm = wal.read(offset).await.unwrap();
			assert_eq!(read_warm, msg);

			// 2. Allow flush to complete
			glommio::timer::sleep(Duration::from_millis(200)).await;

			// 3. Read from Cold Path
			let read_cold = wal.read(offset).await.unwrap();
			assert_eq!(read_cold, msg);
		});
	}
}
