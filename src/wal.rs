use glommio::io::{DmaFile, OpenOptions};

pub struct Wal {
	pub file: DmaFile,
	pub current_offset: u64,
}

impl Wal {
	pub async fn open_or_create(path: &str) -> Self {
		if std::path::Path::new(path).exists() {
			Wal::open(path).await
		} else {
			Wal::create(path).await
		}
	}

	pub async fn create(path: &str) -> Self {
		let file = OpenOptions::new()
			.create_new(true)
			.read(true)
			.write(true)
			.dma_open(path)
			.await
			.unwrap();
		Wal {
			file,
			current_offset: 0,
		}
	}

	pub async fn open(path: &str) -> Self {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.dma_open(path)
			.await
			.unwrap();
		let size = file.file_size().await.unwrap();
		Wal {
			file,
			current_offset: size,
		}
	}

	// header and message are written at file aligned indexes
	// this allows us to always have aligned reads at the cost of some space
	pub async fn append(&mut self, message: &[u8]) -> u64 {
		let header_size = self.file.alignment() as usize;
		let size = header_size + message.len(); // bytes
		let aligned_size = self.file.align_up(size as u64);
		let mut buf = self.file.alloc_dma_buffer(aligned_size as usize);
		let bytes = buf.as_bytes_mut();
		let len_msg_bytes = (message.len() as u64).to_be_bytes();
		bytes[0..8].copy_from_slice(&len_msg_bytes);
		bytes[header_size..header_size + message.len()].copy_from_slice(message);
		let offset = self.current_offset;
		self.file.write_at(buf, offset).await.unwrap();
		//self.file.fdatasync().await.unwrap();
		self.current_offset += aligned_size as u64;
		offset
	}

	// we can use read_at_aligned which is faster than read_at since the offsets are
	// always aligned. this does require the fn to return the next offset to consumers
	pub async fn read(&self, offset: u64) -> (Vec<u8>, u64) {
		let row_size = self.file.alignment();
		let msg_len_bytes = self
			.file
			.read_at_aligned(offset, row_size as usize)
			.await
			.unwrap();
		let msg_len = u64::from_be_bytes(msg_len_bytes[0..8].try_into().unwrap()) as usize;
		let aligned_msg_len = self.file.align_up(msg_len as u64) as usize;
		let msg_bytes = self
			.file
			.read_at_aligned(offset + row_size, aligned_msg_len)
			.await
			.unwrap();
		let msg = msg_bytes[0..msg_len].to_vec();
		let next_offset = offset + row_size + aligned_msg_len as u64; // start + header len + msg len
		(msg, next_offset)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use glommio::LocalExecutorBuilder;
	use std::path::Path;

	// helper to clean up test files
	fn cleanup(path: &str) {
		if Path::new(path).exists() {
			std::fs::remove_file(path).unwrap();
		}
	}

	#[test]
	fn test_append_and_read_single_message() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_single.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let message = b"hello world";
				let offset = wal.append(message).await;

				let (read_msg, _next_offset) = wal.read(offset).await;
				assert_eq!(read_msg, message);

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_append_multiple_messages_and_read_sequentially() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_multi.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let messages: Vec<&[u8]> = vec![b"first", b"second", b"third"];
				let mut offsets = vec![];

				for msg in &messages {
					let offset = wal.append(msg).await;
					offsets.push(offset);
				}

				for (i, offset) in offsets.iter().enumerate() {
					let (read_msg, _) = wal.read(*offset).await;
					assert_eq!(read_msg, messages[i]);
				}

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_next_offset_chains_correctly() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_chain.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let messages: Vec<&[u8]> = vec![b"first", b"second", b"third"];

				let first_offset = wal.append(messages[0]).await;
				wal.append(messages[1]).await;
				wal.append(messages[2]).await;

				// walk through messages using next_offset
				let (msg0, next) = wal.read(first_offset).await;
				let (msg1, next) = wal.read(next).await;
				let (msg2, _) = wal.read(next).await;

				assert_eq!(msg0, messages[0]);
				assert_eq!(msg1, messages[1]);
				assert_eq!(msg2, messages[2]);

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_first_append_offset_is_zero() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_offset_zero.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let offset = wal.append(b"message").await;
				assert_eq!(offset, 0);

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_offsets_are_monotonically_increasing() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_monotonic.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let mut last_offset = 0u64;

				for i in 0..10u8 {
					let offset = wal.append(&[i]).await;
					assert!(offset >= last_offset);
					last_offset = offset;
				}

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_empty_message() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_empty.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let offset = wal.append(b"").await;
				let (msg, _) = wal.read(offset).await;
				assert_eq!(msg, b"");

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_large_message() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_large.log";
				cleanup(path);

				// larger than one alignment block
				let message = vec![42u8; 8192];
				let mut wal = Wal::create(path).await;
				let offset = wal.append(&message).await;
				let (read_msg, _) = wal.read(offset).await;
				assert_eq!(read_msg, message);

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_message_exactly_alignment_size() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_exact_align.log";
				cleanup(path);

				// exactly 512 bytes - sits right on alignment boundary
				let message = vec![1u8; 512];
				let mut wal = Wal::create(path).await;
				let offset = wal.append(&message).await;
				let (read_msg, _) = wal.read(offset).await;
				assert_eq!(read_msg, message);

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_open_or_create_creates_new_file() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_new.log";
				cleanup(path);

				let wal = Wal::open_or_create(path).await;
				assert_eq!(wal.current_offset, 0);
				assert!(Path::new(path).exists());

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_open_or_create_resumes_existing_file() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_resume.log";
				cleanup(path);

				// write some messages
				let mut wal = Wal::create(path).await;
				wal.append(b"first").await;
				wal.append(b"second").await;
				let expected_offset = wal.current_offset;
				drop(wal);

				// reopen and check offset resumed correctly
				let resumed_wal = Wal::open_or_create(path).await;
				assert_eq!(resumed_wal.current_offset, expected_offset);

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_messages_survive_reopen() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_persist.log";
				cleanup(path);

				// write messages and close
				let mut wal = Wal::create(path).await;
				let offset = wal.append(b"persisted message").await;
				drop(wal);

				// reopen and read
				let wal = Wal::open(path).await;
				let (msg, _) = wal.read(offset).await;
				assert_eq!(msg, b"persisted message");

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	#[test]
	fn test_mixed_message_sizes() {
		LocalExecutorBuilder::default()
			.spawn(|| async move {
				let path = "/tmp/test_wal_mixed.log";
				cleanup(path);

				let mut wal = Wal::create(path).await;
				let messages: Vec<Vec<u8>> = vec![
					vec![0u8; 1],    // tiny
					vec![0u8; 511],  // just under alignment
					vec![0u8; 512],  // exactly alignment
					vec![0u8; 513],  // just over alignment
					vec![0u8; 4096], // large
				];

				let mut offsets = vec![];
				for msg in &messages {
					offsets.push(wal.append(msg).await);
				}

				for (i, offset) in offsets.iter().enumerate() {
					let (read_msg, _) = wal.read(*offset).await;
					assert_eq!(read_msg, messages[i]);
				}

				cleanup(path);
			})
			.unwrap()
			.join()
			.unwrap();
	}
}
