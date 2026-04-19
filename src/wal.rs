use glommio::io::DmaFile;

pub struct Wal {
   file: DmaFile,
   current_offset: u64,
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
      let file = DmaFile::create(path).await.unwrap();
      Wal {
         file,
         current_offset: 0,
      }
   }

   pub async fn open(path: &str) -> Self {
      let file = DmaFile::open(path).await.unwrap();
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
      self.file.fdatasync().await.unwrap();
      self.current_offset += aligned_size as u64;
      offset
   }

   // we can use read_at_aligned which is faster than read_at since the offsets are
   // always aligned. this does require the fn to return the next offset to consumers
   pub async fn read(&self, offset: u64) -> (Vec<u8>, u64) {
      let row_size = self.file.alignment();
      let msg_len_bytes = self.file.read_at_aligned(offset, row_size as usize).await.unwrap();
      let msg_len = u64::from_be_bytes(msg_len_bytes[0..8].try_into().unwrap()) as usize;
      let aligned_msg_len = self.file.align_up(msg_len as u64) as usize; 
      let msg_bytes = self.file.read_at_aligned(offset + row_size, aligned_msg_len).await.unwrap();
      let msg = msg_bytes[0..msg_len].to_vec();
      let next_offset = offset + row_size + aligned_msg_len as u64; // start + header len + msg len
      (msg, next_offset)
   }

}