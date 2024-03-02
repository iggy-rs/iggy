use crate::compat::format_sampler::BinaryFormatSampler;
use crate::compat::formats::message::Message;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, BufReader};

pub struct MessageSampler {
    pub log_path: String,
    pub index_path: String,
}
impl MessageSampler {
    pub fn new(log_path: String, index_path: String) -> MessageSampler {
        MessageSampler {
            log_path,
            index_path,
        }
    }
}

#[async_trait]
impl BinaryFormatSampler for MessageSampler {
    async fn sample(&self) {
        let mut index_file = file::open(&self.index_path).await.unwrap();
        let start_position = index_file.read_u32_le().await.unwrap();
        let end_position = index_file.read_u32_le().await.unwrap();

        let mut log_file = file::open(&self.log_path).await.unwrap();
        let buffer_size = end_position as usize;
        let mut buffer = BytesMut::with_capacity(buffer_size);
        let mut reader = BufReader::new(log_file);
        let _ = reader.read_exact(&mut buffer).await.unwrap();

        let message = Message::try_from(buffer.freeze());
    }
}
