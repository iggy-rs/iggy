use crate::streaming::persistence::persister::Persister;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use prometheus_client::metrics::counter::Atomic;
use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::create_dir;
use tokio::io::{AsyncReadExt, BufReader};
use tracing::info;

const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[async_trait]
pub trait Metadata: Send + Sync + Debug {
    async fn init(&self) -> Result<Vec<MetadataRecord>, IggyError>;
    async fn apply(&self, code: u32, data: &[u8]) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct MetadataRecord {
    pub index: u64,
    pub timestamp: IggyTimestamp,
    pub code: u32,
    pub data: Bytes,
}

impl BytesSerializable for MetadataRecord {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(8 + 8 + 4 + 4 + self.data.len());
        bytes.put_u64_le(self.index);
        bytes.put_u64_le(self.timestamp.to_micros());
        bytes.put_u32_le(self.code);
        bytes.put_u32_le(self.data.len() as u32);
        bytes.put_slice(&self.data);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let index = bytes.slice(0..8).get_u64_le();
        let timestamp = IggyTimestamp::from(bytes.slice(8..16).get_u64_le());
        let code = bytes.slice(16..20).get_u32_le();
        let length = bytes.slice(20..24).get_u32_le() as usize;
        let data = bytes.slice(24..24 + length);
        Ok(MetadataRecord {
            index,
            timestamp,
            code,
            data,
        })
    }
}

#[derive(Debug)]
pub struct FileMetadata {
    current_index: AtomicU64,
    directory: String,
    path: String,
    persister: Arc<dyn Persister>,
}

#[derive(Debug, Default)]
pub struct TestMetadata {}

#[async_trait]
impl Metadata for TestMetadata {
    async fn init(&self) -> Result<Vec<MetadataRecord>, IggyError> {
        Ok(Vec::new())
    }

    async fn apply(&self, _: u32, _: &[u8]) -> Result<(), IggyError> {
        Ok(())
    }
}

impl FileMetadata {
    pub fn new(path: &str, persister: Arc<dyn Persister>) -> Self {
        Self {
            current_index: AtomicU64::new(0),
            directory: path.into(),
            path: format!("{}/metadata", path),
            persister,
        }
    }
}

#[async_trait]
impl Metadata for FileMetadata {
    async fn init(&self) -> Result<Vec<MetadataRecord>, IggyError> {
        if !Path::new(&self.directory).exists() && create_dir(&self.directory).await.is_err() {
            return Err(IggyError::CannotCreateMetadataDirectory(
                self.directory.to_owned(),
            ));
        }

        if !Path::new(&self.path).exists() {
            info!("Metadata file does not exist, creating a new one");
            self.persister.overwrite(&self.path, &[]).await?;
            return Ok(Vec::new());
        }

        info!("Reading metadata from file");
        let file = file::open(&self.path).await?;
        let file_size = file.metadata().await?.len();
        if file_size == 0 {
            info!("Metadata file is empty");
            return Ok(Vec::new());
        }

        let mut metadata = Vec::new();
        let mut total_size: u64 = 0;

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        loop {
            let index = reader.read_u64_le().await?;
            let timestamp = IggyTimestamp::from(reader.read_u64_le().await?);
            let code = reader.read_u32_le().await?;
            let length = reader.read_u32_le().await? as usize;
            let mut data = BytesMut::with_capacity(length);
            reader.read_exact(&mut data).await?;
            metadata.push(MetadataRecord {
                index,
                timestamp,
                code,
                data: data.freeze(),
            });
            total_size += 8 + 8 + 4 + 4 + length as u64;
            if total_size >= file_size {
                break;
            }
        }

        let size = IggyByteSize::from(total_size);
        info!(
            "Read {} metadata entries, total size: {}",
            metadata.len(),
            size.as_human_string()
        );
        self.current_index
            .store(metadata.len() as u64, Ordering::SeqCst);
        Ok(metadata)
    }

    async fn apply(&self, code: u32, data: &[u8]) -> Result<(), IggyError> {
        let metadata = MetadataRecord {
            index: self.current_index.get(),
            timestamp: IggyTimestamp::now(),
            code,
            data: Bytes::copy_from_slice(data),
        };

        self.persister
            .append(&self.path, &metadata.as_bytes())
            .await?;
        self.current_index.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
