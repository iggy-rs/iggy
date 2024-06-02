use crate::streaming::persistence::persister::Persister;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::create_dir;
use tokio::io::{AsyncReadExt, BufReader};
use tracing::info;

const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[async_trait]
pub trait Metadata: Send + Sync + Debug {
    async fn init(&self) -> Result<Vec<MetadataEntry>, IggyError>;
    async fn apply(&self, code: u32, data: &[u8]) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct MetadataEntry {
    pub index: u64,
    pub term: u64,
    pub timestamp: IggyTimestamp,
    pub code: u32,
    pub data: Bytes,
}

impl Display for MetadataEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetadataEntry {{ index: {}, term: {}, timestamp: {}, code: {}, name: {}, size: {} }}",
            self.index,
            self.term,
            self.timestamp,
            self.code,
            command::get_name_from_code(self.code).unwrap_or("invalid_command"),
            IggyByteSize::from(self.data.len() as u64).as_human_string()
        )
    }
}

impl BytesSerializable for MetadataEntry {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(8 + 8 + 8 + 4 + 4 + self.data.len());
        bytes.put_u64_le(self.index);
        bytes.put_u64_le(self.term);
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
        let term = bytes.slice(8..16).get_u64_le();
        let timestamp = IggyTimestamp::from(bytes.slice(16..24).get_u64_le());
        let code = bytes.slice(24..28).get_u32_le();
        let length = bytes.slice(28..32).get_u32_le() as usize;
        let data = bytes.slice(32..32 + length);
        Ok(MetadataEntry {
            index,
            term,
            timestamp,
            code,
            data,
        })
    }
}

#[derive(Debug)]
pub struct FileMetadata {
    current_index: AtomicU64,
    term: AtomicU64,
    directory: String,
    path: String,
    persister: Arc<dyn Persister>,
}

#[derive(Debug, Default)]
pub struct TestMetadata {}

#[async_trait]
impl Metadata for TestMetadata {
    async fn init(&self) -> Result<Vec<MetadataEntry>, IggyError> {
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
            term: AtomicU64::new(1),
            directory: path.into(),
            path: format!("{}/metadata", path),
            persister,
        }
    }
}

#[async_trait]
impl Metadata for FileMetadata {
    async fn init(&self) -> Result<Vec<MetadataEntry>, IggyError> {
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

        let file = file::open(&self.path).await?;
        let file_size = file.metadata().await?.len();
        if file_size == 0 {
            info!("Metadata file is empty");
            return Ok(Vec::new());
        }

        info!(
            "Reading metadata, file size: {}",
            IggyByteSize::from(file_size).as_human_string()
        );
        let mut entries = Vec::new();
        let mut total_size: u64 = 0;
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        loop {
            let index = reader.read_u64_le().await?;
            let term = reader.read_u64_le().await?;
            let timestamp = IggyTimestamp::from(reader.read_u64_le().await?);
            let code = reader.read_u32_le().await?;
            let length = reader.read_u32_le().await? as usize;
            let mut data = BytesMut::with_capacity(length);
            data.put_bytes(0, length);
            reader.read_exact(&mut data).await?;
            let entry = MetadataEntry {
                index,
                term,
                timestamp,
                code,
                data: data.freeze(),
            };
            info!("Read metadata entry: {entry}");
            entries.push(entry);
            total_size += 8 + 8 + 8 + 4 + 4 + length as u64;
            if total_size == file_size {
                break;
            }
        }

        info!("Read {} metadata entries", entries.len());
        self.current_index
            .store(entries.len() as u64, Ordering::SeqCst);
        Ok(entries)
    }

    async fn apply(&self, code: u32, data: &[u8]) -> Result<(), IggyError> {
        let metadata = MetadataEntry {
            index: self.current_index.load(Ordering::SeqCst),
            term: self.term.load(Ordering::SeqCst),
            timestamp: IggyTimestamp::now(),
            code,
            data: Bytes::copy_from_slice(data),
        };

        self.persister
            .append(&self.path, &metadata.as_bytes())
            .await?;
        self.current_index.fetch_add(1, Ordering::SeqCst);
        info!("Applied metadata entry: {}", metadata);
        Ok(())
    }
}
