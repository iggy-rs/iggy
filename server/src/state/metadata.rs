use crate::streaming::persistence::persister::Persister;
use crate::streaming::utils::file;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::command;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use log::debug;
use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, BufReader};
use tracing::info;

const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[async_trait]
pub trait Metadata: Send + Sync + Debug {
    async fn init(&self) -> Result<Vec<MetadataEntry>, IggyError>;
    async fn apply(
        &self,
        code: u32,
        user_id: u32,
        command: &[u8],
        data: Option<&[u8]>,
    ) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct MetadataEntry {
    pub index: u64,
    pub term: u64,
    pub timestamp: IggyTimestamp,
    pub user_id: u32,
    pub code: u32,
    pub command: Bytes,
    pub data: Bytes, // Optional data e.g. used to enrich the command with additional information
}

impl Display for MetadataEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetadataEntry {{ index: {}, term: {}, timestamp: {}, user ID: {}, code: {}, name: {}, size: {} }}",
            self.index,
            self.term,
            self.timestamp,
            self.user_id,
            self.code,
            command::get_name_from_code(self.code).unwrap_or("invalid_command"),
            IggyByteSize::from(self.command.len() as u64).as_human_string()
        )
    }
}

impl BytesSerializable for MetadataEntry {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(
            8 + 8 + 8 + 4 + 4 + 4 + self.command.len() + 4 + self.data.len(),
        );
        bytes.put_u64_le(self.index);
        bytes.put_u64_le(self.term);
        bytes.put_u64_le(self.timestamp.to_micros());
        bytes.put_u32_le(self.user_id);
        bytes.put_u32_le(self.code);
        bytes.put_u32_le(self.command.len() as u32);
        bytes.put_slice(&self.command);
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
        let user_id = bytes.slice(24..28).get_u32_le();
        let code = bytes.slice(28..32).get_u32_le();
        let command_length = bytes.slice(32..36).get_u32_le() as usize;
        let command = bytes.slice(36..36 + command_length);
        let data_length = bytes
            .slice(36 + command_length..40 + command_length)
            .get_u32_le() as usize;
        let data = bytes.slice(40 + command_length..40 + command_length + data_length);
        Ok(MetadataEntry {
            index,
            term,
            timestamp,
            user_id,
            code,
            command,
            data,
        })
    }
}

#[derive(Debug)]
pub struct FileMetadata {
    current_index: AtomicU64,
    term: AtomicU64,
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

    async fn apply(&self, _: u32, _: u32, _: &[u8], _: Option<&[u8]>) -> Result<(), IggyError> {
        Ok(())
    }
}

impl FileMetadata {
    pub fn new(path: &str, persister: Arc<dyn Persister>) -> Self {
        Self {
            current_index: AtomicU64::new(0),
            term: AtomicU64::new(1),
            path: path.into(),
            persister,
        }
    }
}

#[async_trait]
impl Metadata for FileMetadata {
    async fn init(&self) -> Result<Vec<MetadataEntry>, IggyError> {
        if !Path::new(&self.path).exists() {
            info!("Store file does not exist, creating a new one");
            self.persister.overwrite(&self.path, &[]).await?;
            return Ok(Vec::new());
        }

        let file = file::open(&self.path).await?;
        let file_size = file.metadata().await?.len();
        if file_size == 0 {
            info!("Store file is empty");
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
            let user_id = reader.read_u32_le().await?;
            let code = reader.read_u32_le().await?;
            let command_length = reader.read_u32_le().await? as usize;
            let mut command = BytesMut::with_capacity(command_length);
            command.put_bytes(0, command_length);
            reader.read_exact(&mut command).await?;
            let data_length = reader.read_u32_le().await? as usize;
            let mut data = BytesMut::with_capacity(data_length);
            data.put_bytes(0, data_length);
            reader.read_exact(&mut data).await?;
            let entry = MetadataEntry {
                index,
                term,
                timestamp,
                user_id,
                code,
                command: command.freeze(),
                data: data.freeze(),
            };
            debug!("Read metadata entry: {entry}");
            entries.push(entry);
            total_size += 8 + 8 + 8 + 4 + 4 + 4 + command_length as u64 + 4 + data_length as u64;
            if total_size == file_size {
                break;
            }
        }

        info!("Read {} metadata entries", entries.len());
        self.current_index
            .store(entries.len() as u64, Ordering::SeqCst);
        Ok(entries)
    }

    async fn apply(
        &self,
        code: u32,
        user_id: u32,
        command: &[u8],
        data: Option<&[u8]>,
    ) -> Result<(), IggyError> {
        debug!("Applying metadata entry: code: {code}, user ID: {user_id}");
        let metadata = MetadataEntry {
            index: self.current_index.load(Ordering::SeqCst),
            term: self.term.load(Ordering::SeqCst),
            timestamp: IggyTimestamp::now(),
            user_id,
            code,
            command: Bytes::copy_from_slice(command),
            data: data.map_or(Bytes::new(), Bytes::copy_from_slice),
        };

        self.persister
            .append(&self.path, &metadata.as_bytes())
            .await?;
        self.current_index.fetch_add(1, Ordering::SeqCst);
        debug!("Applied metadata entry: {}", metadata);
        Ok(())
    }
}
