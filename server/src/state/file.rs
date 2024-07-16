use crate::state::command::EntryCommand;
use crate::state::{State, StateEntry};
use crate::streaming::persistence::persister::Persister;
use crate::streaming::utils::file;
use crate::versioning::SemanticVersion;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::crypto::Encryptor;
use iggy::utils::timestamp::IggyTimestamp;
use log::debug;
use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, BufReader};
use tracing::{error, info};

const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[derive(Debug)]
pub struct FileState {
    current_index: AtomicU64,
    entries_count: AtomicU64,
    current_leader: AtomicU32,
    term: AtomicU64,
    version: u32,
    path: String,
    persister: Arc<dyn Persister>,
    encryptor: Option<Arc<dyn Encryptor>>,
}

impl FileState {
    pub fn new(
        path: &str,
        version: &SemanticVersion,
        persister: Arc<dyn Persister>,
        encryptor: Option<Arc<dyn Encryptor>>,
    ) -> Self {
        Self {
            current_index: AtomicU64::new(0),
            entries_count: AtomicU64::new(0),
            current_leader: AtomicU32::new(0),
            term: AtomicU64::new(0),
            path: path.into(),
            persister,
            encryptor,
            version: version.get_numeric_version().expect("Invalid version"),
        }
    }

    pub fn current_index(&self) -> u64 {
        self.current_index.load(Ordering::SeqCst)
    }

    pub fn entries_count(&self) -> u64 {
        self.entries_count.load(Ordering::SeqCst)
    }

    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl State for FileState {
    async fn init(&self) -> Result<Vec<StateEntry>, IggyError> {
        if !Path::new(&self.path).exists() {
            info!("State file does not exist, creating a new one");
            self.persister.overwrite(&self.path, &[]).await?;
        }

        let entries = self.load_entries().await?;
        let entries_count = entries.len() as u64;
        self.entries_count.store(entries_count, Ordering::SeqCst);
        if entries_count == 0 {
            self.current_index.store(0, Ordering::SeqCst);
        } else {
            let last_index = entries[entries_count as usize - 1].index;
            self.current_index.store(last_index, Ordering::SeqCst);
        }

        return Ok(entries);
    }

    async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError> {
        if !Path::new(&self.path).exists() {
            return Err(IggyError::StateFileNotFound);
        }

        let file = file::open(&self.path).await?;
        let file_size = file.metadata().await?.len();
        if file_size == 0 {
            info!("State file is empty");
            return Ok(Vec::new());
        }

        info!(
            "Loading state, file size: {}",
            IggyByteSize::from(file_size).as_human_string()
        );
        let mut entries = Vec::new();
        let mut total_size: u64 = 0;
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        let mut current_index = 0;
        let mut entries_count = 0;
        loop {
            let index = reader.read_u64_le().await?;
            total_size += 8;
            if entries_count > 0 && index != current_index + 1 {
                error!(
                    "State file is corrupted, expected index: {}, got: {}",
                    current_index + 1,
                    index
                );
                return Err(IggyError::StateFileCorrupted);
            }

            current_index = index;
            entries_count += 1;
            let term = reader.read_u64_le().await?;
            total_size += 8;
            let leader_id = reader.read_u32_le().await?;
            total_size += 4;
            let version = reader.read_u32_le().await?;
            total_size += 4;
            let flags = reader.read_u64_le().await?;
            total_size += 8;
            let timestamp = IggyTimestamp::from(reader.read_u64_le().await?);
            total_size += 8;
            let user_id = reader.read_u32_le().await?;
            total_size += 4;
            let checksum = reader.read_u32_le().await?;
            total_size += 4;
            let context_length = reader.read_u32_le().await? as usize;
            total_size += 4;
            let mut context = BytesMut::with_capacity(context_length);
            context.put_bytes(0, context_length);
            reader.read_exact(&mut context).await?;
            let context = context.freeze();
            total_size += context_length as u64;
            let code = reader.read_u32_le().await?;
            total_size += 4;
            let mut command_length = reader.read_u32_le().await? as usize;
            total_size += 4;
            let mut command = BytesMut::with_capacity(command_length);
            command.put_bytes(0, command_length);
            reader.read_exact(&mut command).await?;
            total_size += command_length as u64;
            let command_payload;
            if let Some(encryptor) = &self.encryptor {
                debug!("Decrypting state entry with index: {index}");
                command_payload = Bytes::from(encryptor.decrypt(&command.freeze())?);
                command_length = command_payload.len();
            } else {
                command_payload = command.freeze();
            }

            let mut entry_command = BytesMut::with_capacity(4 + 4 + command_length);
            entry_command.put_u32_le(code);
            entry_command.put_u32_le(command_length as u32);
            entry_command.extend(command_payload);
            let command = entry_command.freeze();
            EntryCommand::from_bytes(command.clone())?;
            let calculated_checksum = StateEntry::calculate_checksum(
                index, term, leader_id, version, flags, timestamp, user_id, &context, &command,
            );
            let entry = StateEntry::new(
                index,
                term,
                leader_id,
                version,
                flags,
                timestamp,
                user_id,
                calculated_checksum,
                context,
                command,
            );
            debug!("Read state entry: {entry}");
            if entry.checksum != checksum {
                return Err(IggyError::InvalidStateEntryChecksum(
                    entry.checksum,
                    checksum,
                    entry.index,
                ));
            }

            entries.push(entry);
            if total_size == file_size {
                break;
            }
        }

        info!("Loaded {entries_count} state entries, current index: {current_index}");
        Ok(entries)
    }

    async fn apply(&self, user_id: u32, command: EntryCommand) -> Result<(), IggyError> {
        debug!("Applying state entry with command: {command}, user ID: {user_id}");
        let timestamp = IggyTimestamp::now();
        let index = if self.entries_count.load(Ordering::SeqCst) == 0 {
            0
        } else {
            self.current_index.fetch_add(1, Ordering::SeqCst) + 1
        };
        let term = self.term.load(Ordering::SeqCst);
        let current_leader = self.current_leader.load(Ordering::SeqCst);
        let version = self.version;
        let flags = 0;
        let context = Bytes::new();
        let mut command = command.to_bytes();
        let checksum = StateEntry::calculate_checksum(
            index,
            term,
            current_leader,
            version,
            flags,
            timestamp,
            user_id,
            &context,
            &command,
        );

        if let Some(encryptor) = &self.encryptor {
            debug!("Encrypting state entry command with index: {index}");
            let command_code = command.slice(0..4).get_u32_le();
            let mut command_length = command.slice(4..8).get_u32_le() as usize;
            let command_payload = command.slice(8..8 + command_length);
            let encrypted_command_payload = encryptor.encrypt(&command_payload)?;
            command_length = encrypted_command_payload.len();
            let mut command_bytes = BytesMut::with_capacity(4 + 4 + command_length);
            command_bytes.put_u32_le(command_code);
            command_bytes.put_u32_le(command_length as u32);
            command_bytes.extend(encrypted_command_payload);
            command = command_bytes.freeze();
        }

        let entry = StateEntry::new(
            index,
            term,
            current_leader,
            version,
            flags,
            timestamp,
            user_id,
            checksum,
            context,
            command,
        );
        let bytes = entry.to_bytes();
        self.entries_count.fetch_add(1, Ordering::SeqCst);
        self.persister.append(&self.path, &bytes).await?;
        debug!("Applied state entry: {entry}");
        Ok(())
    }
}
