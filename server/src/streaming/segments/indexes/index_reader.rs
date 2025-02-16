use super::{
    index::{Index, IndexRange},
    INDEX_SIZE,
};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::Mutex,
};
use tracing::{error, trace, warn};

/// A dedicated struct for reading from the index file.
#[derive(Debug)]
pub struct SegmentIndexReader {
    file_path: String,
    file: Mutex<Option<File>>,
    index_size_bytes: Arc<AtomicU64>,
}

impl SegmentIndexReader {
    /// Opens the index file in read-only mode.
    pub async fn new(file_path: &str, index_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        let actual_index_size = file
            .metadata()
            .await
            .with_error_context(|e| {
                format!("Failed to get metadata of index file: {file_path}, error: {e}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        index_size_bytes.store(actual_index_size, Ordering::Release);

        trace!("Opened index file for reading: {file_path}, size: {actual_index_size}",);
        Ok(Self {
            file_path: file_path.to_string(),
            file: Mutex::new(Some(file)),
            index_size_bytes,
        })
    }

    /// Loads all indexes from the index file.
    pub async fn load_all_indexes_impl(&self) -> Result<Vec<Index>, IggyError> {
        let file_size = self.index_size_bytes.load(Ordering::Acquire);
        if file_size == 0 {
            warn!("Index {} file is empty.", self.file_path);
            return Ok(Vec::new());
        }
        let mut file = self.file.lock().await;
        let file = file
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        file.seek(SeekFrom::Start(0))
            .await
            .with_error_context(|e| format!("Failed to seek to start of index file: {e}"))
            .map_err(|_| IggyError::CannotReadIndexOffset)?;
        let mut buf = vec![0u8; file_size as usize];
        file.read_exact(&mut buf)
            .await
            .with_error_context(|e| format!("Failed to read index file {}: {e}", self.file_path))
            .map_err(|_| IggyError::CannotReadIndexOffset)?;
        let indexes_count = (file_size / INDEX_SIZE) as usize;
        let mut indexes = Vec::with_capacity(indexes_count);
        for chunk in buf.chunks_exact(INDEX_SIZE as usize) {
            let offset = u32::from_le_bytes(
                chunk[0..4]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index offset: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let position = u32::from_le_bytes(
                chunk[4..8]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index position: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let timestamp = u64::from_le_bytes(
                chunk[8..16]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index timestamp: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let current_index = Index {
                offset,
                position,
                timestamp,
            };
            indexes.push(current_index);
        }
        if indexes.len() as u64 != file_size / INDEX_SIZE {
            error!(
                "Loaded {} indexes from disk, expected {}, file {} is probably corrupted!",
                indexes.len(),
                file_size / INDEX_SIZE,
                self.file_path
            );
        }
        Ok(indexes)
    }

    /// Loads an index range from the index file given a start/end offset.
    pub async fn load_index_range_impl(
        &self,
        index_start_offset: u64,
        index_end_offset: u64,
        segment_start_offset: u64,
    ) -> Result<Option<IndexRange>, IggyError> {
        if index_start_offset > index_end_offset {
            warn!(
                "Index start offset {} is greater than index end offset {} for file {}.",
                index_start_offset, index_end_offset, self.file_path
            );
            return Ok(None);
        }
        let file_length = self.index_size_bytes.load(Ordering::Acquire);
        if file_length == 0 {
            warn!("Index {} file is empty.", self.file_path);
            return Ok(None);
        }
        trace!("Index file length: {} bytes.", file_length);

        let relative_start_offset = (index_start_offset - segment_start_offset) as u32;
        let relative_end_offset = (index_end_offset - segment_start_offset) as u32;
        let mut index_range = IndexRange::default();
        let mut file = self.file.lock().await;

        let file = file
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        file.seek(SeekFrom::Start(0))
            .await
            .with_error_context(|e| format!("Failed to seek to start of index file: {e}"))
            .map_err(|_| IggyError::CannotSeekFile)?;

        let mut buf = vec![0u8; file_length as usize];
        file.read_exact(&mut buf)
            .await
            .with_error_context(|e| format!("Failed to read index file: {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        for chunk in buf.chunks_exact(INDEX_SIZE as usize) {
            let offset = u32::from_le_bytes(
                chunk[0..4]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index offset: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let position = u32::from_le_bytes(
                chunk[4..8]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index position: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let timestamp = u64::from_le_bytes(
                chunk[8..16]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index timestamp: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let current_index = Index {
                offset,
                position,
                timestamp,
            };
            if offset >= relative_start_offset && index_range.start == Index::default() {
                index_range.start = current_index;
            }
            if offset >= relative_end_offset {
                index_range.end = current_index;
                break;
            }
        }
        if index_range.start == Index::default() {
            warn!("Failed to find index >= {}", relative_start_offset);
        }
        Ok(Some(index_range))
    }

    pub async fn load_index_for_timestamp_impl(
        &self,
        timestamp: u64,
    ) -> Result<Option<Index>, IggyError> {
        let file_size = self.index_size_bytes.load(Ordering::Acquire);
        if file_size == 0 {
            trace!("Index file {} is empty.", self.file_path);
            return Ok(Some(Index::default()));
        }

        let mut file = self.file.lock().await;
        let file = file
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        file.seek(SeekFrom::Start(0))
            .await
            .with_error_context(|e| format!("Failed to seek to start of index file: {e}"))
            .map_err(|_| IggyError::CannotSeekFile)?;
        let mut buf = vec![0u8; file_size as usize];
        file.read_exact(&mut buf)
            .await
            .with_error_context(|e| format!("Failed to read index file: {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let mut last_index: Option<Index> = None;
        for chunk in buf.chunks_exact(INDEX_SIZE as usize) {
            let offset = u32::from_le_bytes(
                chunk[0..4]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index offset: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexOffset)?,
            );
            let position = u32::from_le_bytes(
                chunk[4..8]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index position: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexPosition)?,
            );
            let time = u64::from_le_bytes(
                chunk[8..16]
                    .try_into()
                    .with_error_context(|e| format!("Failed to parse index timestamp: {e}"))
                    .map_err(|_| IggyError::CannotReadIndexTimestamp)?,
            );
            let current = Index {
                offset,
                position,
                timestamp: time,
            };
            if current.timestamp >= timestamp {
                return Ok(last_index.or(Some(current)));
            }
            last_index = Some(current);
        }
        Ok(None)
    }
}
