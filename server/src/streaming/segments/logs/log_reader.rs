use crate::{
    state::file::BUF_READER_CAPACITY_BYTES,
    streaming::{
        batching::{
            iterator::IntoMessagesIterator,
            message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD},
        },
        segments::indexes::IndexRange,
    },
};
use bytes::BytesMut;
use error_set::ErrContext;
use iggy::{error::IggyError, utils::byte_size::IggyByteSize};
use std::{
    io::SeekFrom,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    sync::RwLock,
};
use tracing::{trace, warn};

/// A dedicated struct for reading from the log file.
#[derive(Debug)]
pub struct SegmentLogReader {
    file_path: String,
    file: RwLock<Option<File>>,
    log_size_bytes: Arc<AtomicU64>,
}

impl SegmentLogReader {
    /// Opens the log file in read mode.
    pub async fn new(file_path: &str, log_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await
            .with_error_context(|e| format!("Failed to open log file: {file_path}, error: {e}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let actual_log_size = file
            .metadata()
            .await
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        log_size_bytes.store(actual_log_size, Ordering::Release);

        Ok(Self {
            file_path: file_path.to_string(),
            file: RwLock::new(Some(file)),
            log_size_bytes,
        })
    }

    /// Loads message batches given an index range.
    pub async fn load_batches_by_range_impl(
        &self,
        index_range: &IndexRange,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let mut file_guard = self.file.write().await;
        let file = file_guard
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        // TODO(hubcio): perhaps we could read size in below loop,
        // this way if someone is writing, we can read in parallel
        let file_size = self.log_size_bytes.load(Ordering::Acquire);
        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(Vec::new());
        }

        file.seek(SeekFrom::Start(index_range.start.position as u64))
            .await
            .map_err(|_| IggyError::CannotSeekFile)?;

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        let mut read_bytes = index_range.start.position as u64;
        let mut batches = Vec::new();
        let mut last_batch_to_read = false;

        while !last_batch_to_read {
            match read_next_batch(&mut reader, &self.file_path).await? {
                Some((batch, bytes_read)) => {
                    read_bytes += bytes_read;
                    let last_offset_in_batch = batch.base_offset + batch.last_offset_delta as u64;
                    if last_offset_in_batch >= index_range.end.offset as u64
                        || read_bytes >= file_size
                    {
                        last_batch_to_read = true;
                    }
                    batches.push(batch);
                }
                None => break,
            }
        }

        trace!("Loaded {} message batches.", batches.len());
        Ok(batches)
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids_impl(&self) -> Result<Vec<u128>, IggyError> {
        let file_size = self.log_size_bytes.load(Ordering::Acquire);

        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(Vec::new());
        }

        let mut file = self.file.write().await;
        let file = file
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        file.seek(SeekFrom::Start(0))
            .await
            .with_error_context(|e| {
                format!(
                    "Failed to seek to position 0 in file {}, error: {e}",
                    self.file_path
                )
            })
            .map_err(|_| IggyError::CannotSeekFile)?;

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, &mut *file);
        let mut message_ids = Vec::new();
        let mut read_bytes = 0_u64;

        while read_bytes < file_size {
            match read_next_batch(&mut reader, &self.file_path).await? {
                Some((batch, bytes_read)) => {
                    read_bytes += bytes_read;
                    for msg in batch.into_messages_iter() {
                        message_ids.push(msg.id);
                    }
                }
                None => break, // reached EOF or encountered a truncated batch
            }
        }

        trace!("Loaded {} message IDs from the log.", message_ids.len());
        Ok(message_ids)
    }

    /// Loads message batches given an index range and calls the provided callback for each batch.
    pub async fn load_batches_by_range_with_callback<F>(
        &self,
        index_range: &IndexRange,
        mut on_batch: F,
    ) -> Result<(), IggyError>
    where
        F: FnMut(RetainedMessageBatch) -> Result<(), IggyError>,
    {
        let file_size = self.log_size_bytes.load(Ordering::Acquire);

        trace!(
            "Loading message batches by index range: {} [{}] - {} [{}], file size: {}",
            index_range.start.position,
            index_range.start.offset,
            index_range.end.position,
            index_range.end.offset,
            file_size
        );

        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(());
        }

        let mut file_guard = self.file.write().await;
        let file = file_guard
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        file.seek(SeekFrom::Start(index_range.start.position as u64))
            .await
            .with_error_context(|e| {
                format!(
                    "Failed to seek to position {} in file {}, error: {e}",
                    index_range.start.position, self.file_path
                )
            })
            .map_err(|_| IggyError::CannotSeekFile)?;

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        let mut read_bytes = index_range.start.position as u64;
        let mut last_batch_to_read = false;

        while !last_batch_to_read {
            match read_next_batch(&mut reader, &self.file_path).await? {
                Some((batch, bytes_read)) => {
                    read_bytes += bytes_read;
                    let last_offset_in_batch = batch.base_offset + batch.last_offset_delta as u64;
                    if read_bytes >= file_size
                        || last_offset_in_batch >= index_range.end.offset as u64
                    {
                        last_batch_to_read = true;
                    }
                    on_batch(batch)?;
                }
                None => break,
            }
        }

        Ok(())
    }

    /// Loads message batches up to a given size and calls the provided callback for each batch
    /// after a threshold is reached.
    pub async fn load_batches_by_size_with_callback(
        &self,
        bytes_to_load: u64,
        mut on_batch: impl FnMut(RetainedMessageBatch) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        let mut file = self.file.write().await;
        let file = file
            .as_mut()
            .unwrap_or_else(|| panic!("File {} should be open", self.file_path));

        let file_size = self.log_size_bytes.load(Ordering::Acquire);
        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(());
        }

        file.seek(SeekFrom::Start(0))
            .await
            .with_error_context(|e| {
                format!("Failed to seek to the beginning of the file, error: {e}")
            })
            .map_err(|_| IggyError::CannotSeekFile)?;

        let threshold = file_size.saturating_sub(bytes_to_load);
        let mut accumulated_size = 0_u64;
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);

        while accumulated_size < file_size {
            match read_next_batch(&mut reader, &self.file_path).await? {
                Some((batch, bytes_read)) => {
                    if accumulated_size >= threshold {
                        on_batch(batch)?;
                    }
                    accumulated_size += bytes_read;
                }
                None => break, // reached EOF or encountered a truncated batch
            }
        }

        Ok(())
    }
}

/// Helper function that reads one batch (header and payload) from the provided BufReader.
/// It returns `Ok(Some((batch, bytes_read)))` when a full batch is read,
/// and returns `Ok(None)` when the reader cannot read a full header or payload (EOF or a truncated file).
async fn read_next_batch<R>(
    reader: &mut BufReader<R>,
    file_path: &str,
) -> Result<Option<(RetainedMessageBatch, u64)>, IggyError>
where
    R: AsyncReadExt + Unpin,
{
    let mut header = [0u8; RETAINED_BATCH_OVERHEAD as usize];
    if let Err(e) = reader.read_exact(&mut header).await {
        trace!("Cannot read batch header in file {file_path}, error: {e}");
        return Ok(None);
    }

    let batch_base_offset = u64::from_le_bytes(
        header[0..8]
            .try_into()
            .with_error_context(|e| format!("Failed to read batch base offset, error: {e}"))
            .map_err(|_| IggyError::CannotReadBatchBaseOffset)?,
    );
    let batch_length = u32::from_le_bytes(
        header[8..12]
            .try_into()
            .with_error_context(|e| format!("Failed to read batch length, error: {e}"))
            .map_err(|_| IggyError::CannotReadBatchLength)?,
    );
    let last_offset_delta = u32::from_le_bytes(
        header[12..16]
            .try_into()
            .with_error_context(|e| format!("Failed to read last offset delta, error: {e}"))
            .map_err(|_| IggyError::CannotReadLastOffsetDelta)?,
    );
    let max_timestamp = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .with_error_context(|e| format!("Failed to read max timestamp, error: {e}"))
            .map_err(|_| IggyError::CannotReadMaxTimestamp)?,
    );

    let payload_len = batch_length as usize;
    let mut payload = BytesMut::with_capacity(payload_len);
    unsafe { payload.set_len(payload_len) };
    if let Err(error) = reader.read_exact(&mut payload).await {
        warn!(
            "Cannot read batch payload for base offset: {}, delta: {}, timestamp: {}. \
             Batch length: {}, payload length: {} in file {}. Possibly truncated file, error: {:?}",
            batch_base_offset,
            last_offset_delta,
            max_timestamp,
            batch_length,
            payload_len,
            file_path,
            error
        );
        return Ok(None);
    }

    let bytes_read = RETAINED_BATCH_OVERHEAD + payload_len as u64;
    let batch = RetainedMessageBatch::new(
        batch_base_offset,
        last_offset_delta,
        max_timestamp,
        IggyByteSize::from(payload_len as u64),
        payload.freeze(),
    );

    Ok(Some((batch, bytes_read)))
}
