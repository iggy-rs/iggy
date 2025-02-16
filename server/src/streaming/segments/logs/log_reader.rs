use crate::streaming::{
    batching::{
        iterator::IntoMessagesIterator,
        message_batch::{RetainedMessageBatch, RETAINED_BATCH_OVERHEAD},
    },
    segments::indexes::IndexRange,
};
use bytes::BytesMut;
use error_set::ErrContext;
use iggy::{error::IggyError, utils::byte_size::IggyByteSize};
use seekable_async_file::{SeekableAsyncFile, SeekableAsyncFileMetrics};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::{trace, warn};

/// A dedicated struct for reading from the log file.
pub struct SegmentLogReader {
    file_path: String,
    file: Arc<SeekableAsyncFile>,
    log_size_bytes: Arc<AtomicU64>,
    _metrics: Arc<SeekableAsyncFileMetrics>,
}

impl std::fmt::Debug for SegmentLogReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentLogReader")
            .field("file_path", &self.file_path)
            .field("log_size_bytes", &self.log_size_bytes)
            .finish()
    }
}

impl SegmentLogReader {
    /// Opens the log file in read mode.
    pub async fn new(file_path: &str, log_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let metrics = Arc::new(SeekableAsyncFileMetrics::default());
        let file = Arc::new(
            SeekableAsyncFile::open(
                Path::new(file_path),
                Arc::clone(&metrics),
                Duration::from_micros(100),
                0,
            )
            .await,
        );

        let file_clone = file.clone();
        tokio::task::spawn(async move {
            Arc::clone(&file_clone)
                .start_delayed_data_sync_background_loop()
                .await
        });

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            log_size_bytes,
            _metrics: metrics,
        })
    }

    /// Loads message batches given an index range.
    pub async fn load_batches_by_range_impl(
        &self,
        index_range: &IndexRange,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let file_size = self.log_size_bytes.load(Ordering::Acquire);
        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(Vec::new());
        }

        let mut offset = index_range.start.position as u64;
        let mut batches = Vec::new();
        let mut last_batch_to_read = false;

        while !last_batch_to_read && offset < file_size {
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    offset += bytes_read;
                    let last_offset_in_batch = batch.base_offset + batch.last_offset_delta as u64;

                    if last_offset_in_batch >= index_range.end.offset as u64 || offset >= file_size
                    {
                        last_batch_to_read = true;
                    }
                    batches.push(batch);
                }
                None => {
                    break;
                }
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

        let mut offset = 0_u64;
        let mut message_ids = Vec::new();

        while offset < file_size {
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    offset += bytes_read;
                    for msg in batch.into_messages_iter() {
                        message_ids.push(msg.id);
                    }
                }
                None => {
                    // Possibly reached EOF or truncated
                    break;
                }
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
        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(());
        }

        let mut offset = index_range.start.position as u64;
        let mut last_batch_to_read = false;

        while !last_batch_to_read && offset < file_size {
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    offset += bytes_read;
                    let last_offset_in_batch = batch.base_offset + batch.last_offset_delta as u64;
                    if offset >= file_size || last_offset_in_batch >= index_range.end.offset as u64
                    {
                        last_batch_to_read = true;
                    }
                    on_batch(batch)?;
                }
                None => {
                    break;
                }
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
        let file_size = self.log_size_bytes.load(Ordering::Acquire);
        if file_size == 0 {
            warn!("Log file {} is empty.", self.file_path);
            return Ok(());
        }

        let threshold = file_size.saturating_sub(bytes_to_load);
        let mut offset = 0_u64;
        let mut accumulated_size = 0_u64;

        while offset < file_size {
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    if accumulated_size >= threshold {
                        on_batch(batch)?;
                    }
                    offset += bytes_read;
                    accumulated_size += bytes_read;
                }
                None => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn read_next_batch(
        &self,
        offset: u64,
        file_size: u64,
    ) -> Result<Option<(RetainedMessageBatch, u64)>, IggyError> {
        let overhead_size = RETAINED_BATCH_OVERHEAD;
        if offset + overhead_size > file_size {
            return Ok(None);
        }

        let overhead_buf = self.file.read_at(offset, overhead_size).await;
        if overhead_buf.len() < overhead_size as usize {
            warn!(
                "Cannot read full batch overhead at offset {} in file {}. Possibly truncated.",
                offset, self.file_path
            );
            return Ok(None);
        }

        let batch_base_offset = u64::from_le_bytes(
            overhead_buf[0..8]
                .try_into()
                .map_err(|_| IggyError::CannotReadBatchBaseOffset)?,
        );
        let batch_length = u32::from_le_bytes(
            overhead_buf[8..12]
                .try_into()
                .map_err(|_| IggyError::CannotReadBatchLength)?,
        );
        let last_offset_delta = u32::from_le_bytes(
            overhead_buf[12..16]
                .try_into()
                .map_err(|_| IggyError::CannotReadLastOffsetDelta)?,
        );
        let max_timestamp = u64::from_le_bytes(
            overhead_buf[16..24]
                .try_into()
                .map_err(|_| IggyError::CannotReadMaxTimestamp)?,
        );

        warn!(
            "batch base offset: {}, length: {}, max timestamp: {}",
            batch_base_offset, batch_length, max_timestamp
        );

        warn!("actual file size: {}", file_size);

        let payload_len = batch_length as usize;
        let payload_offset = offset + overhead_size;
        if payload_offset + payload_len as u64 > file_size {
            warn!(
                "Cannot read full batch payload at offset {} in file {}. Possibly truncated.",
                payload_offset, self.file_path
            );
            return Ok(None);
        }

        let payload_buf = self.file.read_at(payload_offset, payload_len as u64).await;
        if payload_buf.len() < payload_len {
            warn!(
                "Cannot read full batch payload at offset {} in file {}. Possibly truncated.",
                payload_offset, self.file_path
            );
            return Ok(None);
        }

        let bytes_read = overhead_size + payload_len as u64;
        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            last_offset_delta,
            max_timestamp,
            IggyByteSize::from(payload_len as u64),
            BytesMut::from(&payload_buf[..]).freeze(),
        );

        Ok(Some((batch, bytes_read)))
    }
}
